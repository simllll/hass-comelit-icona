"""Live video streaming (and stills) for Comelit entrances.

Holds a SINGLE ViP video call to an entrance panel, feeding a local RTSP server
that Home Assistant's stream component (go2rtc) connects to. Both live view and
still images come from that one call — the intercom serves only one video call
at a time, so anything that opens a second, competing call causes the device to
drop the connection. Keeping a single shared call avoids that churn.

The video session REUSES the coordinator's single shared, authenticated ICONA
connection and its already-open CTPP registration (never a second CTPP — two
CTPP registrations on one ViP identity make the device drop video after ~3s).
Before starting a session the manager pauses the doorbell VIP listener (which
reads from the same CTPP channel), and resumes it once video stops/idles.

The call starts lazily on first use, is kept alive while anything is watching
(an RTSP client) or recently asked for a still, is recycled if its media stalls
(the device dropped it), and is stopped after a short idle period.

The same single call also carries "speak" (talk-back): a short outbound call in
talk mode that plays prepared A-law audio on the entrance speaker and hangs up.
It never interrupts a ring, a call or another session (see async_speak).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.core import HomeAssistant

from .const import CONF_FACE_RECOGNITION
from .icona.client import IconaBridgeClient as SharedIconaClient
from .icona.models import DeviceConfig
from .icona.rtsp_server import LocalRtspServer
from .icona.video_call import VideoCallSession

if TYPE_CHECKING:
    from .coordinator import ComelitDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

# Stop the call this long after the last viewer/still request.
_IDLE_TIMEOUT = 30.0
# Grace period after starting a call before health/idle checking (lets media ramp
# and HA/go2rtc connect).
_CONNECT_GRACE = 12.0
# Recycle the call if no new media packets arrive for this long (device dropped
# it — e.g. BrokenPipe — so the next request starts a clean one).
_STALL_TIMEOUT = 10.0
# How long to wait for a freshly-decoded frame for a still.
_SNAPSHOT_TIMEOUT = 8.0
# With face-recognition events on, hold the on-ring preview until this long
# after the ring: the monitor recognises a face 4-10 s after the ring, and
# speak(mode=answer) can only answer the call while the preview is held.
_FACE_PREVIEW_HOLD = 16.0

# --- speak (talk-back) -------------------------------------------------
# After a ring the panel is busy (calling the monitor) until the door-open
# frame is seen; without it, assume the call is over after this long (an
# answered conversation at the monitor can last 90 s and more).
_RING_BUSY_MAX = 90.0
# Flags of the only call-end frame verified (0x1860/0x0003, 6741W, door
# opened by the monitor's face recognition).
_CALL_END_DOOR_OPEN = 0x000E
# Let the panel settle this long after its call ended before calling it...
_CALL_END_SETTLE = 3.0
# ...and if it still refuses the call, try once more after this long.
_CALL_RETRY_DELAY = 3.0
# Bound on starting the talk call (signaling + first video, normally 2-5 s).
_SPEAK_START_TIMEOUT = 30.0
# Max wait for the 0x70 answer once the call is up (sent right after video).
_SPEAK_ANSWER_TIMEOUT = 5.0
# Bound on answering an incoming call (ACK drain up to 10 s + device RTPC).
_SPEAK_ANSWER_INBOUND_TIMEOUT = 20.0
# Silence kept flowing after the last speech frame (panel jitter buffer).
_SPEAK_TAIL = 0.6
# Grace on top of the audio length before playback is declared stuck.
_SPEAK_PLAY_GRACE = 5.0
_SPEAK_FRAME_SECONDS = 0.020
# Answer mode answers first: this long at most for the audio (TTS + decode)
# to be ready while the answered call carries silence.
_SPEAK_AUDIO_TIMEOUT = 35.0

# --- door open inside the call (0x1840/0x000D) ---------------------------
# After answering a ring only to open the door, let the answer settle.
_DOOR_AFTER_ANSWER = 0.5
# After the door command, wait at most this long in all for the panel's ACK
# and its CALL_END sub 0x000E (the relay fired: app PCAP, 6701W), then go on
# and hang up a call answered only for this. A panel that replies does so at
# once and resends an unacknowledged frame after 0.45-1 s (6741W logs), so
# 2 s still catch a first resent copy. The 6741W sends neither after an
# in-call open and its lock trips anyway (6741W door test: gate open
# within 2-3 s): there the wait is idle, and the former 6 s (2 s ACK + 4 s
# confirmation) only delayed the hang-up. The hang-up is local (no frame is
# sent to the panel), so it cannot cut the relay.
_DOOR_WAIT = 2.0
# Bound on answering a ring preview before an in-call door open.
_DOOR_ANSWER_TIMEOUT = 20.0


def speak_result(
    reason: str = "", *, mode: str = "call", duration: float = 0.0
) -> dict[str, Any]:
    """Outcome of a speak request: every path returns these keys."""
    return {
        "spoken": False,
        "reason": reason,
        "mode": mode,  # mode actually used ("answer" may fall back to "call")
        "fallback": False,
        "answered": False,  # an incoming call was answered ("answer" mode)
        "answered_after": None,  # seconds from the ring to that answer
        "attempts": 0,
        "waited": 0.0,
        "duration": duration,
        "rx_audio": False,
        "device_ack": False,
        "transport": None,  # TX audio transport used: "tcp" / "udp"
        "transport_auto": False,  # chosen by "auto": the way the call's media came
        "media_transport": None,  # how the call's media reached HA: "tcp" / "udp"
        "frames_sent": 0,
        "audio_ready_after": None,  # answer mode: seconds from answer to audio
        "cached": False,  # frames from the in-memory cache (set by speak.py)
        "prepared": False,  # prepare_only: frames built and cached, no call
        "prepare_time": None,  # seconds spent building the frames
        "door_opened": False,  # open_door: 0x1840/0x000D sent in the call
        "door_ack": False,  # the panel ACKed it (not proof the relay tripped)
        "door_confirmed": False,  # the panel ended the call: the relay fired
        "door_reason": "",
    }


@dataclass
class _SpeakRun:
    """Book-keeping of one speak, so the cleanup knows what to undo."""

    started_at: float = 0.0
    session: VideoCallSession | None = None
    starting: bool = False  # a talk call start paused the listener
    retry: bool = False  # the last attempt may be retried
    inbound: bool = False  # run.session is an answered incoming call


def _note_transport(session: VideoCallSession, result: dict[str, Any]) -> None:
    """Copy the talk transport, and why it was chosen, into the result."""
    result["transport"] = session.talk_transport
    result["transport_auto"] = bool(getattr(session, "talk_transport_auto", False))
    result["media_transport"] = getattr(session, "media_transport", None)


def _transport_text(result: dict[str, Any]) -> str:
    """The transport for the logs, e.g. "tcp (auto: media over tcp)"."""
    transport = result["transport"] or "?"
    if not result["transport_auto"]:
        return f"{transport} (requested)"
    media = result["media_transport"]
    return f"{transport} (auto: media over {media})" if media else f"{transport} (auto: no media seen)"


def _relay_of(door: dict[str, Any]) -> int:
    """The door's relay (output-index) as is, 0 included; 1 when missing."""
    index = door.get("output-index")
    return int(index) if index is not None else 1


def _same_address(a: str, b: str) -> bool:
    """Loose VIP address match, as events.address_matches (kit-mode rings
    drop the "S" prefix of the address book: B100001 vs SB100001)."""
    if not a or not b:
        return False
    a, b = a.upper(), b.upper()
    return a == b or a.startswith(b) or b.startswith(a) or a.endswith(b) or b.endswith(a)


class ComelitStreamManager:
    """Owns one persistent RTSP server and a single on-demand ViP video call."""

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        port: int,
        token: str,
        vip_provider: Callable[[], dict],
        get_shared_client: Callable[[], SharedIconaClient | None],
        coordinator: ComelitDataUpdateCoordinator,
    ) -> None:
        """Initialize the manager (does not connect until first use)."""
        self.hass = hass
        self._host = host
        self._port = port
        self._token = token
        self._vip = vip_provider
        # Returns the coordinator's single shared authenticated ICONA client
        # (the CTPP owner) — video sessions reuse it and its CTPP channel.
        self._get_shared_client = get_shared_client
        self._coordinator = coordinator
        self._server: LocalRtspServer | None = None
        self._session: VideoCallSession | None = None
        self._entrance: str | None = None
        self._lock = asyncio.Lock()
        self._idle_task: asyncio.Task | None = None
        self._last_use = 0.0
        # Loop time of the last live-view (stream_source) request — distinct
        # from _last_use (which snapshots also bump). Used to avoid dropping a
        # snapshot-only preview call out from under a concurrent live viewer.
        self._last_stream_request = 0.0
        # Speak: call activity seen on CTPP (loop times) and the speak guard.
        self._last_ring_at = 0.0
        self._call_end_at = 0.0
        self._speak_lock = asyncio.Lock()
        self._speaking = False
        # The call speak is actually using (talk call or answered ring).
        self._speak_session: VideoCallSession | None = None
        # Calls being set up: they pause the listener before taking the CTPP
        # lock, so they hold CTPP before self._session is set.
        self._starting = 0

    @property
    def session_active(self) -> bool:
        """True while a video/inbound session holds the shared connection."""
        return self._session is not None and self._session.active

    def _touch(self) -> None:
        self._last_use = self.hass.loop.time()

    def _speaking_elsewhere(self, entrance: str) -> bool:
        """True while a message is being spoken at another entrance."""
        if self._speaking and self._entrance and self._entrance != entrance:
            _LOGGER.warning(
                "Entrance %s: busy speaking at entrance %s, try again shortly",
                entrance,
                self._entrance,
            )
            return True
        return False

    async def async_stream_source(self, entrance: str) -> str | None:
        """Ensure the shared call is running and return the RTSP URL."""
        async with self._lock:
            if self._speaking_elsewhere(entrance):
                return None
            try:
                await self._ensure(entrance)
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Failed to start live video call: %s", err)
                await self._stop_session("start failure")
                return None
            self._touch()
            self._last_stream_request = self.hass.loop.time()
            url = self._server.rtsp_url if self._server else None
            _LOGGER.debug("stream_source(%s) -> %s", entrance, url)
            return url

    async def async_get_image(
        self, entrance: str, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return a fresh JPEG still from the shared call."""
        async with self._lock:
            if self._speaking_elsewhere(entrance):
                return None
            try:
                await self._ensure(entrance)
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Snapshot: failed to start video call: %s", err)
                await self._stop_session("snapshot start failure")
                return None
            self._touch()
            rx = self._session.rtp_receiver if self._session else None
            if rx is None:
                return None
            try:
                jpeg = await rx.get_jpeg_frame(timeout=_SNAPSHOT_TIMEOUT)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Snapshot decode failed: %s", err)
                jpeg = None
            if jpeg is None:
                if self._speak_owns_session():
                    # Never drop the call speak is using: it hangs up by itself.
                    return None
                # No fresh frame → the call likely died; drop it so the next
                # request starts a clean one instead of returning a frozen frame.
                _LOGGER.debug("Snapshot: no fresh frame, recycling call")
                await self._stop_session("no fresh frame")
            else:
                _LOGGER.debug("Snapshot: %d byte JPEG", len(jpeg))
            return jpeg

    # --- lifecycle -------------------------------------------------------

    async def _ensure(self, entrance: str) -> None:
        """Start the RTSP server + shared call to *entrance* if not already up."""
        if self._server is None:
            self._server = LocalRtspServer()
            await self._server.start()
            _LOGGER.debug("RTSP server started at %s", self._server.rtsp_url)

        if (
            self._session is None
            or not self._session.active
            or self._entrance != entrance
        ):
            if self._session is not None:
                _LOGGER.debug(
                    "Recreating call (active=%s, entrance %s->%s)",
                    self._session.active,
                    self._entrance,
                    entrance,
                )
            # Don't resume the listener between stop and immediate restart —
            # _start_session pauses it again right away.
            await self._stop_session("restart", resume_listener=False)
            await self._start_session(entrance)
            self._arm_idle_monitor()
        else:
            _LOGGER.debug("Reusing existing call to %s", entrance)

    async def _start_session(
        self, entrance: str, talk: bool = False, talk_transport: str = "auto"
    ) -> None:
        self._starting += 1
        try:
            await self._start_session_inner(entrance, talk, talk_transport)
        finally:
            self._starting -= 1

    async def _start_session_inner(
        self, entrance: str, talk: bool, talk_transport: str
    ) -> None:
        # Reuse the coordinator's single shared, authenticated ICONA client and
        # its already-open CTPP registration — never open a second connection
        # or a second CTPP (two CTPP registrations make the device drop video).
        client = self._get_shared_client()
        if client is None or not client.connected:
            raise RuntimeError("shared ICONA connection not available")

        # Pause the doorbell VIP listener so it stops consuming CTPP messages
        # meant for the video session. CTPP/CSPB stay open and are reused by
        # the session directly; the listener is resumed on stop/idle.
        events = self._coordinator.events_manager
        if events is not None:
            with contextlib.suppress(Exception):
                await events.async_pause_local()

        # Serialise CTPP negotiation with any other CTPP user (device only
        # handles one negotiation at a time).
        async with self._coordinator.ctpp_lock:
            vip = self._vip() or {}
            config = DeviceConfig(
                apt_address=vip.get("apt-address", ""),
                apt_subaddress=vip.get("apt-subaddress", 0),
                caller_address=entrance,
            )
            self._server.reset()
            # Forward any ring seen on CTPP during the call to the events
            # manager — the doorbell listener is paused while video holds the
            # channel, so this keeps mid-call rings from being lost.
            on_ring = None
            if events is not None:
                on_ring = events.handle_ring
            self._session = VideoCallSession(
                client,
                config,
                auto_timeout=False,
                rtsp_server=self._server,
                on_ring=on_ring,
                talk=talk,
                talk_transport=talk_transport,
            )
            await self._session.start()
        self._server.mark_ready()
        self._entrance = entrance
        _LOGGER.info(
            "%s call started to entrance %s", "Talk" if talk else "Live video", entrance
        )

    # --- inbound (device-initiated ring answer) --------------------------

    async def async_start_inbound(
        self, entrance: str, ring_ts: int, renewal_ack_ts: int = 0,
        preview_only: bool = False,
    ) -> bool:
        """Answer a device-initiated ring and start two-way media.

        Mirrors the outbound _start_session flow (pause listener, reuse shared
        client + held CTPP, hold the RTSP server) but drives the PCAP-verified
        20-step inbound answer sequence instead of the outbound one. Returns
        True on success.

        preview_only: grab the stream WITHOUT sending call_accepted, so the
        entrance never shows the ring as answered (true-preview experiment).
        """
        async with self._lock:
            client = self._get_shared_client()
            if client is None or not client.connected:
                _LOGGER.warning("Inbound: shared ICONA connection not available")
                return False

            # If a session is already up, don't stomp it.
            if self._session is not None and self._session.active:
                _LOGGER.debug("Inbound: a video session is already active — skipping")
                return False

            if self._server is None:
                self._server = LocalRtspServer()
                await self._server.start()
            self._starting += 1
            try:
                return await self._start_inbound_locked(
                    client, entrance, ring_ts, renewal_ack_ts, preview_only
                )
            finally:
                self._starting -= 1

    async def _start_inbound_locked(
        self,
        client: SharedIconaClient,
        entrance: str,
        ring_ts: int,
        renewal_ack_ts: int,
        preview_only: bool,
    ) -> bool:
        """Body of async_start_inbound (holding the stream lock)."""
        # Pause the doorbell listener so the answer sequence owns CTPP.
        events = self._coordinator.events_manager
        if events is not None:
            with contextlib.suppress(Exception):
                await events.async_pause_local()

        async with self._coordinator.ctpp_lock:
            vip = self._vip() or {}
            config = DeviceConfig(
                apt_address=vip.get("apt-address", ""),
                apt_subaddress=vip.get("apt-subaddress", 0),
                caller_address=entrance,
            )
            self._server.reset()
            on_ring = events.handle_ring if events is not None else None
            session = VideoCallSession(
                client,
                config,
                auto_timeout=True,
                rtsp_server=self._server,
                on_ring=on_ring,
            )
            try:
                await session.start_inbound(
                    entrance, ring_ts, renewal_ack_ts=renewal_ack_ts,
                    preview_only=preview_only,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("Inbound call answer failed: %s", err)
                with contextlib.suppress(Exception):
                    await session.stop()
                # Hand CTPP back to the doorbell listener.
                if events is not None:
                    with contextlib.suppress(Exception):
                        await events.async_resume_local()
                return False
            self._session = session

        self._server.mark_ready()
        self._entrance = entrance
        self._touch()
        self._arm_idle_monitor()
        _LOGGER.info(
            "Inbound video %s (entrance %s)",
            "preview started (no accept)" if preview_only else "call answered",
            entrance,
        )
        return True

    async def async_answer_inbound(self) -> bool:
        """Start two-way audio (TX) for the active inbound call."""
        session = self._session
        if session is None or not session.active:
            _LOGGER.debug("answer_inbound: no active inbound session")
            return False
        session.answer_inbound()
        return True

    async def async_grab_snapshot(self) -> bytes | None:
        """Grab one JPEG from the currently-active session's receiver.

        Used by the on-ring snapshot: after `async_start_inbound` has the
        preview video flowing, pull a frame without opening a second call.
        """
        rx = self._session.rtp_receiver if self._session else None
        if rx is None:
            return None
        try:
            jpeg = await rx.get_jpeg_frame(timeout=_SNAPSHOT_TIMEOUT)
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("On-ring snapshot decode failed: %s", err)
            return None
        if jpeg:
            self._touch()
            _LOGGER.debug("On-ring snapshot: %d byte JPEG", len(jpeg))
        return jpeg

    async def async_release_after_snapshot(
        self, ring_at: float, grace: float = 6.0
    ) -> None:
        """Drop a snapshot-only preview call — unless someone is watching.

        A live view shares this single session, so we must not cut it off.
        Waits a short grace for a concurrent live view to attach; if a viewer
        is present, or any ``stream_source`` was requested after the ring, the
        call is left for the idle monitor to reap. The final decision is made
        under the lock (which ``stream_source`` also holds) so it can't race a
        viewer that attaches at the last moment.
        """
        loop = self.hass.loop
        deadline = loop.time() + grace
        if self._coordinator.entry.options.get(CONF_FACE_RECOGNITION, False):
            deadline = max(deadline, ring_at + _FACE_PREVIEW_HOLD)
        while loop.time() < deadline:
            server = self._server
            if (server is not None and server.client_count > 0) or (
                self._last_stream_request > ring_at
            ):
                return
            if self._speak_owns_session():  # speak answered this call: it hangs up
                return
            await asyncio.sleep(0.5)
        async with self._lock:
            server = self._server
            if server is None or server.client_count > 0:
                return
            if self._speak_owns_session():
                return
            if self._session is None:  # already closed (speak, door open)
                return
            if self._last_stream_request > ring_at:
                return
            await self._stop_session("on-ring snapshot done")

    # --- speak (talk-back to the entrance) -------------------------------

    def note_ring(self) -> None:
        """A ring was seen: the panel is busy until its call ends."""
        self._last_ring_at = self.hass.loop.time()

    def note_call_end(self, flags: int) -> None:
        """A 0x1860/0x0003 frame was seen (*flags*: its bytes 8-9).

        Only the variant seen when the monitor opened the door (flags 0x000E)
        ends the call early. Other variants (maybe "answered at the monitor")
        are unverified: logged only, the ring then keeps the panel busy up to
        _RING_BUSY_MAX. Only the first copy after a ring counts: the panel
        retransmits it (5 times over 7.4 s) and copies must not move the window.
        """
        if flags != _CALL_END_DOOR_OPEN:
            _LOGGER.debug("Speak guard: call-end frame with flags 0x%04X ignored", flags)
            return
        if self._call_end_at <= self._last_ring_at:
            self._call_end_at = self.hass.loop.time()

    def _ring_call_live(self) -> bool:
        """True while the last ring's call may still be going on."""
        return (
            bool(self._last_ring_at)
            and self._call_end_at <= self._last_ring_at
            and self.hass.loop.time() - self._last_ring_at < _RING_BUSY_MAX
        )

    def _speak_busy_reason(self) -> str | None:
        """Why the entrance can't be called right now, or None when it's free."""
        client = self._get_shared_client()
        if client is None or not client.connected:
            return "no connection to the intercom"
        if self._session is not None and self._session.active:
            return "a video or call session is active"
        if self._ring_call_live():
            return "the entrance is ringing or in a call"
        if (
            self._call_end_at > self._last_ring_at
            and self.hass.loop.time() - self._call_end_at < _CALL_END_SETTLE
        ):
            return "the entrance call has just ended"
        return None

    def _speak_abort_reason(self, session: VideoCallSession, started_at: float) -> str | None:
        """Why a running speak must stop now, or None to carry on."""
        if self._session is not session or not session.active:
            return "the call to the entrance dropped"
        if self._last_ring_at > started_at:
            return "a ring arrived: hanging up to let it through"
        if session.answered and not session.talk_running:
            return "the audio sender is not running"
        return None

    def _speak_owns_session(self) -> bool:
        """True while speak is using the current call (release/idle keep off)."""
        s = self._speak_session
        return s is not None and s is self._session

    def _cancel_idle_monitor(self) -> None:
        """Stop the idle monitor so it can't judge the next call."""
        task, self._idle_task = self._idle_task, None
        if task is not None and not task.done():
            task.cancel()

    @property
    def holds_ctpp(self) -> bool:
        """True while a call (any state, starting too) holds the CTPP channel."""
        return self._session is not None or self._starting > 0

    def _door_addresses(self) -> tuple[str, str]:
        """(our full VIP address, apartment address) for door frames."""
        vip = self._vip() or {}
        apt = str(vip.get("apt-address", ""))
        return f"{apt}{vip.get('apt-subaddress', 0)}", apt

    async def async_open_door_in_call(self, door: dict[str, Any]) -> dict[str, Any] | None:
        """Open *door* inside the ring's call HA holds with the door's entrance.

        None: HA holds no ring's call with that entrance (no call, another
        entrance, or an outgoing call: live view, snapshot, speak "call").
        The caller then sends the usual TAP, as in 1.13.x: one door
        command per request, never both. Otherwise a dict: sent, ack,
        confirmed (CALL_END 0x000E), inbound, answered (HA answered the ring
        preview to open), ended (the ring's call was already over: the
        preview is hung up and the caller may use the TAP), event_flags,
        reason. ACK and confirmation are awaited for at most _DOOR_WAIT in
        all; the 6741W sends neither.

        Only rings' calls: there a 6741W ignored the TAP. A preview
        is answered first, with the same frames as speak's answer, since
        0x1840/0x000D is verified only inside a call HA has answered. In
        outgoing calls the app uses it too (PCAP, 6701W), but the 6741W never
        ACKs it, so without an ACK HA could not tell whether a TAP is still
        needed: the TAP alone is used there. A call answered only for this is
        closed afterwards, unless speak or a viewer uses it.
        """
        door_addr = str(door.get("apt-address") or "")
        relay = _relay_of(door)
        our_addr, apt_addr = self._door_addresses()
        out: dict[str, Any] = {
            "sent": False,
            "ack": False,
            "confirmed": False,  # the panel ended the call: the relay fired
            "inbound": False,  # the call is a ring's (no TAP fallback then)
            "answered": False,
            "ended": False,
            "event_flags": None,
            "reason": "",
        }
        async with self._lock:
            session = self._session
            if (
                session is None
                or not session.active
                or not session.inbound
                or not _same_address(door_addr, self._entrance or "")
            ):
                return None
            out["inbound"] = True
            if not session.inbound_accepted:
                try:
                    ok = await asyncio.wait_for(
                        session.accept_inbound(), timeout=_DOOR_ANSWER_TIMEOUT
                    )
                except Exception as err:  # noqa: BLE001
                    ok = False
                    out["reason"] = f"could not answer the call: {str(err) or type(err).__name__}"
                if not ok:
                    if session.call_already_ended is not None:
                        out["ended"] = True
                        out["reason"] = "the ring's call already ended"
                    elif not out["reason"]:
                        out["reason"] = "could not answer the call"
                    if not self._speak_owns_session() and self._session is session:
                        await self._stop_session("door: call not answered")
                        self._cancel_idle_monitor()
                    return out
                # (if speak owns the call, it answered it and sends the audio)
                out["answered"] = not self._speak_owns_session()
                if out["answered"]:
                    # A normal answer streams our audio (silence / browser
                    # mic), as after the Answer doorbell button.
                    session.answer_inbound()
                await asyncio.sleep(_DOOR_AFTER_ANSWER)
            sent_at = self.hass.loop.time()
            try:
                ack, flags = await session.open_door_in_call(
                    our_addr, door_addr, relay, apt_addr, ack_timeout=_DOOR_WAIT
                )
            except Exception as err:  # noqa: BLE001
                out["reason"] = f"in-call open failed: {str(err) or type(err).__name__}"
                return out
            server = self._server
            viewers = server is not None and server.client_count > 0
            closing = out["answered"] and not self._speak_owns_session() and not viewers
            if closing:
                # Before hanging up a call answered only for this, the rest of
                # _DOOR_WAIT for the panel's CALL_END 0x000E (relay fired),
                # which ends the call cleanly. Without an ACK the budget is
                # already spent (a confirmation seen meanwhile counts).
                await self._wait_door_confirmed(session, sent_at + _DOOR_WAIT)
            confirmed = session.door_confirmed
            out.update(
                sent=True,
                ack=ack,
                confirmed=confirmed,
                event_flags=flags,
                reason="confirmed" if confirmed else "ok" if ack else "no ACK from the panel",
            )
            if closing and self._session is session:
                await self._stop_session("door opened")
                self._cancel_idle_monitor()
            return out

    async def _wait_door_confirmed(self, session: VideoCallSession, deadline: float) -> None:
        """Until *deadline* (loop time) for the panel's CALL_END after the door."""
        while (
            session.active
            and not session.door_confirmed
            and self.hass.loop.time() < deadline
        ):
            await asyncio.sleep(0.1)

    async def async_speak(
        self,
        entrance: str,
        frames: list[bytes] | None = None,
        *,
        audio: Awaitable[list[bytes]] | None = None,
        door: dict[str, Any] | None = None,
        mode: str = "call",
        answer_fallback: bool = True,
        max_wait: float,
        audio_wait: float,
        lead_in: float,
        transport: str = "auto",
    ) -> dict[str, Any]:
        """Play A-law audio on the speaker of *entrance*, then maybe open.

        The audio is *frames*, or *audio* (an awaitable that builds them).
        mode "call": open a short outbound call once the panel is free (waits
        up to *max_wait* while a ring, a call or another session keeps it busy,
        then gives up rather than interrupt), answer it, play, hang up.
        mode "answer": answer the incoming call HA holds right now (the on-ring
        preview, or an auto-answered call) AT ONCE and play on it as soon as
        the audio is ready (silence meanwhile); with no such call, fall back
        to "call" when *answer_fallback*, else give up.
        *door* (an opendoor-address-book entry): after the audio, or right
        after the answer when there is none, open it: inside the answered
        ring's call (0x1840/0x000D, "answer" mode), or with the usual TAP
        during an outgoing call ("call" mode), as lock.open does; never
        otherwise.

        After the answer, silence is streamed and, up to *audio_wait*, the
        panel's own audio is awaited as proof the audio path is open; then
        *lead_in* more silence, the frames and a short tail. Never raises for
        runtime conditions: the result (same keys on every path) tells what
        happened.
        """
        loop = self.hass.loop
        result = speak_result(
            mode=mode, duration=round(len(frames or []) * _SPEAK_FRAME_SECONDS, 2)
        )
        if not frames and audio is None and door is None:
            result["reason"] = "no audio to play"
            return result
        if self._coordinator.events_manager is None:
            # Without the doorbell listener no ring is seen: the guard is blind.
            result["reason"] = "doorbell events are disabled: rings can't be seen"
            _LOGGER.warning("Speak: %s", result["reason"])
            return result
        t0 = loop.time()
        try:
            # (a zero timeout would fail even on a free lock)
            await asyncio.wait_for(
                self._speak_lock.acquire(), timeout=max(max_wait, 1.0)
            )
        except TimeoutError:
            result["reason"] = "another message is still being spoken"
            result["waited"] = round(loop.time() - t0, 1)
            _LOGGER.warning("Speak: gave up, %s", result["reason"])
            return result
        run = _SpeakRun()
        try:
            if mode == "answer":
                if await self._speak_answer(entrance, transport, run, result):
                    return await self._speak_on_call(
                        run, frames, audio, door, audio_wait, lead_in, result
                    )
                if result["reason"]:  # there was a call, answering it failed
                    return result
                if not answer_fallback:
                    result["reason"] = "no incoming call to answer"
                    _LOGGER.info("Speak: %s", result["reason"])
                    return result
                _LOGGER.info("Speak: no incoming call to answer, calling instead")
                result["mode"] = "call"
                result["fallback"] = True
            if not frames and audio is not None:
                # Call mode builds the audio before calling: no call is held
                # open while the TTS runs.
                frames = await self._speak_resolve_audio(audio, result)
                if not frames and door is None:
                    return result
            if await self._speak_call(entrance, transport, t0, max_wait, run, result):
                return await self._speak_on_call(
                    run, frames, None, door, audio_wait, lead_in, result
                )
            return result
        finally:
            try:
                await self._speak_cleanup(run, result)
            finally:
                self._speak_lock.release()

    async def _speak_resolve_audio(
        self, audio: Awaitable[list[bytes]], result: dict[str, Any]
    ) -> list[bytes] | None:
        """Await the audio builder; None (reason set) when it fails or is empty."""
        try:
            frames = await audio
        except Exception as err:  # noqa: BLE001
            result["reason"] = f"audio not available: {str(err) or type(err).__name__}"
            _LOGGER.warning("Speak: %s", result["reason"])
            return None
        if not frames:
            result["reason"] = "the audio is empty or silent"
            return None
        result["duration"] = round(len(frames) * _SPEAK_FRAME_SECONDS, 2)
        return frames

    async def _speak_await_audio(
        self, run: _SpeakRun, audio: Awaitable[list[bytes]], result: dict[str, Any]
    ) -> list[bytes] | None:
        """Answer mode: wait for the audio while the answered call carries silence."""
        loop = self.hass.loop
        session = run.session
        assert session is not None
        task = asyncio.ensure_future(audio)
        deadline = loop.time() + _SPEAK_AUDIO_TIMEOUT
        while not task.done():
            abort = self._speak_abort_reason(session, run.started_at)
            if abort is None and loop.time() > deadline:
                abort = "the audio was not ready in time"
            if abort is not None:
                task.cancel()
                result["reason"] = abort
                _LOGGER.warning("Speak: %s", abort)
                return None
            self._touch()
            await asyncio.wait({task}, timeout=0.1)
        frames = await self._speak_resolve_audio(task, result)
        if frames:
            result["audio_ready_after"] = round(loop.time() - run.started_at, 1)
            _LOGGER.debug(
                "Speak: audio ready %.1f s after the answer", result["audio_ready_after"]
            )
        return frames

    async def _speak_on_call(
        self,
        run: _SpeakRun,
        frames: list[bytes] | None,
        audio: Awaitable[list[bytes]] | None,
        door: dict[str, Any] | None,
        audio_wait: float,
        lead_in: float,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """The call is answered: play (once the audio is ready), then open."""
        if not frames and audio is not None:
            frames = await self._speak_await_audio(run, audio, result)
        if frames:
            await self._speak_play(run, frames, audio_wait, lead_in, result)
        if door is not None:
            if frames is None and audio is None:
                # Door only (e.g. at night): let the answer settle, as the
                # coordinator's in-call open does.
                await asyncio.sleep(_DOOR_AFTER_ANSWER)
            await self._speak_open_door(run, door, result)
            if not frames and not result["reason"]:
                result["reason"] = result["door_reason"]
        return result

    async def _speak_open_door(
        self, run: _SpeakRun, door: dict[str, Any], result: dict[str, Any]
    ) -> None:
        """open_door: 0x1840/0x000D on an answered ring's call, else the TAP."""
        session = run.session
        door_addr = str(door.get("apt-address") or "")
        # On a ring's call a new ring during the message (often the same
        # visitor pressing again) no longer prevents the open: the message
        # was cut and the door opens at once inside this call, still ours.
        new_ring = self._last_ring_at > run.started_at
        if session is None or self._session is not session or not session.active:
            result["door_reason"] = "the call dropped: door not opened"
        elif not _same_address(door_addr, self._entrance or ""):
            result["door_reason"] = "the door belongs to another entrance"
        elif not run.inbound:
            # An outgoing speak call ("call" mode): the usual TAP, sent during
            # the call through the coordinator, as lock.open does with any
            # outgoing call; 0x1840/0x000D only on rings' calls. Not while a
            # new ring is up: a 6741W ignored the TAP during a ring.
            if new_ring:
                result["door_reason"] = "a new ring arrived: door not opened (outgoing call)"
            else:
                try:
                    await self._coordinator.async_open_door(str(door.get("name") or ""))
                except Exception as err:  # noqa: BLE001
                    result["door_reason"] = (
                        f"door open failed: {str(err) or type(err).__name__}"
                    )
                else:
                    result["door_opened"] = True
                    result["door_reason"] = "opened with the TAP (outgoing call)"
        else:
            our_addr, apt_addr = self._door_addresses()
            sent_at = self.hass.loop.time()
            try:
                ack, _flags = await session.open_door_in_call(
                    our_addr, door_addr, _relay_of(door), apt_addr, ack_timeout=_DOOR_WAIT
                )
            except Exception as err:  # noqa: BLE001
                result["door_reason"] = (
                    f"in-call open failed: {str(err) or type(err).__name__}"
                )
            else:
                result["door_opened"] = True
                result["door_ack"] = ack
                await self._wait_door_confirmed(session, sent_at + _DOOR_WAIT)
                result["door_confirmed"] = session.door_confirmed
                result["door_reason"] = (
                    "confirmed" if session.door_confirmed
                    else "ok" if ack else "no ACK from the panel"
                )
                if new_ring:
                    result["door_reason"] += " (opened at once: a new ring arrived)"
        level = logging.INFO if result["door_opened"] else logging.WARNING
        _LOGGER.log(level, "Speak: door %s: %s", door.get("name"), result["door_reason"])

    async def _speak_answer(
        self, entrance: str, transport: str, run: _SpeakRun, result: dict[str, Any]
    ) -> bool:
        """Answer the incoming call held right now and start the TX audio.

        True: run.session is answered and streaming. False without a reason:
        no incoming call to answer. False with a reason: answering failed.
        No wait for a free panel here: the call is the panel's own.
        """
        loop = self.hass.loop
        async with self._lock:
            session = self._session
            if (
                session is None
                or not session.active
                or not session.inbound
                or not self._ring_call_live()
                or not _same_address(entrance, self._entrance or "")
            ):
                return False
            # From here the on-ring snapshot release and the idle stop keep
            # their hands off this call.
            self._speaking = True
            self._speak_session = session
            run.session = session
            run.inbound = True
            run.started_at = loop.time()
        result["attempts"] = 1
        detail = ""
        try:
            ok = await asyncio.wait_for(
                session.start_talk_inbound(transport), timeout=_SPEAK_ANSWER_INBOUND_TIMEOUT
            )
        except Exception as err:  # noqa: BLE001
            ok = False
            detail = f": {str(err) or type(err).__name__}"
        if not ok and getattr(session, "call_already_ended", None) is not None:
            # Nothing to answer any more: hang up our preview, then behave
            # as "no incoming call" (answer_fallback may call instead).
            _LOGGER.info("Speak: the incoming call already ended")
            run.session, run.inbound, self._speak_session = None, False, None
            self._speaking = False
            await self._finish_speak(session, run.started_at, inbound=True)
            return False
        if not ok:
            result["reason"] = f"could not answer the incoming call{detail}"
            _LOGGER.warning("Speak: %s", result["reason"])
            return False
        result["answered"] = True
        result["answered_after"] = round(loop.time() - self._last_ring_at, 1)
        _note_transport(session, result)
        _LOGGER.info(
            "Speak: answered the incoming call %.1f s after the ring, TX audio over %s",
            result["answered_after"],
            _transport_text(result),
        )
        return True

    async def _speak_call(
        self,
        entrance: str,
        transport: str,
        t0: float,
        max_wait: float,
        run: _SpeakRun,
        result: dict[str, Any],
    ) -> bool:
        """Wait for a free panel, then call it (one retry if it refuses)."""
        loop = self.hass.loop
        reason = self._speak_busy_reason()
        if reason is not None:
            _LOGGER.info("Speak: waiting, %s", reason)
        while reason is not None:
            if loop.time() - t0 >= max_wait:
                result["reason"] = f"entrance busy ({reason})"
                result["waited"] = round(loop.time() - t0, 1)
                _LOGGER.warning(
                    "Speak: gave up after %.0f s, %s", loop.time() - t0, reason
                )
                return False
            await asyncio.sleep(0.5)
            reason = self._speak_busy_reason()
        result["waited"] = round(loop.time() - t0, 1)

        run.started_at = loop.time()
        self._speaking = True
        for attempt in (1, 2):
            result["attempts"] = attempt
            if await self._speak_start_call(entrance, transport, run, result):
                return True
            if (
                not run.retry
                or attempt == 2
                or loop.time() - t0 + _CALL_RETRY_DELAY >= max_wait
            ):
                return False
            # The panel may still be in its end-of-call state (it keeps
            # retransmitting the door-open frame for ~7 s): one more try.
            _LOGGER.info(
                "Speak: %s; one more try in %.0f s", result["reason"], _CALL_RETRY_DELAY
            )
            await asyncio.sleep(_CALL_RETRY_DELAY)
            reason = self._speak_busy_reason()
            if reason is not None:
                result["reason"] = f"entrance busy ({reason})"
                _LOGGER.warning("Speak: gave up, %s", reason)
                return False
        return False

    async def _speak_start_call(
        self, entrance: str, transport: str, run: _SpeakRun, result: dict[str, Any]
    ) -> bool:
        """One attempt at the outbound talk call; True once it is answered."""
        run.retry = False
        async with self._lock:
            # Re-check under the lock: nothing (ring snapshot, live view) may
            # slip in between the check and the start.
            reason = self._speak_busy_reason()
            if reason is not None:
                result["reason"] = f"entrance busy ({reason})"
                _LOGGER.warning("Speak: gave up, %s", reason)
                return False
            if self._server is None:
                self._server = LocalRtspServer()
                await self._server.start()
            if self._session is not None:  # a dead leftover: clear it
                await self._stop_session("restart", resume_listener=False)
            # The idle monitor of a previous call (e.g. the on-ring preview)
            # must not judge ours: it would see the talk call still signalling
            # (not active yet) and stop it. A fresh one is armed after start.
            self._cancel_idle_monitor()
            run.starting = True  # _start_session pauses the listener
            try:
                await asyncio.wait_for(
                    self._start_session(entrance, talk=True, talk_transport=transport),
                    timeout=_SPEAK_START_TIMEOUT,
                )
            except Exception as err:  # noqa: BLE001
                result["reason"] = (
                    f"call to the entrance failed: {str(err) or type(err).__name__}"
                )
                _LOGGER.warning("Speak: %s", result["reason"])
                # Drop the half-started call, hand CTPP back to the listener.
                await self._stop_session("speak start failure")
                run.starting = False
                run.retry = True
                return False
            run.session = self._session
            self._speak_session = self._session
            run.starting = False
            self._touch()
            self._arm_idle_monitor()

        session = run.session
        if session is None:
            result["reason"] = "call to the entrance failed"
            return False
        abort = self._speak_abort_reason(session, run.started_at)
        if abort is None and not await session.wait_answered(_SPEAK_ANSWER_TIMEOUT):
            abort = "call not answered (no video, or the 0x70 failed)"
            run.retry = True
        if abort is None:
            _note_transport(session, result)
            return True
        result["reason"] = abort
        _LOGGER.warning("Speak: %s", abort)
        await self._finish_speak(session, run.started_at)  # hang up before a retry
        run.session = None  # only now: a cancel above still hangs up in cleanup
        return False

    async def _speak_play(
        self,
        run: _SpeakRun,
        frames: list[bytes],
        audio_wait: float,
        lead_in: float,
        result: dict[str, Any],
    ) -> bool:
        """Wait for the audio path, play the frames, report (both modes)."""
        loop = self.hass.loop
        session = run.session
        assert session is not None
        started_at = run.started_at

        # The panel's own audio is the best available proof that its audio
        # path is open. Play anyway when it doesn't come.
        rx = session.rtp_receiver
        deadline = loop.time() + audio_wait
        while True:
            abort = self._speak_abort_reason(session, started_at)
            if abort is not None:
                result["reason"] = abort
                _LOGGER.warning("Speak: %s", abort)
                return False
            if rx is not None and rx.audio_packet_count > 0:
                result["rx_audio"] = True
                break
            if loop.time() >= deadline:
                if audio_wait > 0:
                    _LOGGER.warning(
                        "Speak: no audio from the entrance within %.1f s; "
                        "playing anyway (the panel may not play it)",
                        audio_wait,
                    )
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(lead_in)

        # The sender takes one frame per 20 ms.
        session.talk_play(frames)
        play_deadline = (
            loop.time() + len(frames) * _SPEAK_FRAME_SECONDS + _SPEAK_PLAY_GRACE
        )
        while session.talk_pending > 0:
            abort = self._speak_abort_reason(session, started_at)
            if abort is None and loop.time() > play_deadline:
                abort = "playback did not finish in time"
            if abort is not None:
                session.stop_talk()
                result["reason"] = abort
                result["frames_sent"] = session.talk_frames_sent[1]
                _LOGGER.warning("Speak: %s", abort)
                return False
            self._touch()
            await asyncio.sleep(0.1)
        await asyncio.sleep(_SPEAK_TAIL)

        result["frames_sent"] = session.talk_frames_sent[1]
        result["device_ack"] = session.device_talk_ack
        result["spoken"] = True
        result["reason"] = "ok"
        _LOGGER.info(
            "Speak: played %.1f s at entrance %s (%s, waited %.1f s, %s, rx audio %s)",
            result["duration"],
            self._entrance,
            result["mode"],
            result["waited"],
            _transport_text(result),
            "yes" if result["rx_audio"] else "no",
        )
        return True

    async def _speak_cleanup(self, run: _SpeakRun, result: dict[str, Any]) -> None:
        """Hang up and hand CTPP back, whatever happened (even if cancelled)."""
        self._speaking = False
        self._speak_session = None
        session = run.session
        if session is not None:
            result["device_ack"] = result["device_ack"] or session.device_talk_ack
        if session is None and run.starting:
            if self._session is not None and self._session.talk_mode:
                session = self._session  # start cancelled half-way
            elif self._session is None:
                # Cancelled before the call object existed (waiting for
                # ctpp_lock): the listener is paused; resume it.
                task = self.hass.async_create_background_task(
                    self._resume_after_cancel(), "comelit_speak_resume"
                )
                await asyncio.shield(task)
                return
        if session is not None:
            # Own task, so a cancelled service call still hangs up.
            task = self.hass.async_create_background_task(
                self._finish_speak(session, run.started_at, inbound=run.inbound),
                "comelit_speak_finish",
            )
            await asyncio.shield(task)

    async def _finish_speak(
        self, session: VideoCallSession, started_at: float, inbound: bool = False
    ) -> None:
        """Hang up the talk call, unless a live viewer joined it meanwhile.

        Same teardown as live view, snapshots and auto-answered calls today:
        the channels are dropped locally, no hang-up frame is sent.
        """
        async with self._lock:
            if self._session is not session:
                return
            session.stop_talk()
            server = self._server
            if (server is not None and server.client_count > 0) or (
                self._last_stream_request > started_at
            ):
                if inbound and session.active:
                    # Answered call left to a viewer: back to the silence /
                    # browser-mic sender, as after the Answer button.
                    session.answer_inbound()
                _LOGGER.debug("Speak: a viewer joined, leaving the call to them")
                return
            await self._stop_session("speak done")
            # Our idle monitor must not judge the next call either.
            self._cancel_idle_monitor()

    async def _resume_after_cancel(self) -> None:
        """Hand CTPP back to the listener after a speak cancelled mid-start."""
        async with self._lock:
            if self._session is None:
                await self._stop_session("speak cancelled")

    async def _stop_session(self, reason: str = "", resume_listener: bool = True) -> None:
        if self._session is not None:
            _LOGGER.debug("Stopping video call (%s)", reason)
            with contextlib.suppress(Exception):
                await self._session.stop()
            self._session = None
        self._entrance = None
        if self._server is not None:
            self._server.mark_not_ready()
        # Hand the CTPP channel back to the doorbell listener. Skipped on
        # reconnect teardown (the shared client is dead; the coordinator
        # re-attaches the listener after it re-opens CTPP).
        if resume_listener:
            events = self._coordinator.events_manager
            if events is not None:
                with contextlib.suppress(Exception):
                    await events.async_resume_local()

    async def async_stop_for_reconnect(self) -> None:
        """Stop any active session without resuming the listener.

        Called by the coordinator when the shared connection dropped — the old
        session holds a reference to the dead client; the coordinator will
        re-attach the listener itself once CTPP is re-opened.
        """
        async with self._lock:
            if self._idle_task and not self._idle_task.done():
                self._idle_task.cancel()
            await self._stop_session("reconnect", resume_listener=False)

    def _arm_idle_monitor(self) -> None:
        if self._idle_task and not self._idle_task.done():
            return
        self._idle_task = self.hass.async_create_background_task(
            self._idle_monitor(), "comelit_stream_idle"
        )

    async def _idle_monitor(self) -> None:
        """Recycle a stalled (dropped) call and stop the call when idle."""
        await asyncio.sleep(_CONNECT_GRACE)
        loop = self.hass.loop
        last_pkts = -1
        stalled_since: float | None = None
        while True:
            await asyncio.sleep(2.0)
            session = self._session
            server = self._server
            if session is None or server is None:
                return
            if not session.active:
                async with self._lock:
                    await self._stop_session("inactive")
                return

            rx = session.rtp_receiver
            pkts = (
                rx.tcp_media_packet_count + rx.udp_media_packet_count if rx else 0
            )
            now = loop.time()

            # Liveness: media must keep flowing while a call is up.
            if pkts != last_pkts:
                last_pkts = pkts
                stalled_since = None
            elif stalled_since is None:
                stalled_since = now
            elif now - stalled_since >= _STALL_TIMEOUT:
                _LOGGER.warning(
                    "Video media stalled %.0fs (device dropped the call?) — recycling",
                    now - stalled_since,
                )
                async with self._lock:
                    await self._stop_session("media stalled")
                return

            # Idle: no live viewers and no recent still requests (a speak in
            # progress counts as use; it hangs up by itself).
            if (
                server.client_count == 0
                and not self._speak_owns_session()
                and now - self._last_use >= _IDLE_TIMEOUT
            ):
                async with self._lock:
                    if (
                        self._server
                        and self._server.client_count == 0
                        and not self._speak_owns_session()
                        and loop.time() - self._last_use >= _IDLE_TIMEOUT
                    ):
                        _LOGGER.info("No viewers/stills — stopping live video call")
                        await self._stop_session("idle")
                return

    async def async_shutdown(self) -> None:
        """Tear down the call and RTSP server (on entry unload)."""
        async with self._lock:
            if self._idle_task and not self._idle_task.done():
                self._idle_task.cancel()
            await self._stop_session("shutdown", resume_listener=False)
            if self._server is not None:
                with contextlib.suppress(Exception):
                    await self._server.stop()
                self._server = None
