"""Video call signaling via TCP to trigger UDP video streaming."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import struct
import time
from collections import deque
from collections.abc import Callable

from .channels import Channel, ChannelType
from .client import IconaBridgeClient
from .const import is_verbose_logging
from .ctpp import ctpp_init_sequence
from .exceptions import VideoCallError
from .models import DeviceConfig
from .protocol import (
    encode_answer_peer,
    encode_call_ack,
    encode_call_accepted,
    encode_call_init,
    encode_call_response_ack,
    encode_door_open_during_video,
    encode_rtpc2_ready,
    encode_rtpc_link,
    encode_talk_enable,
    encode_video_config,
)
from .rtp_receiver import RtpReceiver
from .rtsp_server import LocalRtspServer
from .vip_listener import (
    ACTION_IN_ALERTING,
    PREFIX_CALL_INIT,
    PREFIX_VIP_EVENT,
    _extract_caller,
    parse_ctpp_message,
)

_LOGGER = logging.getLogger(__name__)

VIDEO_RESPONSE_TIMEOUT = 5.0  # device can be slow to respond to CTPP signaling
VIDEO_SESSION_TIMEOUT = 120.0
VIDEO_READY_TIMEOUT = 6.0  # max wait for first media packet after signaling

# CTPP message counter increment constants (from PCAP analysis)
# Bytes [4-5] in CTPP body encode two independent sub-counters:
#   byte[4] increments by 1 → adds 0x00010000 to the LE32 timestamp field
#   byte[5] increments by 1 → adds 0x01000000 to the LE32 timestamp field
_CTR_INCR_BYTE4 = 0x00010000  # only byte[4] increments
_CTR_INCR_BYTE5 = 0x01000000  # only byte[5] increments
_CTR_INCR_BOTH = 0x01010000  # both byte[4] and byte[5] increment

# Talk-back (TX audio) on outbound "speak" sessions: G.711 A-law, 8 kHz,
# 20 ms per RTP packet (160 bytes), PT 8: the same framing as the inbound
# audio sender and the app.
_TALK_FRAME_SECONDS = 0.020
_TALK_SILENCE = bytes([0xD5]) * 160  # A-law code of linear 0
# Fell this far behind schedule (event-loop stall): resync instead of bursting.
_TALK_MAX_LAG = 0.2
# Consecutive send failures (~0.5 s) after which the talk sender gives up.
_TALK_MAX_ERRORS = 25
# In-call door open (0x1840/0x000D): how long to wait for the panel's ACK.
_DOOR_ACK_TIMEOUT = 2.0
# A CALL_END 0x1840/0x0003 sub 0x000E this soon after our door command is the
# panel ending the call because the relay fired (PCAP of the app's live view).
_DOOR_CONFIRM_WINDOW = 5.0
# Call-end frame queued while answering a preview: look this long at the video
# to tell a call that is really over from a door frame alone (26/10).
_ENDED_VIDEO_CHECK = 0.3


class VideoCallSession:
    """Manages the TCP signaling and UDP video for a video call.

    Uses the coordinator's shared IconaBridgeClient so video signaling
    and VIP event listening coexist on a single TCP connection. The
    sequence (from PCAP analysis):

    1. Open CTPP channel with apt address (client already authenticated)
    2. Send CTPP init + call initiation
    3. ACK device responses, wait for call acceptance
    4. Open UDPM channel (trailing_byte=1) — extract token from response
    5. Send codec negotiation
    6. Open 2x RTPC channels (trailing_byte=1)
    7. Send RTPC link (using RTPC1 request_id)
    8. Send video config trigger (using RTPC2 request_id)
    9. Start RTP receiver with dynamic IDs from channel setup
    10. Auto-timeout after ~120s
    """

    # Channel names opened by start() — cleaned up on stop without
    # disconnecting the shared client.
    _VIDEO_CHANNEL_NAMES = ("CTPP", "CSPB", "UDPM", "RTPC", "RTPC2", "RTPC_DEVICE", "RTPC_DEVICE_REEST")

    def __init__(
        self,
        client: IconaBridgeClient,
        config: DeviceConfig,
        auto_timeout: bool = True,
        rtsp_server: LocalRtspServer | None = None,
        snapshot_only: bool = False,
        on_call_end: Callable[[], None] | None = None,
        on_timeout: Callable[[], None] | None = None,
        on_ring: Callable[[dict], None] | None = None,
        talk: bool = False,
        talk_transport: str = "auto",
    ) -> None:
        self._client = client
        self._config = config
        self._auto_timeout = auto_timeout
        self._snapshot_only = snapshot_only
        self._external_rtsp = rtsp_server is not None
        self._on_call_end = on_call_end
        self._on_timeout = on_timeout
        # Forwarded when a NEW ring is seen on CTPP during this call — the
        # doorbell listener is paused while we hold CTPP, so rings arriving
        # mid-call would otherwise be lost. Deduped per caller.
        self._on_ring = on_ring
        self._last_ring_fired: dict[str, float] = {}
        self._ring_dedup_window = 8.0
        self._rtp_receiver: RtpReceiver | None = None
        self._rtsp_server: LocalRtspServer | None = rtsp_server
        self._timeout_task: asyncio.Task | None = None
        self._tcp_task: asyncio.Task | None = None
        self._tcp_audio_task: asyncio.Task | None = None
        self._ctpp_task: asyncio.Task | None = None
        self._active = False
        # req_id of the device's own RTPC channel (set during inbound answer);
        # used to target outbound PCMA audio frames back to the device.
        self._device_rtpc_req_id: int = 0
        # True when this session opened CTPP itself (notifications OFF).
        # False when reusing the coordinator-opened channel (notifications ON).
        # Determines whether _cleanup removes CTPP/CSPB from the client registry.
        self._owns_ctpp: bool = False
        # Shared CTPP counter — updated by _ctpp_monitor_loop and read by
        # async_open_door_on_ctpp.  Protected by _ctpp_lock so door open and
        # keepalive ACKs don't collide on the wire.
        self._call_counter: int = 0
        self._ctpp_lock: asyncio.Lock = asyncio.Lock()
        # Talk-back ("speak") mode, outbound only. Once the call is answered
        # (0x70 sent) a sender streams A-law RTP to the device's own RTPC
        # channel every 20 ms: queued frames when there are any, silence
        # otherwise (the app keeps streaming too; some firmware only opens
        # the audio path while it receives our audio). Off for live view and
        # snapshots, which stay exactly as before.
        self._talk = talk
        self._talk_transport_pref = talk_transport  # "auto" | "tcp" | "udp"
        self._talk_transport: str | None = None
        self._answer_sent = asyncio.Event()
        self._device_rtpc: Channel | None = None
        self._talk_task: asyncio.Task | None = None
        self._talk_frames: deque[bytes] = deque()
        self._talk_sent = 0  # RTP packets sent (speech + silence)
        self._talk_payload_sent = 0  # queued (speech) frames sent
        # Diagnostic: the device sent its own 0x70 (it "reciprocates" when it
        # accepts our audio, per the app captures).
        self._device_talk_ack = False
        # Inbound calls (start_inbound): whether call_accepted was sent, and
        # what a preview needs to be accepted later (speak in "answer" mode).
        self._inbound = False
        self._inbound_accepted = False
        self._inbound_ctx: dict | None = None
        # Flags of a call-end frame found while answering (the call is over).
        self.call_already_ended: int | None = None
        # One answer at a time (speak and an in-call door open may race), and
        # end the call on the device's CALL_END once HA answered it that way.
        self._accept_lock = asyncio.Lock()
        self._end_on_call_end = False
        # In-call door open: one at a time; the pending ACK and what the panel
        # said about the door (0x1860/0x0003 flags) while it was pending.
        self._door_lock = asyncio.Lock()
        self._door_ack: asyncio.Future | None = None
        self._door_addr = ""
        self._door_event_flags: int | None = None
        # The relay really fired: the panel ended the call with CALL_END sub
        # 0x000E right after our 0x000D (the ACK alone proves nothing: on
        # 26/10 a TAP that did not open got one too).
        self.door_confirmed = False
        self._door_sent_at = 0.0

    @property
    def active(self) -> bool:
        """Return True if the video session is currently active."""
        return self._active

    @property
    def rtp_receiver(self) -> RtpReceiver | None:
        """Return the RTP receiver for getting video frames."""
        return self._rtp_receiver

    @property
    def rtsp_server(self) -> LocalRtspServer | None:
        """Return the RTSP server for go2rtc stream_source."""
        return self._rtsp_server

    def _ts(self) -> int:
        """Return current timestamp for CTPP messages."""
        return int(time.time()) & 0xFFFFFFFF

    # ------------------------------------------------------------------
    # CTPP signaling helpers (shared by start() and _inline_reestablish)
    # ------------------------------------------------------------------

    async def _run_codec_exchange(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        call_counter: int,
    ) -> int:
        """Drive the codec exchange until call_accepted (0x0002). Returns counter."""
        for i in range(10):
            resp = await client.read_response(ctpp, timeout=VIDEO_RESPONSE_TIMEOUT)
            if not resp or len(resp) < 2:
                break
            msg_type = struct.unpack_from("<H", resp, 0)[0]
            action = struct.unpack_from(">H", resp, 6)[0] if len(resp) >= 8 else 0
            if is_verbose_logging():
                _LOGGER.debug(
                    "Codec exchange %d: type=0x%04X action=0x%04X our_counter=0x%08X",
                    i,
                    msg_type,
                    action,
                    call_counter,
                )
            if msg_type in (0x1860, 0x1800):
                continue
            if msg_type == 0x1840:
                if action == 0x0008:
                    call_counter += _CTR_INCR_BOTH
                    await client.send_binary(
                        ctpp,
                        encode_call_response_ack(our_addr, entrance_addr, call_counter),
                    )
                elif action == 0x0002:
                    call_counter += _CTR_INCR_BYTE5
                    await client.send_binary(
                        ctpp,
                        encode_call_response_ack(our_addr, entrance_addr, call_counter),
                    )
                    if is_verbose_logging():
                        _LOGGER.debug("Codec exchange complete (call accepted)")
                    return call_counter
                else:
                    call_counter += _CTR_INCR_BYTE4
                    await client.send_binary(
                        ctpp,
                        encode_call_response_ack(our_addr, entrance_addr, call_counter),
                    )
        _LOGGER.warning("Codec exchange did not reach call-accepted state")
        return call_counter

    async def _ack_device_rtpc_link(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        call_counter: int,
    ) -> int:
        """Read and ACK the device's RTPC link (action 0x000A). Returns counter.

        PCAP-verified: during initial start the device sends 0x1840/0x000A;
        during inline re-establishment it sends 0x1860/0x000A instead.
        Both are accepted.
        """
        for _ in range(5):
            resp = await client.read_response(ctpp, timeout=VIDEO_RESPONSE_TIMEOUT)
            if not resp or len(resp) < 2:
                break
            msg_type = struct.unpack_from("<H", resp, 0)[0]
            action = struct.unpack_from(">H", resp, 6)[0] if len(resp) >= 8 else 0
            if msg_type in (0x1840, 0x1860) and action == 0x000A:
                call_counter += _CTR_INCR_BYTE5
                await client.send_binary(
                    ctpp,
                    encode_call_response_ack(our_addr, entrance_addr, call_counter),
                )
                return call_counter
            if msg_type == 0x1800:
                continue
        return call_counter

    async def start(self) -> RtpReceiver:
        """Execute the full TCP signaling sequence and start UDP receiver.

        The signaling flow matches the real Android app (from PCAP analysis):
        1. Auth → open CTPP + CSPB → CTPP init → ACK device responses
        2. Call init → open UDPM → START UDP CONTROL
        3. Wait for call ACK → codec ack → codec exchange (with UDP running)
        4. Open 2x RTPC → RTPC link → video config → device RTPC → start media
        """
        client = self._client

        try:
            apt_addr = self._config.apt_address
            apt_sub = self._config.apt_subaddress
            # our_addr = full address of the HA/app unit (apt_address + apt_subaddress)
            # This appears as the FIRST address in all CTPP video messages (PCAP-verified).
            our_addr = f"{apt_addr}{apt_sub}"
            # entrance_addr = the entrance panel address from entrance-address-book.
            # This appears as the SECOND address in call-phase messages (PCAP-verified).
            # For init-phase ACKs, we use apt_addr (without sub) as second address.
            entrance_addr = self._config.caller_address or our_addr
            if not self._config.caller_address:
                _LOGGER.warning(
                    "entrance-address-book is empty — using our_addr as entrance_addr. "
                    "Video call may fail if device requires a distinct entrance address."
                )

            # Step 1: Open CTPP + CSPB channels (PCAP shows both are needed).
            # CRITICAL: Use ChannelType.UAUT (type=7) for ALL channels — the real
            # Android app uses type=7 for everything. Using CTPP=16 may cause the
            # device to handle video calls incorrectly.
            #
            # The coordinator opens and initialises CTPP at setup when notifications
            # are enabled; this session reuses it directly. When notifications are
            # disabled, no CTPP exists yet — open and init it here, and take
            # ownership so _cleanup closes it when the session ends.
            ctpp = client.get_channel("CTPP")
            if ctpp is not None:
                self._owns_ctpp = False
                if is_verbose_logging():
                    _LOGGER.debug(
                        "Reusing coordinator CTPP channel (server_id=%d) — skipping ctpp_init (already registered)",
                        ctpp.server_channel_id,
                    )
                if client.get_channel("CSPB") is None:
                    await client.open_channel("CSPB", ChannelType.UAUT)
                # Use a fresh timestamp; call phase needs a different session ID
                # from the VIP init phase (bytes 2-3 of the CTPP timestamp).
                init_ts = self._ts()
            else:
                self._owns_ctpp = True
                ctpp = await client.open_channel("CTPP", ChannelType.UAUT, extra_data=our_addr)
                await client.open_channel("CSPB", ChannelType.UAUT)
                # Step 2: CTPP init + ACK pair (only needed on a fresh channel)
                init_ts = self._ts()
                await ctpp_init_sequence(
                    client,
                    ctpp,
                    apt_addr,
                    apt_sub,
                    our_addr,
                    init_ts,
                    response_timeout=VIDEO_RESPONSE_TIMEOUT,
                )

            # PCAP shows phone proceeds directly to call init after sending ACKs.

            # Register placeholder for device's RTPC channel early — the device
            # opens its own RTPC DURING codec exchange (~4.8s after connect),
            # before we even open our RTPC1/RTPC2 channels. Must be registered
            # here so _dispatch captures it immediately. With the request_id==0
            # filter in _dispatch, this placeholder will NOT steal RTPC1/RTPC2
            # open responses (those go through the request_id!=0 path).
            device_rtpc = client.register_placeholder_channel("RTPC_DEVICE")

            # Step 3: Send call init — uses a new "session" timestamp
            # (PCAP shows call phase uses different session from init phase)
            # PCAP-verified: call_init uses (our_addr, entrance_addr).
            #
            # CRITICAL: The device uses bytes[2-3] of the CTPP body as a
            # "session ID". Init and call phases MUST have different session IDs
            # (different low 16 bits of the timestamp). Since both init_ts and
            # call_ts are generated from int(time.time()) within the same second,
            # they'll be identical. We add 1 to the low byte to force a different
            # session ID while keeping the same counter starting point (high 16 bits).
            call_ts = (init_ts + 1) & 0xFFFFFFFF
            call_init = encode_call_init(our_addr, entrance_addr, call_ts)
            await client.send_binary(ctpp, call_init)

            # Step 4: Open UDPM immediately after call init (PCAP order)
            udpm = await client.open_channel("UDPM", ChannelType.UAUT, trailing_byte=1)
            udpm_token = 0x0000
            if len(udpm.open_response_body) >= 18:
                udpm_token = struct.unpack_from("<H", udpm.open_response_body, 16)[0]
                if is_verbose_logging():
                    _LOGGER.debug("UDPM token: 0x%04X", udpm_token)

            # PCAP-verified: control_req_id = UDPM server_channel_id (device-assigned).
            control_req_id = udpm.server_channel_id
            receiver = RtpReceiver(
                client.host,
                client.port,
                control_req_id=control_req_id,
                media_req_id=0,  # set later after RTPC2 opens
                udpm_token=udpm_token,
            )
            if not self._snapshot_only:
                if self._rtsp_server and self._external_rtsp:
                    # Reuse coordinator-owned persistent RTSP server
                    self._rtsp_server.reset()
                    rtsp_server = self._rtsp_server
                else:
                    # Standalone mode — create and own our own server
                    rtsp_server = LocalRtspServer()
                    await rtsp_server.start()
                    self._rtsp_server = rtsp_server
                receiver.attach_rtsp_queues(
                    rtsp_server.nal_queue,
                    rtsp_server.audio_queue,
                    rtp_queue=rtsp_server.rtp_queue,
                )

            # Open UDP socket + send 2 discovery packets so the device knows
            # our UDP port before video config. Start keepalive immediately so
            # the device doesn't time out during the codec exchange / RTPC setup
            # (which can take 10+ seconds). The PCAP shows keepalives sent
            # throughout the entire session, not just after video starts.
            await receiver.start_control()
            receiver.start_keepalive()
            self._rtp_receiver = receiver

            # Step 5: Wait for device ACK of call init, then send codec msg
            # CRITICAL: Each side maintains its OWN counter independently.
            # PCAP shows client uses call_ts-based counter that increments
            # by 0x10000 per message sent, while device has a completely
            # different counter. We must NEVER adopt the device's counter.
            call_counter = call_ts

            resp1 = await client.read_response(ctpp, timeout=VIDEO_RESPONSE_TIMEOUT)
            if resp1 and len(resp1) >= 6:
                dev_counter = struct.unpack_from("<I", resp1, 2)[0]
                if is_verbose_logging():
                    _LOGGER.debug(
                        "Call response: %d bytes, dev_counter=0x%08X, our_counter=0x%08X",
                        len(resp1),
                        dev_counter,
                        call_counter,
                    )

            # Send codec msg with our own incremented counter.
            # PCAP-verified: only +0x00010000 between call_init and codec
            # (byte[4] increments by 1, byte[5] stays).
            call_counter += _CTR_INCR_BYTE4
            codec_ack = encode_call_ack(our_addr, entrance_addr, call_counter)
            await client.send_binary(ctpp, codec_ack)

            # Step 6: Handle codec exchange (shared helper).
            call_counter = await self._run_codec_exchange(client, ctpp, our_addr, entrance_addr, call_counter)

            # Step 7: Open 2 RTPC channels (PCAP shows phone opens both)
            # RTPC1 is used for the link message, RTPC2 for video media
            rtpc1 = await client.open_channel("RTPC", ChannelType.UAUT, trailing_byte=1)
            rtpc2 = await client.open_channel(
                "RTPC2",
                ChannelType.UAUT,
                trailing_byte=1,
                wire_name="RTPC",
            )
            # PCAP-verified: media_req_id = RTPC2 server_channel_id (device-assigned).
            # In PCAP: RTPC2 server_channel_id=0x606E (= UDPM server_channel_id + 2).
            media_req_id = rtpc2.server_channel_id
            # Incoming AUDIO RTP (G.711 PCMA, PT8) arrives on RTPC1's channel
            # (media_req_id-1), multiplexed on the same UDP socket as video.
            # Tell the receiver to accept it too (PCAP-verified).
            receiver.set_audio_req_id(rtpc1.server_channel_id)
            if is_verbose_logging():
                _LOGGER.debug(
                    "RTPC channels: rtpc1(audio)=0x%04X, rtpc2(media)=0x%04X",
                    rtpc1.server_channel_id,
                    media_req_id,
                )

            # Step 8: Send RTPC link (references RTPC1)
            # PCAP shows RTPC link reuses the last counter (no increment).
            # Must use server_channel_id (device-assigned), not local request_id.
            rtpc_link = encode_rtpc_link(our_addr, entrance_addr, rtpc1.server_channel_id, call_counter)
            await client.send_binary(ctpp, rtpc_link)
            if is_verbose_logging():
                _LOGGER.debug("Sent RTPC link, our_counter=0x%08X", call_counter)

            # Step 8b: Send video config IMMEDIATELY after RTPC link — BEFORE waiting
            # for device RTPC. PCAP shows the Android app sends VIDEO_CONFIG as message
            # #17 while device opens its own RTPC at #18. If we wait for device RTPC
            # first, the device doesn't enter the correct state for HANGUP/ZERO recovery.
            # PCAP: call_counter +0x00010000 (byte[4] +1) for video config DATA message.
            call_counter += _CTR_INCR_BYTE4
            vid_config = encode_video_config(our_addr, entrance_addr, media_req_id, call_counter)
            await client.send_binary(ctpp, vid_config)
            if is_verbose_logging():
                _LOGGER.debug("Sent video config (before device RTPC), our_counter=0x%08X", call_counter)

            # Step 9: Now wait for device to open its own RTPC channel, then ACK
            # its CTPP RTPC link message.
            # PCAP sequence after phone's RTPC link + video config:
            #   device CHAN_OPEN (RTPC) → auto-handled by dispatcher
            #   device CTPP 0x1840/0x000A (device's RTPC link)
            #   phone ACK 0x1800 → call_counter +0x01000000 (only byte[5] +1)
            #   device ACK 0x1800

            # Wait for device to open its own RTPC channel
            try:
                await asyncio.wait_for(device_rtpc.open_event.wait(), timeout=VIDEO_RESPONSE_TIMEOUT)
                if is_verbose_logging():
                    _LOGGER.debug("Device opened RTPC: 0x%04X", device_rtpc.server_channel_id)
            except TimeoutError:
                _LOGGER.warning("Device RTPC channel not received within timeout")
                raise VideoCallError("Device RTPC channel not received")
            # The device's own RTPC is where our TX audio goes (talk sessions).
            self._device_rtpc = device_rtpc

            # Read and ACK device's CTPP RTPC link (0x1840/0x000A)
            call_counter = await self._ack_device_rtpc_link(client, ctpp, our_addr, entrance_addr, call_counter)

            # Step 9b: Send initial HANGUP/ZERO (0x1840/0x0000) to signal "call accepted".
            # PCAP (PCAPdroid_06_Mar_23_28_05): app sends this ~3s after video setup.
            # Device ACKs with 0x1800/0x0000. This tells the device we're ready.
            # The 30s lease timer runs from here; CALL_END arrives ~30s later.
            # Session restart on CALL_END is handled in _ctpp_monitor_loop.
            call_counter += _CTR_INCR_BYTE4
            hangup_zero = encode_call_response_ack(our_addr, entrance_addr, call_counter, prefix=0x1840)
            await client.send_binary(ctpp, hangup_zero)
            if is_verbose_logging():
                _LOGGER.debug("Sent initial HANGUP/ZERO (call accepted), counter=0x%08X", call_counter)

            # Step 10: Set media req_id and start decoder immediately.
            receiver.set_media_req_id(media_req_id)
            await receiver.start_media()

            # Step 10b: Start TCP video reader for RTPC2 BEFORE the CTPP
            # monitor so no firmware variant that streams over TCP loses
            # initial frames.  Start the CTPP monitor immediately after so
            # any device 0x1840 keepalives arriving during the readiness
            # wait are ACKed instead of lingering in the channel buffer.
            self._call_counter = call_counter
            self._tcp_task = asyncio.create_task(self._tcp_video_loop(client, rtpc2, receiver))
            # Audio (G.711 PCMA, PT8) is multiplexed onto RTPC1 as its own TCP
            # channel — 172-byte packets (12-byte RTP + 160-byte A-law frame).
            # In UDP-media firmware audio shares the video socket and this loop
            # simply sees no packets; on TCP-media firmware it is the only place
            # the audio RTP is read.  receive_tcp_rtp PT-routes it to the audio
            # queue, so the same loop handles both channels.
            self._tcp_audio_task = asyncio.create_task(self._tcp_video_loop(client, rtpc1, receiver))
            self._ctpp_task = asyncio.create_task(
                self._ctpp_monitor_loop(
                    client,
                    ctpp,
                    our_addr,
                    entrance_addr,
                    call_counter,
                    rtpc1.server_channel_id,
                    media_req_id,
                )
            )
            self._active = True

            # Step 10d: Readiness gate — wait until the first real NAL has
            # been queued before reporting the session as ready.  Without
            # this, `Video ready in 1.5s` logs before a single video packet
            # has arrived and downstream clients see a silent stream.
            got_media = await receiver.wait_for_first_video(VIDEO_READY_TIMEOUT)
            if not got_media:
                udp = receiver.udp_media_packet_count
                tcp = receiver.tcp_media_packet_count
                _LOGGER.warning(
                    "No media received within %.1fs (udp=%d tcp=%d) — "
                    "signaling succeeded but device is not sending RTP. "
                    "Check NAT/firewall for UDP, or firmware may need TCP.",
                    VIDEO_READY_TIMEOUT,
                    udp,
                    tcp,
                )
            elif is_verbose_logging():
                _LOGGER.info(
                    "Video flowing via %s transport",
                    "TCP" if receiver.tcp_media_packet_count else "UDP",
                )

            # Step 10e: Answer sequence — only runs AFTER video is flowing
            # so it cannot delay the first frame.  Fires 0x1840/0x0070 which
            # the device needs to transition into the "call answered" state;
            # audio RTPC follows at the next renewal cycle.
            if got_media:
                asyncio.create_task(
                    self._run_answer_sequence(
                        client,
                        ctpp,
                        our_addr,
                        entrance_addr,
                        apt_addr,
                        call_counter,
                        media_req_id,
                    )
                )

            if is_verbose_logging():
                _LOGGER.debug(
                    "RTP receiver fully started: control=0x%04X, media=0x%04X, udpm_token=0x%04X",
                    control_req_id,
                    media_req_id,
                    udpm_token,
                )

            # Step 11: Auto-timeout (skipped when stream handles lifecycle)
            if self._auto_timeout:
                self._timeout_task = asyncio.create_task(self._auto_timeout_loop())

            if is_verbose_logging():
                _LOGGER.info(
                    "Video call session started: our_addr=%s entrance=%s",
                    our_addr,
                    entrance_addr,
                )
            return receiver

        except Exception as e:
            await self._cleanup()
            raise VideoCallError(f"Failed to start video call: {e}") from e

    async def stop(self, reason: str = "user request") -> None:
        """Stop the video session and clean up."""
        if is_verbose_logging():
            _LOGGER.info("Stopping video call session (%s)", reason)
        await self._cleanup()

    async def _cleanup(self) -> None:
        """Clean up all resources.

        Tasks are cancelled with a 2s timeout on each await. Without the
        timeout, awaiting a cancelled task stuck on a dead TCP connection can
        freeze the event loop for 30-40s (observed on Python 3.14/aarch64).
        """
        self._active = False

        for task_attr in (
            "_talk_task",
            "_timeout_task",
            "_tcp_task",
            "_tcp_audio_task",
            "_ctpp_task",
        ):
            task = getattr(self, task_attr)
            setattr(self, task_attr, None)
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await asyncio.wait([task], timeout=2.0)

        receiver, self._rtp_receiver = self._rtp_receiver, None
        if receiver:
            with contextlib.suppress(Exception):
                await receiver.stop()

        if self._rtsp_server and not self._external_rtsp:
            with contextlib.suppress(Exception):
                await self._rtsp_server.stop()
            self._rtsp_server = None

        # Release our channels back to the shared client but do NOT disconnect —
        # the coordinator owns the TCP connection and other consumers (VIP
        # listener, door open, PUSH) are still using it.
        # CTPP and CSPB are only removed when this session opened them itself
        # (notifications OFF). When the coordinator opened them, they outlive
        # the video session so the VIP listener can reattach afterwards.
        client = self._client
        if client:
            for name in self._VIDEO_CHANNEL_NAMES:
                if name in ("CTPP", "CSPB") and not self._owns_ctpp:
                    continue
                client.remove_channel(name)

    @staticmethod
    async def _tcp_video_loop(
        client: IconaBridgeClient,
        rtpc2: Channel,
        receiver: RtpReceiver,
    ) -> None:
        """Read TCP RTP packets from RTPC2 and feed to receiver.

        The device sends RTP directly over TCP on the RTPC2 channel.
        The client strips the ICONA header before queuing, so the queued
        body is raw RTP starting with 0x80 (RTP version 2).
        """
        try:
            while receiver.running:
                data = await client.read_response(rtpc2, timeout=2.0)
                if data and len(data) >= 12:
                    receiver.receive_tcp_rtp(data)
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.debug("TCP video loop error", exc_info=True)

    async def _ctpp_monitor_loop(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        call_counter: int,
        rtpc1_server_id: int,
        media_req_id: int,
    ) -> None:
        """Read and ACK incoming CTPP messages during the active video session.

        The device sends periodic 0x1840 messages throughout the call:
        - 0x0000: keepalive — ACK with bare 0x1800
        - 0x0003 / sub=0x0000: CALL_END — device lease timer expired; perform
            inline re-establishment (same TCP connection, no session restart).
        - 0x0003 / sub=0x000E: CALL_END triggered by door-open relay activation.
            PCAP-verified (camera_feed_with_open_door_local.pcap): same renewal
            sequence as the periodic CALL_END — NOT a bare ACK.
        0x1800 device ACKs are silently ignored.
        """
        try:
            while self._active:
                resp = await client.read_response(ctpp, timeout=2.0)
                if not resp or len(resp) < 2:
                    continue
                msg_type = struct.unpack_from("<H", resp, 0)[0]
                action = struct.unpack_from(">H", resp, 6)[0] if len(resp) >= 8 else 0
                sub = struct.unpack_from(">H", resp, 8)[0] if len(resp) >= 10 else 0
                # A NEW incoming ring during our active call — a re-press, or a
                # ring from a different entrance — arrives as 0x18C0/0x0028 or
                # 0x1860/0x0001 (IN_ALERTING). The doorbell listener is paused
                # while we hold CTPP, so this is the only place mid-call rings
                # can be observed; forward them so they still reach HA. This
                # runs before the ACK handling below (0x1860 still gets its
                # bare ACK; 0x18C0 is left as-is, and the per-caller dedup caps
                # any device retransmit storm).
                if self._on_ring is not None and (
                    msg_type == PREFIX_CALL_INIT
                    or (msg_type == PREFIX_VIP_EVENT and action == ACTION_IN_ALERTING)
                ):
                    self._forward_ring(resp)
                # The peer frame carries 0x0070 after its inner length (bytes
                # 8-9), where other frames have their flags.
                if msg_type in (0x1840, 0x1860) and 0x0070 in (action, sub):
                    if self._talk and not self._device_talk_ack:
                        _LOGGER.debug("Talk: device sent its own 0x%04X peer (0x70)", msg_type)
                    self._device_talk_ack = True
                if msg_type == 0x1840:
                    if (
                        action == 0x0003
                        and sub == 0x000E
                        and self._door_sent_at
                        and time.monotonic() - self._door_sent_at < _DOOR_CONFIRM_WINDOW
                    ):
                        self.door_confirmed = True  # CALL_END caused by the relay
                    if action == 0x0003 and self._end_on_call_end:
                        # Incoming call answered by speak or by an in-call door
                        # open, or a speak call that opened the door: CALL_END
                        # ends it. Never re-establish as an outbound call here.
                        async with self._ctpp_lock:
                            call_counter += _CTR_INCR_BYTE5
                            self._call_counter = call_counter
                            await client.send_binary(
                                ctpp, encode_call_response_ack(our_addr, entrance_addr, call_counter)
                            )
                        _LOGGER.debug("Talk: incoming call ended by the device (sub=0x%04X)", sub)
                        self._active = False
                        return
                    if action == 0x0003:
                        # CALL_END (sub=0x0000 = timer, sub=0x000E = door-open triggered)
                        if is_verbose_logging():
                            _LOGGER.debug(
                                "CTPP monitor: CALL_END received (sub=0x%04X) — re-establishing",
                                sub,
                            )
                        try:
                            async with self._ctpp_lock:
                                call_counter = await self._inline_reestablish(
                                    client,
                                    ctpp,
                                    our_addr,
                                    entrance_addr,
                                    rtpc1_server_id,
                                    media_req_id,
                                    call_counter,
                                )
                                self._call_counter = call_counter
                            if is_verbose_logging():
                                _LOGGER.debug("CTPP monitor: re-established, lease renewed")
                        except Exception:
                            _LOGGER.warning(
                                "CTPP monitor: inline re-establish failed — falling back to full session restart",
                                exc_info=True,
                            )
                            self._active = False
                            if self._on_call_end:
                                self._on_call_end()
                            return
                    else:
                        # Keepalive (0x0000) or any other non-CALL_END 0x1840 — bare ACK.
                        async with self._ctpp_lock:
                            call_counter += _CTR_INCR_BYTE4
                            self._call_counter = call_counter
                            ack = encode_call_response_ack(our_addr, entrance_addr, call_counter)
                            await client.send_binary(ctpp, ack)
                        if is_verbose_logging():
                            _LOGGER.debug(
                                "CTPP monitor: ACKed 0x1840/0x%04X (sub=0x%04X), counter=0x%08X",
                                action,
                                sub,
                                call_counter,
                            )
                elif msg_type == 0x1860:
                    # Device 0x1860 messages during an active session (e.g.
                    # 0x000A RTPC link that _ack_device_rtpc_link missed, or
                    # other device-initiated messages) — bare ACK.
                    if action == 0x0003 and self._door_ack is not None:
                        self._door_event_flags = sub  # door FSM after our open
                    async with self._ctpp_lock:
                        call_counter += _CTR_INCR_BYTE4
                        self._call_counter = call_counter
                        ack = encode_call_response_ack(our_addr, entrance_addr, call_counter)
                        await client.send_binary(ctpp, ack)
                    if is_verbose_logging():
                        _LOGGER.debug(
                            "CTPP monitor: ACKed 0x1860/0x%04X, counter=0x%08X",
                            action,
                            call_counter,
                        )
                elif msg_type == 0x1800:
                    # Device ACK: no response needed. While an in-call door
                    # open waits, the ACK sent by the door's unit settles it.
                    self._note_door_ack(resp)
                elif is_verbose_logging():
                    _LOGGER.debug(
                        "CTPP monitor: unexpected type=0x%04X (%d bytes)",
                        msg_type,
                        len(resp),
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.debug("CTPP monitor loop error", exc_info=True)

    def _forward_ring(self, resp: bytes) -> None:
        """Parse a ring frame seen mid-call and forward it to the events manager.

        Deduped per caller (the device may retransmit an unACKed ring) so a
        held ring produces at most one event per _ring_dedup_window.
        """
        parsed = parse_ctpp_message(resp)
        if not parsed:
            return
        caller = _extract_caller(
            parsed.get("addresses", []), self._config.apt_address
        )
        # 6741W floor call carries the entrance address; the "FF" origin tag is
        # the only discriminator — report our apartment address so it routes to
        # the Floor call entity (issue #45), matching the VIP listener.
        if parsed.get("call_tag") == b"FF" and self._config.apt_address:
            caller = self._config.apt_address
        now = time.monotonic()
        if now - self._last_ring_fired.get(caller, 0.0) < self._ring_dedup_window:
            return
        self._last_ring_fired[caller] = now
        if is_verbose_logging():
            _LOGGER.debug(
                "CTPP monitor: ring from %s during active call — forwarding", caller
            )
        try:
            self._on_ring(
                {"caller": caller, "source": "local", "event_type": "ring"}
            )
        except Exception:
            _LOGGER.exception("Error forwarding mid-call ring")

    async def _inline_reestablish(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        rtpc1_server_id: int,
        media_req_id: int,
        call_counter: int,
    ) -> int:
        """Perform inline re-establishment after CALL_END, returning updated counter.

        Full media session restart on the same TCP/CTPP connection — verified
        working in commit efc75d91 on the dedicated-connection architecture and
        confirmed to work identically on the shared-connection architecture.

        Sequence (from working reference implementation):
        1. ACK CALL_END (+byte5)
        2. CTPP init + ACK pair (resets device-side session state)
        3. New call_init + codec exchange (reuse existing RTPC channels)
        4. RTPC_LINK + VIDEO_CONFIG
        5. Wait for device RTPC, ACK its link
        6. HANGUP/ZERO (0x1840/0x0000) — signals "call accepted" to device
        7. Renewal peer/accept (0x1860/0x0070) — triggers audio RTPC reopening
        8. Drain stale RTSP queues
        """
        apt_addr = our_addr[:-1]
        apt_sub = int(our_addr[-1])

        # 1. ACK CALL_END
        call_counter += _CTR_INCR_BYTE5
        await client.send_binary(
            ctpp,
            encode_call_response_ack(our_addr, entrance_addr, call_counter),
        )

        # 2. CTPP init + ACK pair
        init_ts = self._ts()
        await ctpp_init_sequence(
            client,
            ctpp,
            apt_addr,
            apt_sub,
            our_addr,
            init_ts,
            response_timeout=VIDEO_RESPONSE_TIMEOUT,
        )

        # 3. Placeholder for device's new RTPC channel
        device_rtpc = client.register_placeholder_channel("RTPC_DEVICE_REEST")

        # 4. Call init + codec ACK + codec exchange (resets call counter to new ts)
        call_ts = (init_ts + 1) & 0xFFFFFFFF
        call_counter = call_ts
        await client.send_binary(ctpp, encode_call_init(our_addr, entrance_addr, call_ts))
        await client.read_response(ctpp, timeout=VIDEO_RESPONSE_TIMEOUT)
        call_counter += _CTR_INCR_BYTE4
        await client.send_binary(ctpp, encode_call_ack(our_addr, entrance_addr, call_counter))
        call_counter = await self._run_codec_exchange(client, ctpp, our_addr, entrance_addr, call_counter)

        # 5. RTPC_LINK + VIDEO_CONFIG (reuse existing RTPC channels)
        await client.send_binary(
            ctpp,
            encode_rtpc_link(our_addr, entrance_addr, rtpc1_server_id, call_counter),
        )
        call_counter += _CTR_INCR_BYTE4
        await client.send_binary(
            ctpp,
            encode_video_config(our_addr, entrance_addr, media_req_id, call_counter),
        )

        # 6. Wait for device RTPC and ACK its link
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(device_rtpc.open_event.wait(), timeout=VIDEO_RESPONSE_TIMEOUT)
        if device_rtpc.is_open:
            # TX audio (talk sessions) follows the device's newest RTPC.
            self._device_rtpc = device_rtpc
        call_counter = await self._ack_device_rtpc_link(client, ctpp, our_addr, entrance_addr, call_counter)

        # 7. HANGUP/ZERO — signals "call accepted" to device (required for renewal)
        call_counter += _CTR_INCR_BYTE4
        await client.send_binary(
            ctpp,
            encode_call_response_ack(our_addr, entrance_addr, call_counter, prefix=0x1840),
        )

        # 8. Renewal peer/accept (0x1860/0x0070) — triggers audio RTPC reopening
        call_counter += _CTR_INCR_BYTE4
        if self._talk:
            renewal_peer = encode_talk_enable(our_addr, entrance_addr, call_counter, renewal=True)
        else:
            renewal_peer = encode_answer_peer(our_addr, entrance_addr, call_counter, renewal=True)
        await client.send_binary(ctpp, renewal_peer)
        if is_verbose_logging():
            _LOGGER.debug("Re-establish: sent renewal peer/accept (0x1860/0x0070)")

        # 9. Drain stale RTSP queues while keeping RTP seq/ts monotonic
        if self._rtsp_server:
            self._rtsp_server.reset(renewal=True)

        if is_verbose_logging():
            _LOGGER.debug("Re-establish: done, counter=0x%08X", call_counter)
        return call_counter

    async def _run_answer_sequence(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        apt_addr: str,
        call_counter: int,
        media_req_id: int,
    ) -> None:
        """Run answer sequence as a background task (fire-and-forget)."""
        try:
            await self._send_answer_sequence(
                client,
                ctpp,
                our_addr,
                entrance_addr,
                apt_addr,
                call_counter,
                media_req_id,
            )
        except Exception:
            _LOGGER.warning("Answer sequence failed — continuing with video only", exc_info=True)

    async def _send_answer_sequence(
        self,
        client: IconaBridgeClient,
        ctpp: Channel,
        our_addr: str,
        entrance_addr: str,
        apt_addr: str,
        call_counter: int,
        media_req_id: int,
    ) -> None:
        """Send the initial peer/accept (0x70) to signal "call answered".

        PCAP-verified: pressing the phone button in the app sends a single
        0x1840/0x0070 message. Audio does NOT flow yet — audio only starts
        at the next renewal cycle when _inline_reestablish sends 0x1860/0x0070.

        Uses _ctpp_lock and self._call_counter (not the stale call_counter
        parameter) so the counter is in sync with keepalive ACKs that
        _ctpp_monitor_loop may have sent during the 6s readiness wait.
        """
        async with self._ctpp_lock:
            self._call_counter += _CTR_INCR_BYTE4
            if self._talk:
                # Talk sessions send the app's own enable frame (4-byte flag).
                peer = encode_talk_enable(our_addr, entrance_addr, self._call_counter)
            else:
                peer = encode_answer_peer(our_addr, entrance_addr, self._call_counter)
            await client.send_binary(ctpp, peer)
        self._answer_sent.set()
        if is_verbose_logging():
            _LOGGER.info("Answer peer/accept (0x70) sent — audio should start within ~400ms")
        if self._talk and self._active:
            self._start_talk_sender(client)

    # ------------------------------------------------------------------
    # Talk-back (TX audio) for outbound "speak" sessions
    # ------------------------------------------------------------------

    @property
    def talk_mode(self) -> bool:
        """True for a talk ("speak") session."""
        return self._talk

    @property
    def inbound(self) -> bool:
        """True for a device-initiated (ring) call: preview or answered."""
        return self._inbound

    @property
    def inbound_accepted(self) -> bool:
        """True once call_accepted was sent on an inbound call."""
        return self._inbound_accepted

    @property
    def answered(self) -> bool:
        """True once the 0x70 answer (talk enable) has been sent."""
        return self._answer_sent.is_set()

    @property
    def device_talk_ack(self) -> bool:
        """True if the device sent its own 0x70 during this session."""
        return self._device_talk_ack

    @property
    def talk_transport(self) -> str | None:
        """Transport the talk sender uses ("tcp"/"udp"), None until it starts."""
        return self._talk_transport

    @property
    def talk_running(self) -> bool:
        """True while the talk sender is streaming to the device."""
        return self._talk_task is not None and not self._talk_task.done()

    @property
    def talk_pending(self) -> int:
        """Queued speech frames not yet sent."""
        return len(self._talk_frames)

    @property
    def talk_frames_sent(self) -> tuple[int, int]:
        """(RTP packets sent, of which speech frames)."""
        return self._talk_sent, self._talk_payload_sent

    async def wait_answered(self, timeout: float) -> bool:
        """Wait until the 0x70 answer went out (only when video started)."""
        try:
            await asyncio.wait_for(self._answer_sent.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True

    def talk_play(self, frames: list[bytes]) -> None:
        """Queue 160-byte A-law frames; the sender plays one every 20 ms."""
        self._talk_frames.extend(frames)

    def stop_talk(self) -> None:
        """Stop streaming TX audio (the call itself stays up)."""
        self._talk_frames.clear()
        task, self._talk_task = self._talk_task, None
        if task is not None and not task.done():
            task.cancel()

    def _start_talk_sender(self, client: IconaBridgeClient) -> None:
        """Start the TX sender on the device's RTPC (talk sessions only)."""
        if self.talk_running or self._device_rtpc is None:
            if self._device_rtpc is None:
                _LOGGER.warning("Talk: no device RTPC channel, cannot send audio")
            return
        transport = self._talk_transport_pref
        if transport not in ("tcp", "udp"):
            # Mirror the device: send the way its media reaches us.
            rx = self._rtp_receiver
            tcp = rx.tcp_media_packet_count if rx else 0
            udp = rx.udp_media_packet_count if rx else 0
            transport = "tcp" if tcp >= udp else "udp"
        self._talk_transport = transport
        self._talk_task = asyncio.create_task(self._talk_send_loop(client, transport))
        _LOGGER.debug(
            "Talk: TX audio started over %s to device RTPC 0x%04X",
            transport.upper(),
            self._device_rtpc.server_channel_id,
        )

    async def _talk_send_loop(self, client: IconaBridgeClient, transport: str) -> None:
        """Stream A-law RTP (PT 8) to the device every 20 ms, drift-free.

        Each tick sends the next queued speech frame, or silence when none is
        queued, so the panel sees one continuous stream (seq +1, ts +160).
        Pacing follows absolute deadlines, so the average rate stays at
        exactly 50 packets/s whatever the event-loop jitter.
        """
        ssrc = secrets.randbelow(0x100000000)
        seq = 0
        rtp_ts = 0
        errors = 0
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        try:
            while self._active:
                speech = bool(self._talk_frames)
                payload = self._talk_frames.popleft() if speech else _TALK_SILENCE
                rtp = struct.pack(">BBHII", 0x80, 0x08, seq, rtp_ts, ssrc) + payload
                try:
                    sent = await self._send_talk_rtp(client, transport, rtp)
                except Exception:  # noqa: BLE001 - keep streaming, count it
                    sent = False
                if sent:
                    errors = 0
                    self._talk_sent += 1
                    if speech:
                        self._talk_payload_sent += 1
                else:
                    errors += 1
                    if errors >= _TALK_MAX_ERRORS:
                        _LOGGER.warning(
                            "Talk: %d consecutive TX send failures (%s), stopping",
                            errors,
                            transport,
                        )
                        return
                seq = (seq + 1) & 0xFFFF
                rtp_ts = (rtp_ts + len(payload)) & 0xFFFFFFFF
                next_at += _TALK_FRAME_SECONDS
                delay = next_at - loop.time()
                if delay < -_TALK_MAX_LAG:
                    next_at = loop.time()
                    delay = 0.0
                await asyncio.sleep(max(0.0, delay))
        except asyncio.CancelledError:
            pass
        finally:
            _LOGGER.debug(
                "Talk: TX audio stopped (%d packets, %d speech frames)",
                self._talk_sent,
                self._talk_payload_sent,
            )

    async def _send_talk_rtp(
        self, client: IconaBridgeClient, transport: str, rtp: bytes
    ) -> bool:
        """Send one TX RTP packet on the device's current RTPC channel."""
        device_rtpc = self._device_rtpc
        if device_rtpc is None or device_rtpc.server_channel_id == 0:
            return False
        if transport == "tcp":
            # ICONA-framed on the shared TCP connection, like RTPC media.
            await client.send_binary(device_rtpc, rtp)
            return True
        rx = self._rtp_receiver
        return rx is not None and rx.send_rtp_udp(device_rtpc.server_channel_id, rtp)

    async def start_inbound(
        self, entrance_addr: str, ring_ts: int, renewal_ack_ts: int = 0,
        preview_only: bool = False,
    ) -> RtpReceiver:  # noqa: C901
        """Execute the inbound call answer sequence (PCAP2-verified, steps 1-20).

        Called when the device initiates a ring (PREFIX_CALL_INIT). Reuses the
        existing CTPP channel opened by the coordinator VIP listener.
        Does NOT start audio — call answer_inbound() separately.

        preview_only: run the media setup through video-config (which requests
        the stream) but STOP before the PEER + call_accepted messages — i.e.
        never signal the entrance that the call was answered. This is the
        "true preview" experiment: if the device streams video after
        video-config but before accept, we can grab a snapshot without the
        panel showing "answered". If it doesn't, media is gated on accept and
        no frame arrives (logged). No two-way audio in this mode.
        """
        client = self._client
        self._inbound = True
        # The panel retransmits the ring (~7.5 s): once this call forwards
        # rings, those copies must not count as a new ring (they would cut a
        # message speak plays on it).
        self._last_ring_fired[entrance_addr] = time.monotonic()
        try:
            apt_addr = self._config.apt_address
            apt_sub = self._config.apt_subaddress
            our_addr = f"{apt_addr}{apt_sub}"
            # All inbound CTPP messages use our_base_addr as callee (PCAP2-verified).
            our_base_addr = apt_addr

            # Derive fresh_ts from ring_ts via device's proprietary transform (PCAP2-verified).
            _rb = bytearray(struct.pack("<I", ring_ts))
            _rb[0] |= 0x80
            _rb[2], _rb[3] = _rb[3], (_rb[2] + 1) & 0xFF
            fresh_ts = struct.unpack("<I", bytes(_rb))[0]

            ctpp = client.get_channel("CTPP")
            if ctpp is None:
                raise VideoCallError("No held CTPP channel for inbound answer")

            device_rtpc = client.register_placeholder_channel("RTPC_DEVICE")

            # Step 1: ACK ring with fresh_ts
            await client.send_binary(ctpp, encode_call_response_ack(our_addr, our_base_addr, fresh_ts))

            # Steps 2-4 burst: RTPC OPEN + UDPM OPEN + codec ACK within 8ms.
            # PCAP shows all three sent before device ACKs any channel; codec ACK
            # must arrive while channels are still in flight or device ignores it.
            codec_ack = encode_call_ack(our_addr, our_base_addr, fresh_ts, codec_param=0x07)
            rtpc1_task = asyncio.create_task(client.open_channel("RTPC", ChannelType.UAUT, trailing_byte=1))
            await asyncio.sleep(0)
            udpm_task = asyncio.create_task(client.open_channel("UDPM", ChannelType.UAUT, trailing_byte=1))
            await asyncio.sleep(0)
            await client.send_binary(ctpp, codec_ack)

            rtpc1 = await rtpc1_task
            udpm = await udpm_task
            udpm_token = 0
            if len(udpm.open_response_body) >= 18:
                udpm_token = struct.unpack_from("<H", udpm.open_response_body, 16)[0]

            # Step 5: RTP receiver (UDP control + keepalive; media_req_id set after RTPC2)
            control_req_id = udpm.server_channel_id
            receiver = RtpReceiver(
                client.host,
                client.port,
                control_req_id=control_req_id,
                media_req_id=0,
                udpm_token=udpm_token,
            )
            if self._rtsp_server and self._external_rtsp:
                self._rtsp_server.reset()
                rtsp_server = self._rtsp_server
            else:
                rtsp_server = LocalRtspServer()
                await rtsp_server.start()
                self._rtsp_server = rtsp_server
            receiver.attach_rtsp_queues(
                rtsp_server.nal_queue,
                rtsp_server.audio_queue,
                rtp_queue=rtsp_server.rtp_queue,
            )
            # Real mic → visitor talk-back: the audio sender pulls browser-mic
            # PCMA from the RTSP backchannel queue (go2rtc ANNOUNCE/RECORD) and
            # forwards it to the device, falling back to silence when quiet.
            receiver.attach_backchannel_queue(rtsp_server.backchannel_queue)
            await receiver.start_control()
            receiver.start_keepalive()
            self._rtp_receiver = receiver

            # Step 6: Wait for device response bundle (drain until non-ring response).
            # Device sends 0x18C0/0x0029 retransmits; ACK them with fresh_ts.
            try:
                async with asyncio.timeout(10.0):
                    while True:
                        data = await ctpp.response_queue.get()
                        if len(data) < 2:
                            continue
                        msg_type = struct.unpack_from("<H", data, 0)[0]
                        action = struct.unpack_from(">H", data, 6)[0] if len(data) >= 8 else 0
                        if msg_type == 0x18C0 and action == 0x0029:
                            await client.send_binary(
                                ctpp, encode_call_response_ack(our_addr, our_base_addr, fresh_ts)
                            )
                            continue
                        if msg_type == 0x1860 and action == 0x0010 and renewal_ack_ts:
                            await client.send_binary(
                                ctpp, encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts)
                            )
                            await client.send_binary(
                                ctpp,
                                encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts, prefix=0x1820),
                            )
                            continue
                        if msg_type != 0x18C0:
                            break
            except TimeoutError:
                _LOGGER.warning("start_inbound: no device bundle within 10s — proceeding")

            # Drain any remaining bundle messages before sending ACK2
            await asyncio.sleep(0.05)
            while not ctpp.response_queue.empty():
                try:
                    _fd = ctpp.response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if renewal_ack_ts and len(_fd) >= 8:
                    _ft = struct.unpack_from("<H", _fd, 0)[0]
                    _fa = struct.unpack_from(">H", _fd, 6)[0]
                    if _ft == 0x1860 and _fa == 0x0010:
                        await client.send_binary(
                            ctpp, encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts)
                        )
                        await client.send_binary(
                            ctpp,
                            encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts, prefix=0x1820),
                        )

            # Step 7: ACK2 (fresh_ts + B5) + RTPC2 open simultaneously (PCAP2-verified)
            call_counter = (fresh_ts + _CTR_INCR_BYTE5) & 0xFFFFFFFF
            await client.send_binary(ctpp, encode_call_response_ack(our_addr, our_base_addr, call_counter))
            rtpc2_task = asyncio.create_task(
                client.open_channel("RTPC2", ChannelType.UAUT, trailing_byte=1, wire_name="RTPC")
            )
            await asyncio.sleep(0)

            # Step 8: Codec ACK retransmit using same counter as ACK2 (PCAP2-verified)
            await client.send_binary(
                ctpp, encode_call_ack(our_addr, our_base_addr, call_counter, codec_param=0x07)
            )

            rtpc2 = await rtpc2_task
            media_req_id = rtpc2.server_channel_id
            receiver.set_media_req_id(media_req_id)
            await receiver.start_media()

            # Step 9: RTPC2-ready (+B4) — purpose unknown; required by device (PCAP2)
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(ctpp, encode_rtpc2_ready(our_addr, our_base_addr, call_counter))

            # Step 10: RTPC link (+B4)
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(
                ctpp, encode_rtpc_link(our_addr, our_base_addr, rtpc1.server_channel_id, call_counter)
            )

            # Steps 11-12: Video config (320x240 for inbound, PCAP2-verified) + 3s retransmit
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            vid_cfg = encode_video_config(our_addr, our_base_addr, media_req_id, call_counter, width=320, height=240)
            await client.send_binary(ctpp, vid_cfg)
            await asyncio.sleep(3.0)
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(
                ctpp,
                encode_video_config(our_addr, our_base_addr, media_req_id, call_counter, width=320, height=240),
            )
            await asyncio.sleep(0.4)

            if preview_only:
                # TRUE-PREVIEW experiment: video-config above already asked the
                # entrance for the stream. Do NOT send PEER/call_accepted, so
                # the panel never registers the ring as answered. Start the
                # media router and see whether the device streams video
                # pre-accept (grabbable as a snapshot) or gates it on accept.
                self._tcp_task = asyncio.create_task(
                    self._tcp_inbound_media_router(client, rtpc1, rtpc2, receiver)
                )
                self._call_counter = call_counter
                # What accept_preview needs to answer this call later (speak
                # in "answer" mode): steps 13-18 continue from here.
                self._inbound_ctx = {
                    "ctpp": ctpp,
                    "our_addr": our_addr,
                    "our_base_addr": our_base_addr,
                    "rtpc1": rtpc1,
                    "media_req_id": media_req_id,
                    "device_rtpc": device_rtpc,
                    "renewal_ack_ts": renewal_ack_ts,
                }
                self._active = True
                got_video = await receiver.wait_for_first_video(VIDEO_READY_TIMEOUT)
                _LOGGER.info(
                    "Inbound PREVIEW (no accept sent): device %s video pre-accept",
                    "STREAMED" if got_video else "did NOT stream",
                )
                if self._auto_timeout:
                    self._timeout_task = asyncio.create_task(self._auto_timeout_loop())
                return receiver

            # Step 13: PEER message — inbound=True uses 48B format with our_base_addr as callee
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(
                ctpp, encode_answer_peer(our_addr, our_base_addr, call_counter, inbound=True)
            )

            # Step 14: call_accepted sent TO device (reversed from outbound, PCAP2-verified)
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(ctpp, encode_call_accepted(our_addr, our_base_addr, call_counter))

            # Step 15: Drain CTPP — ACK 0x1840/0x000A (rtpc_link) + 0x1840/0x000E (peer)
            # using transform(device_ts). Device opens its RTPC only AFTER receiving these ACKs.
            acked_rtpc_link = False
            acked_peer = False
            drain_deadline = asyncio.get_running_loop().time() + 10.0
            while not (acked_rtpc_link and acked_peer):
                remaining = drain_deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    data = await asyncio.wait_for(ctpp.response_queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if len(data) < 2:
                    continue
                msg_type = struct.unpack_from("<H", data, 0)[0]
                action = struct.unpack_from(">H", data, 6)[0] if len(data) >= 8 else 0
                dev_ts = struct.unpack_from("<I", data, 2)[0] if len(data) >= 6 else 0
                if msg_type == 0x1840:
                    _rb = bytearray(struct.pack("<I", dev_ts))
                    _rb[0] |= 0x80
                    _rb[2], _rb[3] = _rb[3], (_rb[2] + 1) & 0xFF
                    ack_ts = struct.unpack("<I", bytes(_rb))[0]
                    await client.send_binary(ctpp, encode_call_response_ack(our_addr, our_base_addr, ack_ts))
                    if action == 0x000A:
                        acked_rtpc_link = True
                    elif action == 0x000E:
                        acked_peer = True
                elif msg_type == 0x1860 and action == 0x0010 and renewal_ack_ts:
                    await client.send_binary(
                        ctpp, encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts)
                    )
                    await client.send_binary(
                        ctpp,
                        encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts, prefix=0x1820),
                    )

            # Step 16: Wait for device to open its RTPC channel
            try:
                await asyncio.wait_for(device_rtpc.open_event.wait(), timeout=VIDEO_RESPONSE_TIMEOUT)
                self._device_rtpc_req_id = device_rtpc.server_channel_id
                self._device_rtpc = device_rtpc
                self._inbound_accepted = True
                if is_verbose_logging():
                    _LOGGER.debug("Inbound: device RTPC=0x%04X", self._device_rtpc_req_id)
                # Step 17: Start audio sender immediately — device requires this for video to flow
                if self._rtp_receiver:
                    self._rtp_receiver.start_audio_sender(self._device_rtpc_req_id)
            except TimeoutError:
                _LOGGER.warning("start_inbound: device RTPC not opened within timeout")

            # Step 18: Start TCP media router — device sends video (RTPC2) + audio (RTPC1) via TCP
            self._tcp_task = asyncio.create_task(
                self._tcp_inbound_media_router(client, rtpc1, rtpc2, receiver)
            )
            self._call_counter = call_counter
            self._ctpp_task = asyncio.create_task(
                self._ctpp_monitor_loop(
                    client,
                    ctpp,
                    our_addr,
                    our_base_addr,
                    call_counter,
                    rtpc1.server_channel_id,
                    media_req_id,
                )
            )
            self._active = True

            # Step 19: Wait for first video frame (adapted to our bool-returning API)
            got_video = await receiver.wait_for_first_video(VIDEO_READY_TIMEOUT)
            if got_video:
                if is_verbose_logging():
                    _LOGGER.info(
                        "Inbound video call ready: our_addr=%s entrance=%s", our_addr, entrance_addr
                    )
            else:
                _LOGGER.warning(
                    "start_inbound: no video within %.1fs — signaling succeeded but device not sending RTP",
                    VIDEO_READY_TIMEOUT,
                )

            # Step 20: Auto-timeout
            if self._auto_timeout:
                self._timeout_task = asyncio.create_task(self._auto_timeout_loop())

            return receiver

        except Exception as e:
            await self._cleanup()
            raise VideoCallError(f"Failed to start inbound call: {e}") from e

    def answer_inbound(self) -> None:
        """Start two-way audio for an active inbound call (step 21).

        Sends PCMA silence frames to the device so the visitor hears audio.
        Must be called after start_inbound() succeeds and device RTPC opened.
        """
        if not self._rtp_receiver or self._device_rtpc_req_id == 0:
            _LOGGER.warning("answer_inbound: no receiver or device RTPC req_id — cannot start audio")
            return
        self._rtp_receiver.start_audio_sender(self._device_rtpc_req_id)
        if is_verbose_logging():
            _LOGGER.info(
                "Inbound call answered — audio sender started (req_id=0x%04X)", self._device_rtpc_req_id
            )

    async def start_talk_inbound(self, transport: str = "auto") -> bool:
        """Answer the incoming call this session holds and start the talk sender.

        Speak in "answer" mode. A preview (the on-ring snapshot call, no accept
        sent) is accepted now with steps 13-18 of start_inbound; an already
        accepted call (auto-answer) only swaps its silence/mic sender for ours.
        Sends no door command. Returns False when the call can't be answered.
        """
        if not self._inbound or not self._active:
            return False
        client = self._client
        self._talk = True
        # On an answered inbound call the only TX verified so far is UDP (the
        # inbound audio sender, 6701W), so "auto" means UDP here.
        self._talk_transport_pref = "udp" if transport == "auto" else transport
        if not await self.accept_inbound():
            return False
        if self._rtp_receiver is not None:
            self._rtp_receiver.stop_audio_sender()  # ours replaces it
        self._answer_sent.set()
        self._start_talk_sender(client)
        return self.talk_running

    async def accept_inbound(self) -> bool:
        """Answer the incoming call this session holds (idempotent).

        A preview (the on-ring snapshot call) is accepted with steps 13-18 of
        start_inbound; an already accepted call (auto-answer, or a previous
        accept) needs nothing. Speak and an in-call door open may both ask:
        the lock makes the second one wait and find it accepted. Afterwards
        the device's CALL_END ends the call (no outbound re-establishment).
        """
        async with self._accept_lock:
            if not self._inbound or not self._active:
                return False
            if not self._inbound_accepted and not await self._accept_preview(self._client):
                return False
            self._end_on_call_end = True
            return True

    async def _accept_preview(self, client: IconaBridgeClient) -> bool:
        """Accept an inbound call held in preview: start_inbound steps 13-18.

        Same frames and order as the full answer (PCAP2): PEER, call_accepted,
        ACK the device's RTPC link / peer, wait for its RTPC, then the CTPP
        monitor. The media router already runs since the preview.
        """
        ctx = self._inbound_ctx
        if ctx is None:
            _LOGGER.warning("Talk: no preview context, cannot answer the call")
            return False
        ctpp = ctx["ctpp"]
        our_addr = ctx["our_addr"]
        our_base_addr = ctx["our_base_addr"]
        renewal_ack_ts = ctx["renewal_ack_ts"]

        # Frames queued while the preview held CTPP (nobody read them): if the
        # call already ended (door opened, hung up) don't answer it. They go
        # back in the queue, so the listener still sees them afterwards.
        backlog: list[bytes] = []
        while not ctpp.response_queue.empty():
            backlog.append(ctpp.response_queue.get_nowait())
        for data in backlog:
            ctpp.response_queue.put_nowait(data)
        end_flags = next(
            (
                struct.unpack_from(">H", data, 8)[0]
                for data in backlog
                if len(data) >= 10
                and struct.unpack_from("<H", data, 0)[0] in (0x1840, 0x1860)
                and struct.unpack_from(">H", data, 6)[0] == 0x0003
            ),
            None,
        )
        if end_flags is not None:
            # A door frame alone is no proof: on 26/10 the panel sent one after
            # each (failed) TAP open while the ring went on. The call is over
            # only if the panel also stopped streaming to the preview (on 23/10
            # the video stopped 150 ms before the call-end frame).
            rx = self._rtp_receiver
            seen = (rx.tcp_media_packet_count + rx.udp_media_packet_count) if rx else 0
            await asyncio.sleep(_ENDED_VIDEO_CHECK)
            now = (rx.tcp_media_packet_count + rx.udp_media_packet_count) if rx else 0
            if now == seen:
                self.call_already_ended = end_flags
                _LOGGER.info("Talk: the incoming call already ended, not answering it")
                return False
            _LOGGER.info(
                "Talk: call-end frame 0x%04X queued but the video still flows: answering",
                end_flags,
            )

        # Steps 13-14: PEER (inbound format) + call_accepted, each +B4.
        async with self._ctpp_lock:
            call_counter = (self._call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(
                ctpp, encode_answer_peer(our_addr, our_base_addr, call_counter, inbound=True)
            )
            call_counter = (call_counter + _CTR_INCR_BYTE4) & 0xFFFFFFFF
            await client.send_binary(ctpp, encode_call_accepted(our_addr, our_base_addr, call_counter))
            self._call_counter = call_counter

        # Step 15: ACK 0x1840/0x000A (rtpc link) + 0x1840/0x000E (peer) with
        # transform(device ts); the device opens its RTPC only after these.
        acked_rtpc_link = acked_peer = False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10.0
        while not (acked_rtpc_link and acked_peer):
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                data = await asyncio.wait_for(ctpp.response_queue.get(), timeout=remaining)
            except TimeoutError:
                break
            if len(data) < 2:
                continue
            msg_type = struct.unpack_from("<H", data, 0)[0]
            action = struct.unpack_from(">H", data, 6)[0] if len(data) >= 8 else 0
            dev_ts = struct.unpack_from("<I", data, 2)[0] if len(data) >= 6 else 0
            if msg_type == 0x1840:
                _rb = bytearray(struct.pack("<I", dev_ts))
                _rb[0] |= 0x80
                _rb[2], _rb[3] = _rb[3], (_rb[2] + 1) & 0xFF
                ack_ts = struct.unpack("<I", bytes(_rb))[0]
                await client.send_binary(ctpp, encode_call_response_ack(our_addr, our_base_addr, ack_ts))
                if action == 0x000A:
                    acked_rtpc_link = True
                elif action == 0x000E:
                    acked_peer = True
            elif msg_type == 0x1860 and action == 0x0010 and renewal_ack_ts:
                await client.send_binary(
                    ctpp, encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts)
                )
                await client.send_binary(
                    ctpp,
                    encode_call_response_ack(our_addr, our_base_addr, renewal_ack_ts, prefix=0x1820),
                )

        # Step 16: the device's RTPC: our TX audio goes there.
        device_rtpc = ctx["device_rtpc"]
        try:
            await asyncio.wait_for(device_rtpc.open_event.wait(), timeout=VIDEO_RESPONSE_TIMEOUT)
        except TimeoutError:
            _LOGGER.warning("Talk: device RTPC not opened after the answer")
            return False
        self._device_rtpc = device_rtpc
        self._device_rtpc_req_id = device_rtpc.server_channel_id
        self._inbound_accepted = True

        # Step 18 (CTPP part): ACK keepalives, forward new rings.
        self._ctpp_task = asyncio.create_task(
            self._ctpp_monitor_loop(
                client,
                ctpp,
                our_addr,
                our_base_addr,
                self._call_counter,
                ctx["rtpc1"].server_channel_id,
                ctx["media_req_id"],
            )
        )
        _LOGGER.debug("Talk: incoming call answered (device RTPC 0x%04X)", self._device_rtpc_req_id)
        return True

    @staticmethod
    async def _tcp_inbound_media_router(
        client: IconaBridgeClient,
        rtpc1: Channel,
        rtpc2: Channel,
        receiver: RtpReceiver,
    ) -> None:
        """Route TCP RTP from RTPC1 (audio) and RTPC2 (video) into receiver.

        On inbound calls the device streams both tracks over TCP (not UDP).
        The client strips the ICONA header, so queued data is raw RTP.
        """
        try:
            while receiver.running:
                for ch in (rtpc1, rtpc2):
                    try:
                        data = ch.response_queue.get_nowait()
                        if len(data) >= 12:
                            receiver.receive_tcp_rtp(data)
                    except asyncio.QueueEmpty:
                        pass
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.debug("TCP inbound media router error", exc_info=True)

    async def async_open_door_on_ctpp(
        self,
        our_addr: str,
        entrance_addr: str,
        relay_index: int,
        apt_addr: str | None = None,
    ) -> None:
        """Open a door by sending 0x1840/0x000D on the active video CTPP channel.

        PCAP-verified: the Android app sends this single message on the existing
        video CTPP channel — no separate channel open, no 6-step sequence.
        The device ACKs with 0x1800/0x0000 and the relay activates.
        """
        ctpp = self._client.get_channel("CTPP")
        if ctpp is None or not self._active:
            raise RuntimeError("No active video CTPP channel")
        async with self._ctpp_lock:
            self._call_counter += _CTR_INCR_BYTE4
            payload = encode_door_open_during_video(
                our_addr,
                entrance_addr,
                self._call_counter,
                relay_index,
                apt_addr=apt_addr,
            )
            await self._client.send_binary(ctpp, payload)
        if is_verbose_logging():
            _LOGGER.info(
                "Door open sent on video CTPP (relay=%d, counter=0x%08X)",
                relay_index,
                self._call_counter,
            )

    async def open_door_in_call(
        self,
        our_addr: str,
        door_addr: str,
        relay_index: int,
        apt_addr: str,
        ack_timeout: float = _DOOR_ACK_TIMEOUT,
    ) -> tuple[bool, int | None]:
        """Open a door inside this call and wait for the panel's ACK.

        Sends 0x1840/0x000D (async_open_door_on_ctpp) and waits for the 0x1800
        ACK sent by the door's unit (the panel ACKs from its own address,
        e.g. SB100001). Returns (acked, flags of a 0x1860/0x0003 door frame
        seen meanwhile, or None). Neither proves that the relay tripped: on
        26/10 the panel answered a TAP open during a ring with 0x1860/0x0003
        and the lock stayed shut.
        """
        async with self._door_lock:
            loop = asyncio.get_running_loop()
            self._door_ack = loop.create_future()
            self._door_addr = door_addr
            self._door_event_flags = None
            self.door_confirmed = False
            if self._talk:
                # A speak call ends with the door: the panel's CALL_END after
                # the relay closes it instead of a renewal.
                self._end_on_call_end = True
            try:
                await self.async_open_door_on_ctpp(
                    our_addr, door_addr, relay_index, apt_addr=apt_addr
                )
                self._door_sent_at = time.monotonic()
                try:
                    await asyncio.wait_for(asyncio.shield(self._door_ack), ack_timeout)
                    acked = True
                except TimeoutError:
                    acked = False
                return acked, self._door_event_flags
            finally:
                self._door_ack = None

    def _note_door_ack(self, resp: bytes) -> None:
        """Settle a pending in-call door open on an ACK from the door's unit."""
        waiter = self._door_ack
        if waiter is None or waiter.done():
            return
        parsed = parse_ctpp_message(resp) or {}
        addresses = parsed.get("addresses") or [""]
        sender = (addresses[0] or "").upper()
        door = self._door_addr.upper()
        # Kit-mode frames may drop the "S" prefix (B100001 vs SB100001).
        if sender and door and (sender.endswith(door) or door.endswith(sender)):
            waiter.set_result(True)

    async def _auto_timeout_loop(self) -> None:
        """Automatically stop the session after VIDEO_SESSION_TIMEOUT."""
        try:
            await asyncio.sleep(VIDEO_SESSION_TIMEOUT)
            if is_verbose_logging():
                _LOGGER.info("Video session timed out after %ds", VIDEO_SESSION_TIMEOUT)
            await self._cleanup()
            if self._on_timeout:
                self._on_timeout()
        except asyncio.CancelledError:
            pass
