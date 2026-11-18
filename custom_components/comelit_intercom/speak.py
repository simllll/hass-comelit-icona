"""The speak service: say a message (TTS) or play audio on an entrance panel.

The audio comes from Home Assistant's text-to-speech (the official
``tts.generate_media_source_id`` + ``tts.async_get_media_source_audio``) or
from a ready file (media id, allowed local path or allow-listed URL). It is
decoded with PyAV, turned into 20 ms G.711 A-law frames (:mod:`.audio`) and
played by the stream manager, either over a short outbound "talk" call
(mode "call") or on the incoming call HA holds after a ring (mode "answer").
In mode "answer" the call is answered first and the audio is built meanwhile
(silence until it is ready): the monitor takes the call about 1 s after a
face is recognised. Built frames are kept in a small in-memory cache, and
``prepare_only`` builds them without any call (to warm the cache up).
With ``open_door`` the door is opened inside the same call after the audio.

Registered as an entity service on the entrance cameras, so automations target
the entrance by its camera entity. Wrong input raises; runtime conditions
(panel busy, call refused, TTS failure) are logged and returned in the
response with ``spoken: false``, so an automation is never broken by them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .audio import FRAME_SECONDS, build_frames
from .events import address_matches
from .video_stream import speak_result

if TYPE_CHECKING:
    from .coordinator import ComelitDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

SERVICE_SPEAK = "speak"

ATTR_MESSAGE = "message"
ATTR_MEDIA_CONTENT_ID = "media_content_id"
ATTR_TTS_ENTITY = "tts_entity"
ATTR_LANGUAGE = "language"
ATTR_VOICE = "voice"
ATTR_OPTIONS = "options"
ATTR_CACHE = "cache"
ATTR_GAIN_DB = "gain_db"
ATTR_MAX_WAIT = "max_wait"
ATTR_AUDIO_WAIT = "audio_wait"
ATTR_LEAD_IN = "lead_in"
ATTR_TRANSPORT = "transport"
ATTR_MODE = "mode"
ATTR_ANSWER_FALLBACK = "answer_fallback"
ATTR_OPEN_DOOR = "open_door"
ATTR_DOOR = "door"
ATTR_PREPARE_ONLY = "prepare_only"

MODE_CALL = "call"
MODE_ANSWER = "answer"

DEFAULT_LANGUAGE = "it"
# Longest audio played; the rest is cut (keeps the call short).
MAX_AUDIO_SECONDS = 30.0
_MAX_MEDIA_BYTES = 10 * 1024 * 1024
_FETCH_TIMEOUT = 20.0
_TTS_TIMEOUT = 30.0
# Built frames of recent TTS messages (a greeting is ~40 kB): skips TTS and
# decoding, the ~2 s that used to come before the answer. Room for the
# greetings plus the other messages (courier, announcements).
_FRAME_CACHE_SIZE = 32
_FRAME_CACHE: OrderedDict[tuple, list[bytes]] = OrderedDict()

# Female voice used when the call names none, per TTS platform (voice ids as
# these engines list them). Gemini "Kore" (firm, clear) and OpenAI "Nova"
# (bright) stay intelligible through the panel's 8 kHz speaker.
FEMALE_VOICES = {
    "google_generative_ai_conversation": "kore",
    "openai_conversation": "nova",
}

_LOCAL_MEDIA_PREFIX = "media-source://media_source/"
_TTS_MEDIA_PREFIX = "media-source://tts/"
# File extension -> FFmpeg demuxer, only used when probing the data fails.
_FORMAT_HINTS = {
    "mp3": "mp3",
    "wav": "wav",
    "ogg": "ogg",
    "oga": "ogg",
    "opus": "ogg",
    "flac": "flac",
    "aac": "aac",
    "m4a": "mov",
    "mp4": "mov",
    "webm": "webm",
}

SPEAK_SCHEMA: dict[Any, Any] = {
    vol.Exclusive(ATTR_MESSAGE, "audio_source"): vol.All(
        cv.string, vol.Length(min=1, max=500)
    ),
    vol.Exclusive(ATTR_MEDIA_CONTENT_ID, "audio_source"): cv.string,
    vol.Optional(ATTR_TTS_ENTITY): cv.string,
    vol.Optional(ATTR_LANGUAGE, default=DEFAULT_LANGUAGE): cv.string,
    vol.Optional(ATTR_VOICE): cv.string,
    vol.Optional(ATTR_OPTIONS, default=dict): dict,
    vol.Optional(ATTR_CACHE, default=True): cv.boolean,
    vol.Optional(ATTR_GAIN_DB, default=0.0): vol.All(
        vol.Coerce(float), vol.Range(min=-20, max=12)
    ),
    vol.Optional(ATTR_MAX_WAIT, default=60.0): vol.All(
        vol.Coerce(float), vol.Range(min=0, max=300)
    ),
    vol.Optional(ATTR_AUDIO_WAIT, default=5.0): vol.All(
        vol.Coerce(float), vol.Range(min=0, max=60)
    ),
    vol.Optional(ATTR_LEAD_IN, default=0.4): vol.All(
        vol.Coerce(float), vol.Range(min=0, max=5)
    ),
    vol.Optional(ATTR_TRANSPORT, default="auto"): vol.In(["auto", "tcp", "udp"]),
    vol.Optional(ATTR_MODE, default=MODE_CALL): vol.In([MODE_CALL, MODE_ANSWER]),
    vol.Optional(ATTR_ANSWER_FALLBACK, default=True): cv.boolean,
    vol.Optional(ATTR_OPEN_DOOR, default=False): cv.boolean,
    vol.Optional(ATTR_DOOR): cv.string,
    vol.Optional(ATTR_PREPARE_ONLY, default=False): cv.boolean,
}


@dataclass
class _Audio:
    """Frames ready to send, and how they were obtained."""

    frames: list[bytes]
    cached: bool = False
    seconds: float = 0.0  # time spent building them
    meta: dict[str, Any] = field(default_factory=dict)


async def async_speak(
    hass: HomeAssistant,
    coordinator: ComelitDataUpdateCoordinator,
    entrance: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Build the audio, play it on *entrance*, maybe open; returns the outcome."""
    message = data.get(ATTR_MESSAGE)
    media_id = data.get(ATTR_MEDIA_CONTENT_ID)
    mode = data.get(ATTR_MODE, MODE_CALL)
    open_door = data.get(ATTR_OPEN_DOOR, False)

    if data.get(ATTR_PREPARE_ONLY, False):
        if not message and not media_id:
            raise ServiceValidationError(
                "prepare_only needs a message or a media_content_id"
            )
        return await _async_prepare_only(hass, message, media_id, data, mode)
    if not message and not media_id and not open_door:
        raise ServiceValidationError(
            "Give a message, a media_content_id, or open_door: true"
        )
    door = _pick_door(coordinator, entrance, data.get(ATTR_DOOR)) if open_door else None

    audio_task: asyncio.Task | None = None
    frames_task: asyncio.Task | None = None
    if message or media_id:
        # Validate the request now (wrong engine, language or options, a media
        # id that is not allowed: they raise here, before any call is
        # touched); build the frames in a task.
        plan = _tts_plan(hass, message, data) if message else None
        if media_id:
            await _async_check_media(hass, media_id)
        audio_task = hass.async_create_task(
            _async_build_audio(hass, data, plan, media_id), eager_start=True
        )
        if mode == MODE_ANSWER:
            # Answer mode answers first: the stream manager awaits this task
            # only once the call is answered (silence meanwhile).
            frames_task = hass.async_create_task(_frames_of(audio_task))
    try:
        frames: list[bytes] | None = None
        if audio_task is not None and mode != MODE_ANSWER:
            # Call mode needs the audio before calling: wait for it here, so
            # wrong input still raises and a TTS failure returns a reason.
            try:
                frames = (await audio_task).frames or None
            except ServiceValidationError:
                raise
            except Exception as err:  # noqa: BLE001 - reported, never raised
                if door is None:
                    return _audio_failed(err, mode)
                _LOGGER.warning("Speak: audio not available (%s); opening only", err)
            if frames is None and door is None:
                return speak_result("the audio is empty or silent", mode=mode)
        result = await coordinator.stream.async_speak(
            entrance,
            frames,
            audio=frames_task,
            door=door,
            mode=mode,
            answer_fallback=data.get(ATTR_ANSWER_FALLBACK, True),
            max_wait=data.get(ATTR_MAX_WAIT, 60.0),
            audio_wait=data.get(ATTR_AUDIO_WAIT, 5.0),
            lead_in=data.get(ATTR_LEAD_IN, 0.4),
            transport=data.get(ATTR_TRANSPORT, "auto"),
        )
    finally:
        for task in (frames_task, audio_task):
            if task is not None and not task.done():
                task.cancel()
    if frames_task is not None and frames_task.done() and not frames_task.cancelled():
        frames_task.exception()  # retrieved (it is reported in the result)
    if audio_task is not None and audio_task.done() and not audio_task.cancelled():
        if audio_task.exception() is None:
            built = audio_task.result()
            result["cached"] = built.cached
            result["prepare_time"] = round(built.seconds, 2)
    return result


async def _frames_of(task: asyncio.Task) -> list[bytes]:
    """The frames of an audio task (for the stream manager)."""
    return (await asyncio.shield(task)).frames


def _audio_failed(err: BaseException, mode: str) -> dict[str, Any]:
    reason = f"audio not available: {str(err) or type(err).__name__}"
    _LOGGER.warning("Speak: %s", reason)
    return speak_result(reason, mode=mode)


async def _async_prepare_only(
    hass: HomeAssistant,
    message: str | None,
    media_id: str | None,
    data: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    """Build (and cache) the frames without touching any call."""
    plan = _tts_plan(hass, message, data) if message else None
    try:
        built = await _async_build_audio(hass, data, plan, media_id)
    except ServiceValidationError:
        raise
    except Exception as err:  # noqa: BLE001
        return _audio_failed(err, mode)
    result = speak_result("prepared", mode=mode)
    result.update(
        prepared=bool(built.frames),
        cached=built.cached,
        prepare_time=round(built.seconds, 2),
        duration=round(len(built.frames) * FRAME_SECONDS, 2),
    )
    if not built.frames:
        result["reason"] = "the audio is empty or silent"
    return result


def _pick_door(
    coordinator: ComelitDataUpdateCoordinator, entrance: str, name: str | None
) -> dict[str, Any]:
    """The door to open: by name, else the one on this entrance."""
    doors = list((coordinator.data or {}).get("doors", []))
    if name:
        door = next(
            (d for d in doors if str(d.get("name", "")).lower() == name.lower()), None
        )
        if door is None:
            raise ServiceValidationError(f"Door not found: {name}")
        return door
    here = [d for d in doors if address_matches(d.get("apt-address"), entrance)]
    if len(here) == 1:
        return here[0]
    raise ServiceValidationError(
        "open_door: give the door's name (none or several doors on this entrance)"
    )


# --- text-to-speech ---------------------------------------------------------


@dataclass
class _TtsPlan:
    """A validated TTS request: engine, media id and the frame cache key."""

    engine: str
    media_id: str
    key: tuple


def _tts_plan(hass: HomeAssistant, message: str, data: dict[str, Any]) -> _TtsPlan:
    """Validate a TTS request and build its media id (raises on bad input)."""
    if "tts" not in hass.config.components:
        raise ServiceValidationError("Text-to-speech is not set up")
    from homeassistant.components import tts
    from homeassistant.components.tts.helper import get_engine_instance

    engine = tts.async_resolve_engine(hass, data.get(ATTR_TTS_ENTITY))
    if engine is None:
        raise ServiceValidationError(
            f"TTS engine not found: {data.get(ATTR_TTS_ENTITY) or '(default)'}"
        )
    instance = get_engine_instance(hass, engine)
    language = _match_language(engine, instance, data.get(ATTR_LANGUAGE))

    options = dict(data.get(ATTR_OPTIONS) or {})
    if voice := data.get(ATTR_VOICE):
        options["voice"] = voice
    elif "voice" not in options and (
        female := _default_female_voice(hass, engine, instance)
    ):
        options["voice"] = female
    # Without a preferred format HA re-encodes every result to mp3; ask for
    # WAV instead (Gemini already returns it, OpenAI produces it natively),
    # so the only lossy step left is the final A-law encoding.
    options.setdefault("preferred_format", "wav")

    try:
        media_id = tts.generate_media_source_id(
            hass,
            message,
            engine=engine,
            language=language,
            options=options,
            cache=data.get(ATTR_CACHE, True),
        )
    except HomeAssistantError as err:
        raise ServiceValidationError(
            f"TTS request rejected by {engine}: {err}"
        ) from err
    key = (
        " ".join(message.split()),  # the same text, however a template spaced it
        engine,
        language,
        json.dumps(options, sort_keys=True, default=str),
        float(data.get(ATTR_GAIN_DB, 0.0)),
    )
    return _TtsPlan(engine, media_id, key)


async def _async_build_audio(
    hass: HomeAssistant,
    data: dict[str, Any],
    plan: _TtsPlan | None,
    media_id: str | None,
) -> _Audio:
    """Frames for a TTS plan (cached) or a ready media id (not cached).

    With ``cache: false`` the frame cache is neither read nor written, like
    Home Assistant's own TTS cache.
    """
    t0 = time.monotonic()
    use_cache = plan is not None and data.get(ATTR_CACHE, True)
    if use_cache and (hit := _FRAME_CACHE.get(plan.key)) is not None:
        _FRAME_CACHE.move_to_end(plan.key)
        _LOGGER.debug("Speak: frames from the cache (%d)", len(hit))
        return _Audio(hit, cached=True, seconds=time.monotonic() - t0)
    if plan is not None:
        from homeassistant.components import tts

        _LOGGER.debug("Speak: TTS with %s", plan.engine)
        ext, raw = await asyncio.wait_for(
            tts.async_get_media_source_audio(hass, plan.media_id), timeout=_TTS_TIMEOUT
        )
    else:
        assert media_id is not None
        raw, ext = await _async_load_media(hass, media_id)
    frames, truncated = await hass.async_add_executor_job(
        partial(
            build_frames,
            raw,
            format_hint=_FORMAT_HINTS.get((ext or "").lower().lstrip(".")),
            max_seconds=MAX_AUDIO_SECONDS,
            gain_db=data.get(ATTR_GAIN_DB, 0.0),
        )
    )
    if truncated:
        _LOGGER.warning("Speak: audio longer than %.0f s, cut", MAX_AUDIO_SECONDS)
    if use_cache and frames:
        _FRAME_CACHE[plan.key] = frames
        while len(_FRAME_CACHE) > _FRAME_CACHE_SIZE:
            _FRAME_CACHE.popitem(last=False)
    return _Audio(frames, cached=False, seconds=time.monotonic() - t0)


def _match_language(engine: str, instance: Any, language: str | None) -> str | None:
    """Map a short tag ("it") to what the engine lists ("it-IT").

    Engines like Gemini and OpenAI only accept full tags. None means "the
    engine's default" (both detect the language from the text anyway).
    """
    if not language:
        return None
    supported = list(getattr(instance, "supported_languages", None) or [])
    if not supported or language in supported:
        return language

    def norm(tag: str) -> str:
        return tag.replace("_", "-").lower()

    wanted = norm(language)
    base = wanted.split("-")[0]
    same = [s for s in supported if norm(s) == wanted]
    same = same or [s for s in supported if norm(s) == f"{base}-{base}"]
    same = same or [s for s in supported if norm(s).split("-")[0] == base]
    if same:
        return same[0]
    _LOGGER.warning(
        "Speak: %s does not list language %s; using its default", engine, language
    )
    return None


def _default_female_voice(
    hass: HomeAssistant, engine: str, instance: Any
) -> str | None:
    """Female voice for known engines that take a "voice" option."""
    if "voice" not in (getattr(instance, "supported_options", None) or []):
        return None
    entry = er.async_get(hass).async_get(engine)
    return FEMALE_VOICES.get(entry.platform) if entry is not None else None


# --- ready-made media -------------------------------------------------------


async def _async_check_media(hass: HomeAssistant, media_id: str) -> None:
    """The quick checks of _async_load_media, before any call is answered."""
    if media_id.startswith(_TTS_MEDIA_PREFIX):
        return
    if media_id.startswith(_LOCAL_MEDIA_PREFIX):
        source_dir, _, location = media_id[len(_LOCAL_MEDIA_PREFIX) :].partition("/")
        base = hass.config.media_dirs.get(source_dir)
        if base is None or not location:
            raise ServiceValidationError(f"Unknown local media: {media_id}")
        root = Path(base)
        await hass.async_add_executor_job(
            _check_media_path, hass, root / location, root
        )
    elif media_id.startswith("/"):
        await hass.async_add_executor_job(_check_media_path, hass, Path(media_id), None)
    elif media_id.startswith(("http://", "https://")):
        if not hass.config.is_allowed_external_url(media_id):
            raise ServiceValidationError(
                f"URL not allowed: {media_id} (add it to allowlist_external_urls)"
            )
    elif not media_id.startswith("media-source://"):
        raise ServiceValidationError(f"Unsupported media: {media_id}")


def _check_media_path(hass: HomeAssistant, path: Path, root: Path | None) -> None:
    """Path checks of _read_media_file without reading (executor)."""
    real = path.resolve()
    if root is not None:
        if not real.is_relative_to(root.resolve()):
            raise ServiceValidationError(f"Path outside the media folder: {path}")
    elif not hass.config.is_allowed_path(str(real)):
        raise ServiceValidationError(
            f"Path not allowed: {path} (see allowlist_external_dirs)"
        )
    if not real.is_file():
        raise ServiceValidationError(f"Media file not found: {path}")


async def _async_load_media(
    hass: HomeAssistant, media_id: str
) -> tuple[bytes, str | None]:
    """Fetch a TTS media id, a local media/file or an http(s) URL."""
    if media_id.startswith(_TTS_MEDIA_PREFIX):
        from homeassistant.components import tts

        ext, audio = await asyncio.wait_for(
            tts.async_get_media_source_audio(hass, media_id), timeout=_TTS_TIMEOUT
        )
        return audio, ext

    url = media_id
    path: Path | None = None
    root: Path | None = None
    if media_id.startswith(_LOCAL_MEDIA_PREFIX):
        source_dir, _, location = media_id[len(_LOCAL_MEDIA_PREFIX) :].partition("/")
        base = hass.config.media_dirs.get(source_dir)
        if base is None or not location:
            raise ServiceValidationError(f"Unknown local media: {media_id}")
        root = Path(base)
        path = root / location
    elif media_id.startswith("/"):
        path = Path(media_id)
    elif media_id.startswith("media-source://"):
        from homeassistant.components import media_source

        play = await media_source.async_resolve_media(hass, media_id, None)
        path = getattr(play, "path", None)  # set for local files on recent HA
        url = play.url

    if path is not None:
        audio = await hass.async_add_executor_job(_read_media_file, hass, path, root)
        return audio, path.suffix
    if url.startswith(("http://", "https://")):
        # Only URLs the user allow-listed: no requests from HA to arbitrary
        # (e.g. internal) addresses on behalf of any service caller.
        if not hass.config.is_allowed_external_url(url):
            raise ServiceValidationError(
                f"URL not allowed: {url} (add it to allowlist_external_urls)"
            )
        return await _async_fetch(hass, url), Path(url.split("?", 1)[0]).suffix
    raise ServiceValidationError(f"Unsupported media: {media_id}")


def _read_media_file(hass: HomeAssistant, path: Path, root: Path | None) -> bytes:
    """Read a local media file (executor: path checks do blocking I/O)."""
    try:
        real = path.resolve()
        if root is not None:
            if not real.is_relative_to(root.resolve()):
                raise ServiceValidationError(f"Path outside the media folder: {path}")
        elif not hass.config.is_allowed_path(str(real)):
            raise ServiceValidationError(
                f"Path not allowed: {path} (see allowlist_external_dirs)"
            )
        if real.stat().st_size > _MAX_MEDIA_BYTES:
            raise ServiceValidationError(f"Media file too large: {path}")
        return real.read_bytes()
    except OSError as err:
        raise ServiceValidationError(f"Cannot read {path}: {err}") from err


async def _async_fetch(hass: HomeAssistant, url: str) -> bytes:
    """Download an audio file: size-capped, redirects not followed."""
    session = async_get_clientsession(hass)
    data = bytearray()
    async with (
        asyncio.timeout(_FETCH_TIMEOUT),
        session.get(url, allow_redirects=False) as resp,
    ):
        if 300 <= resp.status < 400:
            raise ServiceValidationError(f"Redirect not followed: {url}")
        resp.raise_for_status()
        if (resp.content_length or 0) > _MAX_MEDIA_BYTES:
            raise ServiceValidationError(f"Media too large: {url}")
        # content.read(n) returns only what is buffered: read to the end.
        async for chunk in resp.content.iter_chunked(64 * 1024):
            data += chunk
            if len(data) > _MAX_MEDIA_BYTES:
                raise ServiceValidationError(f"Media too large: {url}")
    return bytes(data)
