"""Audio helpers for talking to an entrance panel (any audio -> G.711 A-law).

The panel plays G.711 A-law (PCMA, RTP payload type 8): 8 kHz, mono, one byte
per sample, sent as 20 ms frames of 160 bytes. ``audioop`` is gone from Python
3.13+, so the A-law encoder lives here: a 65536-entry table built once from the
ITU-T G.711 segment rules (same algorithm as the reference Sun ``g711.c``),
indexed by the 16-bit sample. Decoding and resampling use PyAV, which the
integration already requires for video.

No Home Assistant imports, so this module can be tested on its own. The heavy
functions (PyAV decode, encoding) are blocking: run them in an executor.
"""

from __future__ import annotations

import io
from array import array
from functools import cache

SAMPLE_RATE = 8000
FRAME_SAMPLES = 160  # 20 ms at 8 kHz
FRAME_BYTES = 160  # A-law: one byte per sample
FRAME_SECONDS = FRAME_SAMPLES / SAMPLE_RATE
ALAW_SILENCE = 0xD5  # A-law code of linear 0

# Upper end of each A-law segment, on the 13-bit magnitude (sample >> 3).
_SEG_END = (0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF)

# Leading/trailing samples quieter than this (about -50 dBFS) are trimmed:
# TTS engines often pad their output with silence, which only delays speech.
_TRIM_THRESHOLD = 100
# Peak normalisation target and the most it may boost (keeps noise down).
_PEAK_TARGET_DBFS = -1.0
_MAX_NORMALISE_GAIN_DB = 20.0


def linear_to_alaw(sample: int) -> int:
    """Encode one signed 16-bit sample to a G.711 A-law code."""
    pcm = sample >> 3
    if pcm >= 0:
        mask = 0xD5  # sign bit set for positive values (after the 0x55 XOR)
    else:
        mask = 0x55
        pcm = -pcm - 1
    seg = 0
    while seg < 8 and pcm > _SEG_END[seg]:
        seg += 1
    if seg >= 8:  # out of range: clip to the largest code
        return 0x7F ^ mask
    quant = (pcm >> 1) if seg < 2 else (pcm >> seg)
    return ((seg << 4) | (quant & 0x0F)) ^ mask


def alaw_to_linear(code: int) -> int:
    """Decode one G.711 A-law code to a signed 16-bit sample."""
    code ^= 0x55
    value = (code & 0x0F) << 4
    seg = (code & 0x70) >> 4
    if seg == 0:
        value += 8
    elif seg == 1:
        value += 0x108
    else:
        value = (value + 0x108) << (seg - 1)
    return value if code & 0x80 else -value


@cache
def _alaw_table() -> bytes:
    """Return the encoder table, indexed by the sample as an unsigned 16-bit."""
    return bytes(
        linear_to_alaw(u - 0x10000 if u & 0x8000 else u) for u in range(0x10000)
    )


def alaw_encode(pcm16: bytes) -> bytes:
    """Encode native-endian signed 16-bit mono PCM to A-law (1 byte/sample)."""
    samples = array("H")
    samples.frombytes(pcm16[: len(pcm16) & ~1])
    table = _alaw_table()
    return bytes(map(table.__getitem__, samples))


def alaw_decode(data: bytes) -> bytes:
    """Decode A-law to native-endian signed 16-bit PCM (for tests/diagnostics)."""
    return array("h", (alaw_to_linear(b) for b in data)).tobytes()


def decode_to_pcm8k(
    data: bytes, *, max_seconds: float = 30.0, format_hint: str | None = None
) -> tuple[bytes, bool]:
    """Decode any audio file PyAV/FFmpeg can read to 8 kHz mono signed 16-bit.

    Returns ``(pcm, truncated)``. libswresample (behind ``av.AudioResampler``)
    low-pass filters while downsampling, so nothing above 4 kHz aliases into
    the speech band. Raises ``ValueError`` when the data holds no audio.
    """
    import av  # noqa: PLC0415 - heavy import, only in the executor

    max_bytes = int(max_seconds * SAMPLE_RATE) * 2
    try:
        container = av.open(io.BytesIO(data), mode="r")
    except Exception:
        if not format_hint:
            raise
        # Raw streams (e.g. headerless mp3/aac) may need the demuxer named.
        container = av.open(io.BytesIO(data), mode="r", format=format_hint)

    out = bytearray()
    truncated = False
    with container:
        stream = next((s for s in container.streams if s.type == "audio"), None)
        if stream is None:
            raise ValueError("no audio stream in the media")
        resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)

        def _append(frames: list) -> None:
            for frame in frames:
                # The plane buffer is padded for alignment: keep real samples.
                out.extend(bytes(frame.planes[0])[: frame.samples * 2])

        for frame in container.decode(stream):
            frame.pts = None  # timestamps are irrelevant here; avoid pts checks
            _append(resampler.resample(frame))
            if len(out) >= max_bytes:
                truncated = True
                break
        else:
            _append(resampler.resample(None))  # flush the resampler

    if truncated:
        del out[max_bytes:]
    return bytes(out), truncated


def prepare_pcm(pcm16: bytes, *, gain_db: float = 0.0, normalize: bool = True) -> bytes:
    """Trim edge silence, peak-normalise and apply a gain (with clipping)."""
    samples = array("h")
    samples.frombytes(pcm16[: len(pcm16) & ~1])
    if not samples:
        return b""
    # Never more than 40 dB under the peak, so quiet recordings keep their
    # soft attacks (and a very quiet one is not trimmed away entirely).
    peak_all = max(max(samples), -min(samples))
    threshold = min(_TRIM_THRESHOLD, peak_all // 100)
    start, end = 0, len(samples)
    while start < end and abs(samples[start]) <= threshold:
        start += 1
    while end > start and abs(samples[end - 1]) <= threshold:
        end -= 1
    samples = samples[start:end]
    if not samples:
        return b""
    peak = max(max(samples), -min(samples))
    gain = 10 ** (gain_db / 20)
    if normalize and peak > 0:
        target = 32767 * 10 ** (_PEAK_TARGET_DBFS / 20)
        gain *= min(target / peak, 10 ** (_MAX_NORMALISE_GAIN_DB / 20))
    if abs(gain - 1.0) > 0.001:
        samples = array(
            "h", (max(-32768, min(32767, round(s * gain))) for s in samples)
        )
    return samples.tobytes()


def pcm_to_alaw_frames(pcm16: bytes) -> list[bytes]:
    """Encode 8 kHz mono PCM to A-law and split it into 20 ms frames.

    The last frame is padded with A-law silence to the full 160 bytes.
    """
    alaw = alaw_encode(pcm16)
    rem = len(alaw) % FRAME_BYTES
    if rem:
        alaw += bytes([ALAW_SILENCE]) * (FRAME_BYTES - rem)
    return [alaw[i : i + FRAME_BYTES] for i in range(0, len(alaw), FRAME_BYTES)]


def build_frames(
    data: bytes,
    *,
    format_hint: str | None = None,
    max_seconds: float = 30.0,
    gain_db: float = 0.0,
) -> tuple[list[bytes], bool]:
    """Turn an audio file into ready-to-send 20 ms A-law frames.

    Returns ``(frames, truncated)``; blocking, run it in an executor.
    """
    pcm, truncated = decode_to_pcm8k(
        data, max_seconds=max_seconds, format_hint=format_hint
    )
    return pcm_to_alaw_frames(prepare_pcm(pcm, gain_db=gain_db)), truncated
