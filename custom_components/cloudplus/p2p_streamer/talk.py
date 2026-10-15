"""Talk (phone mic -> camera speaker) queue shared by HA and the live loop.

The live loop owns the socket and the KCP tunnel, which are not thread safe,
so HA-side callers only queue: ``begin`` / ``push`` / ``stop``. The loop calls
``drain`` once per iteration (after the VVP heartbeat block) and sends the
open command, paced 40 ms audio frames and the close command itself.

Wire format and timing follow the ieGeek app capture; see docs/protocol.md
("Talk uplink"). The channel lives on the coordinator, so the frame sequence
continues across P2P sessions like the app's app-wide counter, and a talk
that spans a session reconnect re-opens on the new session.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from typing import Callable, NamedTuple

from .codec import build_talk_audio_frame, pcm16_to_ulaw
from .protocol import TALK_FRAME_BYTES, TALK_FRAME_INTERVAL_S, TALK_SAMPLE_RATE

_LOGGER = logging.getLogger(__name__)

# Stop a talk session after this long without audio to send.
TALK_IDLE_TIMEOUT_S = 2.0
# Hard limit per talk session.
TALK_MAX_S = 60.0
# Gap between the open command and the first frame (the app waits ~0.65 s).
TALK_OPEN_LEAD_S = 0.3
# If the loop falls this far behind the pacing clock, resync instead of
# bursting the backlog.
TALK_MAX_LAG_S = 0.2
# At most this many frames per loop iteration (the app packs up to 3).
TALK_MAX_FRAMES_PER_DRAIN = 3
TALK_QUEUE_MAX_FRAMES = int(TALK_MAX_S / TALK_FRAME_INTERVAL_S)
# Audio more than this far past its play time is dropped before it is sent
# (real-time audio is useless late; once sent, KCP must deliver it).
TALK_STALE_S = 2.0


class TalkPushResult(NamedTuple):
    """Frames accepted into the queue and frames refused (queue full)."""

    queued: int
    dropped: int


class TalkChannel:
    """Thread-safe talk queue; only the live loop touches the wire."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        # (due monotonic time, 320 bytes of u-law); due = when it should play.
        self._frames: deque[tuple[float, bytes]] = deque()
        self._last_due = 0.0
        self._logged_drop = False
        self._want_open = False
        self._session_open = False  # owned by the live loop
        self._started_at = 0.0
        self._last_activity = 0.0
        self._next_send = 0.0
        self._seq = 0
        self.stop_reason = ""

    # ---- HA side -------------------------------------------------------

    def begin(self) -> None:
        """Start a talk session (idempotent while one is running)."""
        with self._lock:
            now = self._clock()
            self._expire_locked(now)
            if self._want_open:
                self._last_activity = now
                return
            self._frames.clear()
            self._want_open = True
            self._started_at = now
            self._last_activity = now
            self._last_due = 0.0
            self._logged_drop = False
            self.stop_reason = ""

    def push(self, ulaw: bytes) -> TalkPushResult:
        """Queue G.711 u-law audio; reports frames queued and refused."""
        with self._lock:
            now = self._clock()
            self._expire_locked(now)
            if not self._want_open:
                raise RuntimeError("no talk session")
            data = bytes(ulaw)
            queued = dropped = 0
            for offset in range(0, len(data), TALK_FRAME_BYTES):
                if len(self._frames) >= TALK_QUEUE_MAX_FRAMES:
                    dropped += 1
                    continue
                chunk = data[offset : offset + TALK_FRAME_BYTES]
                if len(chunk) < TALK_FRAME_BYTES:  # pad with u-law silence
                    chunk += b"\xff" * (TALK_FRAME_BYTES - len(chunk))
                self._last_due = max(now, self._last_due + TALK_FRAME_INTERVAL_S)
                self._frames.append((self._last_due, chunk))
                queued += 1
            if queued:
                self._last_activity = now
            if dropped and not self._logged_drop:
                self._logged_drop = True
                _LOGGER.debug("Talk queue full; dropped %d frame(s)", dropped)
            return TalkPushResult(queued, dropped)

    def stop(self, reason: str = "stopped") -> None:
        """End the talk session.

        If talk is open on the wire, audio still due plays, then it closes.
        If it never opened, the queue is cleared: leftover audio must never
        open talk later (e.g. on the next motion wake).
        """
        with self._lock:
            self._stop_locked(reason)
            if not self._session_open:
                self._frames.clear()

    @property
    def active(self) -> bool:
        with self._lock:
            self._expire_locked(self._clock())
            return self._want_open

    @property
    def queued_s(self) -> float:
        with self._lock:
            return len(self._frames) * TALK_FRAME_INTERVAL_S

    @property
    def started_at(self) -> float:
        return self._started_at

    # ---- live loop side ------------------------------------------------

    def drain(
        self,
        send_open: Callable[[], None],
        send_close: Callable[[], None],
        send_frame: Callable[[bytes], None],
    ) -> int:
        """Send what is due on the wire; returns the number of audio frames."""
        actions: list[tuple[str, bytes]] = []
        with self._lock:
            now = self._clock()
            self._expire_locked(now)
            # Only a wanted talk opens; leftover frames never do.
            if self._want_open and not self._session_open:
                self._session_open = True
                self._next_send = now + TALK_OPEN_LEAD_S
                actions.append(("open", b""))
            if self._session_open:
                if self._next_send < now - TALK_MAX_LAG_S:
                    self._next_send = now
                budget = TALK_MAX_FRAMES_PER_DRAIN
                while self._frames and now >= self._next_send and budget > 0:
                    budget -= 1
                    _due, chunk = self._frames.popleft()
                    frame = build_talk_audio_frame(chunk, self._seq)
                    self._seq = (self._seq + 1) & 0xFFFFFFFF
                    self._next_send += TALK_FRAME_INTERVAL_S
                    self._last_activity = now
                    actions.append(("frame", frame))
                if not self._want_open and not self._frames:
                    self._session_open = False
                    actions.append(("close", b""))
        sent = 0
        for kind, payload in actions:
            if kind == "open":
                _LOGGER.debug("Talk open")
                send_open()
            elif kind == "frame":
                send_frame(payload)
                sent += 1
            else:
                _LOGGER.debug("Talk close (%s)", self.stop_reason or "stopped")
                send_close()
        return sent

    def session_ended(self, send_close: Callable[[], None] | None = None) -> None:
        """The P2P session is going away: close talk on it if it was open.

        A still-wanted talk re-opens on the next session.
        """
        with self._lock:
            was_open = self._session_open
            self._session_open = False
            if not self._want_open:
                self._frames.clear()
        if was_open and send_close is not None:
            try:
                send_close()
            except OSError:
                pass

    # ---- internals -----------------------------------------------------

    def _stop_locked(self, reason: str) -> None:
        if self._want_open:
            self.stop_reason = reason
        self._want_open = False

    def _expire_locked(self, now: float) -> None:
        stale = 0
        while self._frames and now - self._frames[0][0] > TALK_STALE_S:
            self._frames.popleft()
            stale += 1
        if stale:
            _LOGGER.debug("Talk dropped %d stale frame(s)", stale)
        if not self._want_open:
            return
        if now - self._started_at >= TALK_MAX_S:
            self._frames.clear()
            self._stop_locked("max duration")
        elif not self._frames and now - self._last_activity >= TALK_IDLE_TIMEOUT_S:
            self._stop_locked("idle")


def tone_ulaw(frequency_hz: float, duration_s: float, volume: float = 0.5) -> bytes:
    """A sine tone as G.711 u-law, 8 kHz mono, padded to whole 40 ms frames."""
    samples = int(round(duration_s * TALK_SAMPLE_RATE))
    samples = int(math.ceil(samples / TALK_FRAME_BYTES)) * TALK_FRAME_BYTES
    amplitude = max(0.0, min(1.0, float(volume))) * 32767.0
    step = 2.0 * math.pi * float(frequency_hz) / TALK_SAMPLE_RATE
    return pcm16_to_ulaw(
        int(amplitude * math.sin(step * index)) for index in range(samples)
    )


class TalkStreamerMixin:
    """P2PStreamer talk API: queue-only, drained by the live loop."""

    _talk: TalkChannel | None

    def talk_start(self) -> None:
        if self._talk is not None:
            self._talk.begin()

    def talk_push(self, ulaw: bytes) -> TalkPushResult:
        if self._talk is None:
            raise RuntimeError("talk is not available on this session")
        return self._talk.push(ulaw)

    def talk_stop(self) -> None:
        if self._talk is not None:
            self._talk.stop()
