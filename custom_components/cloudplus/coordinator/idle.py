"""Idle still for battery cameras: repeat the last keyframe while asleep.

Moved out of ``coordinator/__init__.py`` (unchanged) to keep that module
under the 1000-line limit.
"""

from __future__ import annotations

import threading
import time

IDLE_ADVERTISED_FPS = 15.0
IDLE_REFRESH_INTERVAL = 0.5
IDLE_NO_CLIENT_SLEEP = 1.0


class IdleStillMixin:
    """Coordinator mixin owning the idle still loop."""

    def _start_idle_loop(self) -> None:
        if not self._is_snap:
            return
        if self._idle_thread is not None and self._idle_thread.is_alive():
            return
        self._idle_stop.clear()
        self._idle_thread = threading.Thread(
            target=self._idle_loop,
            name=f"cloudplus_idle_{self._sn_num}",
            daemon=True,
        )
        self._idle_thread.start()

    def _idle_loop(self) -> None:
        next_frame = time.monotonic()
        pts_interval = IDLE_REFRESH_INTERVAL
        while self._running and not self._idle_stop.is_set():
            if self._idle_stream_reset.is_set():
                self._idle_stream_reset.clear()
                self._muxer.stop()
                self._stream_server.reset_bootstrap()
                next_frame = time.monotonic()

            if self._session_active and not self._cover_stall_with_still():
                next_frame = time.monotonic()
                self._idle_stop.wait(0.1)
                continue

            with self._idle_frame_lock:
                frame = self._idle_video_frame
                codec = self._idle_video_codec
            if not frame:
                self._idle_stop.wait(0.2)
                continue

            has_clients = self._stream_server.client_count > 0
            bootstrap_ready = bool(
                self._stream_server.bootstrap_state().get("ready", False)
            )
            if not has_clients and bootstrap_ready:
                self._muxer.stop()
                next_frame = time.monotonic()
                self._idle_stop.wait(IDLE_NO_CLIENT_SLEEP)
                continue

            self._muxer.start(codec, advertised_fps=IDLE_ADVERTISED_FPS)
            self._muxer.write_video(frame, pts_interval_s=pts_interval)
            next_frame += IDLE_REFRESH_INTERVAL
            delay = max(0.0, next_frame - time.monotonic())
            if delay > IDLE_REFRESH_INTERVAL:
                next_frame = time.monotonic() + IDLE_REFRESH_INTERVAL
                delay = IDLE_REFRESH_INTERVAL
            self._idle_stop.wait(delay)

    def _prime_idle_stream(self) -> None:
        with self._idle_frame_lock:
            frame = self._idle_video_frame
            codec = self._idle_video_codec
        if not frame:
            return
        self._muxer.start(codec, advertised_fps=IDLE_ADVERTISED_FPS)
        self._muxer.replace_video(frame, pts_interval_s=IDLE_REFRESH_INTERVAL)
