"""Wake a battery camera while someone watches its stream ("wake when watched").

A snap camera only goes live inside a live window (motion or the Wake button);
otherwise the stream server repeats the last keyframe. With this enabled, any
client connected to the local MPEG-TS server opens and keeps extending that
window, so a viewer gets live video without pressing a button.

Battery guard: one continuous viewing session (clients connected, with short
reconnect gaps tolerated) gets at most ``max minutes`` of watch-driven live
video. After that the stream falls back to the idle still until every client
has been gone for ``WATCH_SESSION_GAP_S`` or someone presses Wake. Extra
connections during a capped session do not reset it, so a client that holds
the port open (HA preload stream, Frigate, go2rtc) or reconnects in a loop
cannot keep the camera awake.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from ..const import (
    CONF_WATCH_WAKE,
    CONF_WATCH_WAKE_MAX_MINUTES,
    DEFAULT_WATCH_WAKE,
    DEFAULT_WATCH_WAKE_MAX_MINUTES,
    WATCH_WAKE_MAX_MINUTES_RANGE,
)

_LOGGER = logging.getLogger(__name__)

# How long live continues after the last viewer leaves (covers a quick
# reconnect or a dashboard re-render without re-waking the camera).
WATCH_WAKE_LINGER_S = 10.0
# Clients gone for this long end the viewing session (and reset the cap).
WATCH_SESSION_GAP_S = 30.0


def watch_wake_max_minutes(options: Mapping[str, Any] | None) -> int:
    """Return the configured per-session cap in minutes, clamped."""
    low, high = WATCH_WAKE_MAX_MINUTES_RANGE
    raw = (options or {}).get(
        CONF_WATCH_WAKE_MAX_MINUTES, DEFAULT_WATCH_WAKE_MAX_MINUTES
    )
    try:
        minutes = int(raw)
    except (TypeError, ValueError):
        minutes = DEFAULT_WATCH_WAKE_MAX_MINUTES
    return max(low, min(high, minutes))


class WatchWakeMixin:
    """Coordinator mixin: keep the live window open while clients watch."""

    def _init_watch_wake(self, options: Mapping[str, Any] | None) -> None:
        options = options or {}
        self._watch_wake_enabled = bool(
            options.get(CONF_WATCH_WAKE, DEFAULT_WATCH_WAKE)
        )
        self._watch_wake_max_s = 60.0 * watch_wake_max_minutes(options)
        self._watch_started_at = 0.0
        self._watch_last_seen = 0.0
        self._watch_capped = False

    @property
    def watch_wake_enabled(self) -> bool:
        return self._watch_wake_enabled

    def set_watch_wake_enabled(self, enabled: bool) -> None:
        self._watch_wake_enabled = bool(enabled)
        self._reset_watch_session()
        self._fire_update()

    def wake_camera(self) -> None:
        # An explicit wake starts a fresh viewing session (and cap).
        self._reset_watch_session()
        super().wake_camera()

    def _reset_watch_session(self) -> None:
        self._watch_started_at = 0.0
        self._watch_capped = False

    def _watch_wake_active(self) -> bool:
        return (
            self._watch_wake_enabled
            and self._watch_started_at > 0.0
            and not self._watch_capped
            and self._stream_server.client_count > 0
        )

    def _live_end_mono(self) -> float:
        end = super()._live_end_mono()
        if self._watch_wake_active():
            # Live lasts until the cap if the viewer keeps watching.
            end = max(end, self._watch_started_at + self._watch_wake_max_s)
        return end

    def _update_watch_wake(self, now: float) -> None:
        """Track the viewing session and extend the live window while watched."""
        if not self._is_snap:
            return
        if self._stream_server.client_count <= 0:
            if (
                self._watch_started_at
                and now - self._watch_last_seen >= WATCH_SESSION_GAP_S
            ):
                self._reset_watch_session()
            return
        self._watch_last_seen = now
        if not self._watch_started_at:
            self._watch_started_at = now
            self._watch_capped = False
        if not self._watch_wake_enabled or self._watch_capped:
            return
        cap_end = self._watch_started_at + self._watch_wake_max_s
        if now >= cap_end:
            self._watch_capped = True
            _LOGGER.info(
                "Wake when watched: %s reached its %.0f min cap; serving the "
                "idle still until viewers leave or the camera is woken",
                self._sn_num,
                self._watch_wake_max_s / 60.0,
            )
            return
        if now >= self._live_deadline:
            _LOGGER.info(
                "Wake when watched: stream client on %s; waking for live video",
                self._sn_num,
            )
            self._send_wake()
        self._live_deadline = max(
            self._live_deadline, min(now + WATCH_WAKE_LINGER_S, cap_end)
        )
