"""Wake a battery camera while someone watches its stream ("wake when watched").

A snap camera only goes live inside a live window (motion or the Wake button);
otherwise the stream server repeats the last keyframe. With this enabled, any
client connected to the local MPEG-TS server opens and keeps extending that
window, so a viewer gets live video without pressing a button.

Battery guards, all aimed at clients that hold the port open (HA preload
stream, HA's stream worker for ~30 s after the last viewer, Frigate, go2rtc) or
reconnect in a loop:

- One viewing session (clients connected, short reconnect gaps tolerated)
  gets at most ``max minutes`` of watch-driven live video.
- A capped session only ends after every client has been gone for
  ``WATCH_CAPPED_QUIET_S``. Extra connections, an explicit Wake (which gives
  its normal window) and toggling the option do not re-arm it.
- Watch-driven live time is also limited per rolling hour.
- Session and budget state live in ``hass.data`` per camera, so an entry
  reload (options save) does not re-arm them. An HA restart does.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ..const import (
    CONF_WATCH_WAKE,
    CONF_WATCH_WAKE_MAX_MINUTES,
    DEFAULT_WATCH_WAKE,
    DEFAULT_WATCH_WAKE_MAX_MINUTES,
    DOMAIN,
    WATCH_WAKE_MAX_MINUTES_RANGE,
)

_LOGGER = logging.getLogger(__name__)

# How long live continues after the last viewer leaves (covers a quick
# reconnect or a dashboard re-render without re-waking the camera).
WATCH_WAKE_LINGER_S = 10.0
# Clients gone for this long end an uncapped viewing session.
WATCH_SESSION_GAP_S = 30.0
# Clients gone for this long end a capped viewing session.
WATCH_CAPPED_QUIET_S = 300.0
# Minimum spacing between watch-triggered HTTP wake requests.
WATCH_WAKE_RETRY_S = 30.0
# Live frames missing this long during a watch session: serve the idle still
# instead of a frozen picture until frames resume (or the session reconnects).
WATCH_STALL_STILL_S = 5.0
# Watch-driven live time per rolling window: this many session caps.
WATCH_BUDGET_WINDOW_S = 3600.0
WATCH_BUDGET_CAPS_PER_WINDOW = 3
WATCH_STATE_KEY = "_watch_wake"


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


@dataclass
class WatchState:
    """Per-camera viewing-session bookkeeping that outlives entry reloads."""

    started_at: float = 0.0  # monotonic; 0 = no session
    ended_at: float = 0.0
    capped: bool = False
    last_seen: float = 0.0  # last time a client was known to be connected
    last_wake: float = float("-inf")
    grants: deque[list[float]] = field(default_factory=deque)

    def used(self, now: float) -> float:
        """Watch-granted live seconds inside the rolling window."""
        horizon = now - WATCH_BUDGET_WINDOW_S
        while self.grants and self.grants[0][1] <= horizon:
            self.grants.popleft()
        return sum(end - max(start, horizon) for start, end in self.grants)

    def grant(self, start: float, end: float) -> None:
        if self.grants and start <= self.grants[-1][1]:
            self.grants[-1][1] = max(self.grants[-1][1], end)
        else:
            self.grants.append([start, end])


class WatchWakeMixin:
    """Coordinator mixin: keep the live window open while clients watch."""

    _idle_covering = False  # the idle loop is covering a live stall

    def _init_watch_wake(self, options: Mapping[str, Any] | None) -> None:
        options = options or {}
        self._watch_wake_enabled = bool(
            options.get(CONF_WATCH_WAKE, DEFAULT_WATCH_WAKE)
        )
        self._watch_wake_max_s = 60.0 * watch_wake_max_minutes(options)
        states = self.hass.data.setdefault(DOMAIN, {}).setdefault(
            WATCH_STATE_KEY, {}
        )
        self._watch: WatchState = states.setdefault(self._sn_num, WatchState())

    @property
    def watch_wake_enabled(self) -> bool:
        return self._watch_wake_enabled

    def set_watch_wake_enabled(self, enabled: bool) -> None:
        # Toggling does not reset the session or the budget.
        self._watch_wake_enabled = bool(enabled)
        self._fire_update()

    def _watch_budget_s(self) -> float:
        return min(
            WATCH_BUDGET_WINDOW_S,
            WATCH_BUDGET_CAPS_PER_WINDOW * self._watch_wake_max_s,
        )

    def _watch_wake_active(self) -> bool:
        state = self._watch
        return (
            self._watch_wake_enabled
            and state.started_at > 0.0
            and not state.capped
            and self._stream_server.client_count > 0
        )

    def _live_end_mono(self) -> float:
        end = super()._live_end_mono()
        if self._watch_wake_active():
            # Live lasts until the cap (or the budget) if the viewer stays.
            now = time.monotonic()
            remaining = max(0.0, self._watch_budget_s() - self._watch.used(now))
            planned = min(
                self._watch.started_at + self._watch_wake_max_s,
                max(end, now) + remaining,
            )
            end = max(end, planned)
        return end

    def _update_watch_wake(self, now: float) -> None:
        """Track the viewing session and extend the live window while watched."""
        if not self._is_snap:
            return
        state = self._watch
        # Presence comes from the server's own connect/disconnect times, so a
        # loop that was blocked (frame grab, wake, status poll) cannot mistake
        # one zero-client instant for a long gap.
        presence = self._stream_server.presence()
        if presence.count <= 0:
            state.last_seen = max(state.last_seen, presence.last_left)
            quiet = WATCH_CAPPED_QUIET_S if state.capped else WATCH_SESSION_GAP_S
            if state.started_at and now - state.last_seen >= quiet:
                state.started_at = 0.0
                state.ended_at = now
                state.capped = False
            return
        state.last_seen = now
        if not state.started_at:
            # Count from when the client connected (the loop may have been
            # busy), but never from before the previous session ended.
            started = max(presence.occupied_since, state.ended_at)
            state.started_at = min(now, started) if started > 0 else now
        if not self._watch_wake_enabled or state.capped:
            return
        cap_end = state.started_at + self._watch_wake_max_s
        if now >= cap_end:
            self._cap_watch_session(
                f"reached its {self._watch_wake_max_s / 60.0:.0f} min cap"
            )
            return
        if self._watch_budget_s() - state.used(now) <= 0.0:
            self._cap_watch_session("used its hourly watch budget")
            return
        was_live = now < self._live_deadline
        # Open the window before waking: the wake can block for a while.
        self._extend_watch_window(now, cap_end)
        # Only the initial wake: the official app sends no wake while live
        # (the live session itself is kept up by the P2P/MsgSvr keep-alives).
        if was_live or now - state.last_wake < WATCH_WAKE_RETRY_S:
            return
        state.last_wake = now
        _LOGGER.info(
            "Wake when watched: stream client on %s; waking for live video",
            self._sn_num,
        )
        self._send_wake()
        # The wake is two cloud HTTP calls (up to ~30 s each). Re-anchor the
        # window on the time it returned, or a slow wake leaves the deadline
        # already in the past and the next loop tick wakes again.
        if self._stream_server.client_count > 0:
            self._extend_watch_window(time.monotonic(), cap_end)

    def _cover_stall_with_still(self) -> bool:
        """True while the idle still should cover a stalled watched session."""
        covering = (
            self._watch_wake_active()
            and self._last_live_frame_time > 0.0
            and time.monotonic() - self._last_live_frame_time > WATCH_STALL_STILL_S
        )
        if covering and not self._idle_covering:
            _LOGGER.info(
                "Wake when watched: no live video from %s for %.0fs; serving "
                "the idle still until it resumes",
                self._sn_num,
                WATCH_STALL_STILL_S,
            )
            self._idle_covering = True
        return covering

    def _take_back_from_idle(self) -> None:
        """Live frames are back after a covered stall: restart on a keyframe.

        The idle loop restarted the muxer for the still, so the live stream
        must begin again with a keyframe and fresh timing.
        """
        self._idle_covering = False
        self._stream_started_keyframe = False
        self._reset_live_mux_timing()
        streamer = self._p2p_streamer
        if streamer is not None:
            streamer.request_keyframe()

    def _cap_watch_session(self, reason: str) -> None:
        self._watch.capped = True
        _LOGGER.info(
            "Wake when watched: %s %s; serving the idle still until viewers "
            "have been gone for %.0f min",
            self._sn_num,
            reason,
            WATCH_CAPPED_QUIET_S / 60.0,
        )

    def _extend_watch_window(self, now: float, cap_end: float) -> None:
        state = self._watch
        grant_from = max(self._live_deadline, now)
        remaining = self._watch_budget_s() - state.used(now)
        target = min(now + WATCH_WAKE_LINGER_S, cap_end, grant_from + remaining)
        if target > grant_from:
            state.grant(grant_from, target)
            self._live_deadline = target
