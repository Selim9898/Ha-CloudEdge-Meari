"""Offline regressions for "wake when watched" on battery (snap) cameras.

A client on the local MPEG-TS server opens and extends the live window, with a
per-viewing-session cap so a client that never disconnects (HA preload stream,
Frigate, go2rtc) cannot keep the camera awake. No cloud, no camera.
"""

from __future__ import annotations

import asyncio
import importlib
import socket
import sys
import threading
import time
import unittest
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator
CONST = MODULES["const"]
WATCH = importlib.import_module("custom_components.cloudplus.coordinator.watch_wake")
STREAM_SERVER = importlib.import_module(
    "custom_components.cloudplus.coordinator.stream_server"
)

T0 = 1000.0
LINGER = WATCH.WATCH_WAKE_LINGER_S
GAP = WATCH.WATCH_SESSION_GAP_S
QUIET = WATCH.WATCH_CAPPED_QUIET_S


def load_switch() -> ModuleType:
    """Import the switch platform with the HA stubs it needs."""
    switch_stub = ModuleType("homeassistant.components.switch")
    switch_stub.SwitchEntity = type("SwitchEntity", (), {})
    const_stub = ModuleType("homeassistant.const")
    const_stub.EntityCategory = SimpleNamespace(CONFIG="config")
    with patch.dict(
        sys.modules,
        {
            "homeassistant.components.switch": switch_stub,
            "homeassistant.const": const_stub,
        },
    ):
        return importlib.import_module("custom_components.cloudplus.switch")


SWITCH = load_switch()


class _Loop:
    @staticmethod
    def is_closed() -> bool:
        return False

    @staticmethod
    def call_soon_threadsafe(cb, *args) -> None:
        cb(*args)


class FakeStreamServer:
    """Client presence on a synthetic monotonic timeline."""

    def __init__(self) -> None:
        self.client_count = 0
        self.occupied_since = 0.0
        self.last_left = 0.0

    def set(self, clients: int, now: float) -> None:
        if clients and not self.client_count:
            self.occupied_since = now
        if self.client_count and not clients:
            self.last_left = now
        self.client_count = clients

    def presence(self) -> Any:
        return STREAM_SERVER.ClientPresence(
            self.client_count, self.occupied_since, self.last_left
        )


def coordinator(
    category: str = "snap", options: dict | None = None, hass: Any = None
) -> Any:
    """A never-started coordinator with a fake stream server and wake call."""
    hass = hass or SimpleNamespace(loop=_Loop(), data={})
    device = {"deviceID": 1, "snNum": "synthetic-camera", "_category": category}
    entry = SimpleNamespace(options=options or {})
    coord = COORDINATOR(hass, "user@example.invalid", "x", device, entry=entry)
    coord._stream_server = FakeStreamServer()
    coord._send_wake = Mock()
    return coord


class WatchWakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coord = coordinator()
        self.cap = self.coord._watch_wake_max_s

    def watch(self, clients: int, now: float) -> None:
        """One snap-loop tick at synthetic monotonic time ``now``."""
        self.coord._stream_server.set(clients, now)
        with patch.object(WATCH, "time", SimpleNamespace(monotonic=lambda: now)):
            self.coord._update_watch_wake(now)

    def test_defaults_on_with_a_five_minute_cap(self) -> None:
        # A battery budget per viewing; the hourly budget is three caps
        # (15 min).
        self.assertTrue(self.coord.watch_wake_enabled)
        self.assertEqual(self.cap, 300.0)
        self.assertEqual(self.coord._watch_budget_s(), 900.0)

    def test_no_viewer_no_wake(self) -> None:
        self.watch(0, T0)
        self.coord._send_wake.assert_not_called()
        self.assertEqual(self.coord._live_deadline, 0.0)

    def test_viewer_wakes_once_and_keeps_extending(self) -> None:
        self.watch(1, T0)
        self.coord._send_wake.assert_called_once_with()
        self.assertEqual(self.coord._live_deadline, T0 + LINGER)
        for step in range(1, 20):
            self.watch(1, T0 + step)
        self.coord._send_wake.assert_called_once_with()
        self.assertEqual(self.coord._live_deadline, T0 + 19 + LINGER)

    def test_live_ends_shortly_after_the_viewer_leaves(self) -> None:
        self.watch(1, T0)
        self.watch(0, T0 + 30)
        self.assertEqual(self.coord._live_deadline, T0 + LINGER)

    def test_cap_stops_extending_for_a_client_that_never_leaves(self) -> None:
        self.watch(1, T0)
        for now in range(int(T0), int(T0 + self.cap) + 60, 5):
            self.watch(1, float(now))
        self.assertTrue(self.coord._watch.capped)
        # The window never reaches past the cap.
        self.assertLessEqual(self.coord._live_deadline, T0 + self.cap)

    def test_more_connections_during_a_capped_session_do_not_reset_it(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        deadline = self.coord._live_deadline
        self.watch(2, T0 + self.cap + 5)  # a second viewer joins
        self.watch(1, T0 + self.cap + 10)
        self.assertTrue(self.coord._watch.capped)
        self.assertEqual(self.coord._live_deadline, deadline)
        self.coord._send_wake.assert_called_once_with()

    def test_short_reconnect_keeps_the_session_and_its_cap_clock(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + 100)
        self.watch(0, T0 + 101)
        self.watch(1, T0 + 101 + GAP / 2)
        self.assertEqual(self.coord._watch.started_at, T0)

    def test_a_stale_loop_tick_does_not_end_the_session(self) -> None:
        self.watch(1, T0)
        # The loop was blocked (frame grab, wake call) while the viewer stayed;
        # it then catches a one-second reconnect gap.
        self.coord._stream_server.set(0, T0 + 100)
        self.coord._update_watch_wake(T0 + 101)
        self.watch(1, T0 + 102)
        self.assertEqual(self.coord._watch.started_at, T0)

    def test_session_starts_when_the_client_connected(self) -> None:
        self.coord._stream_server.set(1, T0)
        self.watch(1, T0 + 40)  # first tick after a 40 s frame grab
        self.assertEqual(self.coord._watch.started_at, T0)

    def test_capped_session_needs_a_long_quiet_period_to_end(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        # A client that reconnects after a 30 s+ backoff stays capped.
        self.watch(0, T0 + self.cap + 1)
        self.watch(0, T0 + self.cap + 1 + 2 * GAP)
        self.watch(1, T0 + self.cap + 2 + 2 * GAP)
        self.assertTrue(self.coord._watch.capped)
        self.assertEqual(self.coord._watch.started_at, T0)
        self.coord._send_wake.assert_called_once_with()

    def test_new_session_after_everyone_left_wakes_again(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        self.watch(0, T0 + self.cap + 1)
        self.watch(0, T0 + self.cap + 1 + QUIET)
        later = T0 + self.cap + 2 + QUIET
        self.watch(1, later)
        self.assertFalse(self.coord._watch.capped)
        self.assertEqual(self.coord._watch.started_at, later)
        self.assertEqual(self.coord._send_wake.call_count, 2)
        self.assertEqual(self.coord._live_deadline, later + LINGER)

    def test_explicit_wake_gives_its_window_but_keeps_the_cap(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        self.coord.wake_camera()  # normal wake window (motion timeout)
        self.assertGreater(self.coord._live_deadline, time.monotonic())
        self.watch(1, T0 + self.cap + 1)
        self.assertTrue(self.coord._watch.capped)
        self.assertEqual(self.coord._watch.started_at, T0)

    def test_disabled_never_wakes_or_extends(self) -> None:
        self.coord.set_watch_wake_enabled(False)
        self.watch(1, T0)
        self.watch(1, T0 + 5)
        self.coord._send_wake.assert_not_called()
        self.assertEqual(self.coord._live_deadline, 0.0)

    def test_toggling_does_not_re_arm_a_capped_session(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        deadline = self.coord._live_deadline
        self.coord.set_watch_wake_enabled(False)
        self.coord.set_watch_wake_enabled(True)
        self.watch(1, T0 + self.cap + 1)
        self.assertTrue(self.coord._watch.capped)
        self.assertEqual(self.coord._live_deadline, deadline)

    def test_rolling_budget_limits_repeated_sessions(self) -> None:
        budget = self.coord._watch_budget_s()
        self.assertEqual(budget, WATCH.WATCH_BUDGET_CAPS_PER_WINDOW * self.cap)
        now = T0
        for _ in range(WATCH.WATCH_BUDGET_CAPS_PER_WINDOW):
            # A full session, then everyone leaves for the capped quiet period.
            for step in range(0, int(self.cap) + 1, 5):
                self.watch(1, now + step)
            self.watch(0, now + self.cap + 1)
            self.watch(0, now + self.cap + 1 + QUIET)
            self.assertEqual(self.coord._watch.started_at, 0.0)
            now += self.cap + 2 + QUIET
        self.assertGreaterEqual(self.coord._watch.used(now), budget - 1)
        self.watch(1, now)  # a new session inside the same hour
        self.assertTrue(self.coord._watch.capped)
        self.assertLess(self.coord._live_deadline, now)
        self.assertLessEqual(self.coord._watch.used(now), budget + 1e-6)

    def test_reload_does_not_re_arm_the_cap_or_the_budget(self) -> None:
        self.watch(1, T0)
        self.watch(1, T0 + self.cap)
        self.assertTrue(self.coord._watch.capped)
        used = self.coord._watch.used(T0 + self.cap)

        # Options save / reload: a new coordinator for the same camera.
        reloaded = coordinator(hass=self.coord.hass)
        reloaded._stream_server.set(1, T0 + self.cap + 5)  # client reconnects
        reloaded._update_watch_wake(T0 + self.cap + 6)
        self.assertTrue(reloaded._watch.capped)
        self.assertEqual(reloaded._watch.used(T0 + self.cap), used)
        reloaded._send_wake.assert_not_called()

    def test_slow_wake_still_leaves_the_window_open(self) -> None:
        clock = SimpleNamespace(now=T0)

        def slow_wake() -> None:
            clock.now = T0 + 45  # two cloud calls near their read timeout

        self.coord._send_wake.side_effect = slow_wake
        self.coord._stream_server.set(1, T0)
        with patch.object(WATCH, "time", SimpleNamespace(monotonic=lambda: clock.now)):
            self.coord._update_watch_wake(T0)
        self.assertEqual(self.coord._live_deadline, T0 + 45 + LINGER)
        # The live loop now runs; the next tick must not wake again.
        self.watch(1, T0 + 45.2)
        self.coord._send_wake.assert_called_once_with()

    def test_wake_retries_are_rate_limited(self) -> None:
        self.watch(1, T0)
        for step in (1, 2, 5, 20):
            self.coord._live_deadline = 0.0  # e.g. the loop was blocked
            self.watch(1, T0 + step)
        self.coord._send_wake.assert_called_once_with()
        self.coord._live_deadline = 0.0
        self.watch(1, T0 + WATCH.WATCH_WAKE_RETRY_S)
        self.assertEqual(self.coord._send_wake.call_count, 2)

    def test_mains_cameras_are_untouched(self) -> None:
        coord = coordinator(category="ipc")
        coord._stream_server.set(1, T0)
        coord._update_watch_wake(T0)
        coord._send_wake.assert_not_called()
        self.assertEqual(coord._live_deadline, 0.0)

    def test_live_until_shows_the_cap_while_watched(self) -> None:
        now = time.monotonic()
        self.coord._stream_server.set(1, now)
        self.coord._update_watch_wake(now)
        self.coord._set_session_active(True)
        self.assertEqual(self.coord._live_end_mono(), now + self.cap)
        self.watch(0, now + 1)
        self.assertEqual(self.coord._live_end_mono(), self.coord._live_deadline)


class WatchWakeOptionsTests(unittest.TestCase):
    def test_options_turn_it_off_and_set_the_cap(self) -> None:
        coord = coordinator(
            options={
                CONST.CONF_WATCH_WAKE: False,
                CONST.CONF_WATCH_WAKE_MAX_MINUTES: 3,
            }
        )
        self.assertFalse(coord.watch_wake_enabled)
        self.assertEqual(coord._watch_wake_max_s, 180.0)

    def test_cap_is_clamped(self) -> None:
        for raw, minutes in ((0, 1), (999, 60), ("bad", 5), (None, 5)):
            with self.subTest(raw=raw):
                options = {CONST.CONF_WATCH_WAKE_MAX_MINUTES: raw}
                self.assertEqual(WATCH.watch_wake_max_minutes(options), minutes)

    def test_switch_persists_its_state_in_entry_options(self) -> None:
        coord = coordinator()
        entry = SimpleNamespace(entry_id="test", options={"stream_quality": "AUTO"})
        switch = SWITCH.CloudEdgeMeariWatchWakeSwitch(coord, entry)
        switch.hass = Mock()
        switch.async_write_ha_state = Mock()
        self.assertTrue(switch.is_on)
        self.assertEqual(switch._attr_entity_category, "config")

        switch._set(False)
        self.assertFalse(coord.watch_wake_enabled)
        switch.hass.config_entries.async_update_entry.assert_called_once_with(
            entry, options={"stream_quality": "AUTO", CONST.CONF_WATCH_WAKE: False}
        )

    def test_switch_only_on_battery_cameras(self) -> None:
        for category, expected in (("snap", 1), ("ipc", 0)):
            with self.subTest(category=category):
                coord = coordinator(category=category)
                entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
                hass = SimpleNamespace(data={SWITCH.DOMAIN: {entry.entry_id: coord}})
                add_entities = Mock()
                asyncio.run(SWITCH.async_setup_entry(hass, entry, add_entities))
                added = [
                    entity
                    for call in add_entities.call_args_list
                    for entity in call.args[0]
                    if isinstance(entity, SWITCH.CloudEdgeMeariWatchWakeSwitch)
                ]
                self.assertEqual(len(added), expected)


class _FakeWorker:
    @staticmethod
    def is_alive() -> bool:
        return True

    @staticmethod
    def join(timeout: float | None = None) -> None:
        del timeout
        time.sleep(0.02)


class LiveLoopCapTests(unittest.TestCase):
    def test_a_permanent_client_gets_live_only_until_the_cap(self) -> None:
        coord = coordinator()
        coord._running = True
        coord._watch_wake_max_s = 0.8
        coord._stream_server.set(1, time.monotonic())
        coord._start_streamer_once = lambda **_kw: setattr(
            coord, "_stream_thread", _FakeWorker()
        )
        coord._stream_video_stale = lambda _now: False

        started = time.monotonic()
        with patch.object(WATCH, "WATCH_WAKE_LINGER_S", 0.2):
            coord._update_watch_wake(started)
            coord._run_live_until_deadline()
        elapsed = time.monotonic() - started

        # Well past one linger window (kept alive by the viewer), ended by the cap.
        self.assertGreater(elapsed, 0.7)
        self.assertLess(elapsed, 2.0)
        self.assertFalse(coord._session_active)

        # The snap loop's next tick sees the cap and leaves the idle still up.
        deadline = coord._live_deadline
        coord._update_watch_wake(time.monotonic())
        self.assertTrue(coord._watch.capped)
        self.assertEqual(coord._live_deadline, deadline)
        coord._send_wake.assert_called_once_with()


class NoPeriodicWakeTests(unittest.TestCase):
    """The app sends no wake while live; neither do we."""

    def test_a_long_watched_session_sends_only_the_initial_wake(self) -> None:
        coord = coordinator()
        now = T0
        while now <= T0 + coord._watch_wake_max_s:
            coord._stream_server.set(1, now)
            with patch.object(WATCH, "time", SimpleNamespace(monotonic=lambda: now)):
                coord._update_watch_wake(now)
            now += 0.5
        coord._send_wake.assert_called_once_with()


class FakeMuxer:
    def __init__(self) -> None:
        self.stills = 0

    def start(self, *_args, **_kwargs) -> None:
        pass

    def stop(self) -> None:
        pass

    def write_video(self, *_args, **_kwargs) -> None:
        self.stills += 1


class StallStillTests(unittest.TestCase):
    """A stalled watched session shows the idle still, not a frozen frame."""

    def setUp(self) -> None:
        self.coord = coordinator()
        now = time.monotonic()
        self.coord._stream_server.set(1, now)
        self.coord._update_watch_wake(now)  # watch session, live window open
        self.coord._set_session_active(True)

    def stall(self, seconds: float) -> None:
        self.coord._last_live_frame_time = time.monotonic() - seconds

    def test_covers_only_after_frames_stop_for_a_while(self) -> None:
        self.stall(1.0)
        self.assertFalse(self.coord._cover_stall_with_still())
        self.stall(WATCH.WATCH_STALL_STILL_S + 1)
        self.assertTrue(self.coord._cover_stall_with_still())
        self.assertTrue(self.coord._idle_covering)

    def test_not_before_the_first_live_frame(self) -> None:
        self.coord._last_live_frame_time = 0.0  # still waking
        self.assertFalse(self.coord._cover_stall_with_still())

    def test_not_outside_a_watch_session(self) -> None:
        self.coord._stream_server.set(0, time.monotonic())
        self.stall(WATCH.WATCH_STALL_STILL_S + 1)
        self.assertFalse(self.coord._cover_stall_with_still())

    def test_awake_is_off_while_the_still_covers(self) -> None:
        self.stall(WATCH.WATCH_STALL_STILL_S + 1)
        self.coord._cover_stall_with_still()
        self.coord._refresh_camera_awake()
        self.assertFalse(self.coord.camera_awake)

    def test_live_resumes_on_a_keyframe(self) -> None:
        streamer = Mock()
        self.coord._p2p_streamer = streamer
        self.coord._stream_started_keyframe = True
        self.coord._idle_covering = True
        self.coord._take_back_from_idle()
        self.assertFalse(self.coord._idle_covering)
        self.assertFalse(self.coord._stream_started_keyframe)
        streamer.request_keyframe.assert_called_once_with()

    def run_idle_loop(self) -> int:
        coord = self.coord
        coord._running = True
        coord._idle_video_frame = b"keyframe"
        coord._muxer = FakeMuxer()
        coord._stream_server.bootstrap_state = lambda: {"ready": True}
        worker = threading.Thread(target=coord._idle_loop, daemon=True)
        worker.start()
        time.sleep(0.4)
        coord._idle_stop.set()
        worker.join(timeout=2)
        return coord._muxer.stills

    def test_idle_loop_serves_the_still_during_a_stall(self) -> None:
        self.stall(WATCH.WATCH_STALL_STILL_S + 1)
        self.assertGreater(self.run_idle_loop(), 0)

    def test_idle_loop_stays_out_while_live_frames_flow(self) -> None:
        self.stall(0.0)
        self.assertEqual(self.run_idle_loop(), 0)


class DeadlineThreadSafetyTests(unittest.TestCase):
    """The deadline and budget are written from the HA loop and from threads."""

    def test_deadline_never_moves_earlier(self) -> None:
        coord = coordinator()
        coord._raise_live_deadline(T0 + 100)
        coord._raise_live_deadline(T0 + 50)
        self.assertEqual(coord._live_deadline, T0 + 100)
        self.assertEqual(coord._grant_live(T0, T0 + 80), 0.0)
        self.assertEqual(coord._live_deadline, T0 + 100)
        self.assertEqual(coord._grant_live(T0, T0 + 130), 30.0)
        self.assertEqual(coord._live_deadline, T0 + 130)

    def test_concurrent_grants_and_budget_reads(self) -> None:
        coord = coordinator()
        errors: list[BaseException] = []
        seen: list[float] = []
        stop = threading.Event()

        def writer(offset: float) -> None:
            try:
                step = 0
                while not stop.is_set():
                    step += 1
                    now = time.monotonic()
                    # Disjoint spans so the grants deque keeps growing.
                    coord._watch.grant(offset + step * 10.0, offset + step * 10.0 + 1.0)
                    coord._grant_live(now, now + 0.001 * step)
            except BaseException as exc:  # pylint: disable=broad-exception-caught
                errors.append(exc)

        def reader() -> None:
            try:
                while not stop.is_set():
                    coord._watch.used(time.monotonic() + 7200.0)  # pops old spans
                    seen.append(coord._live_deadline)
            except BaseException as exc:  # pylint: disable=broad-exception-caught
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(o,)) for o in (0.0, 1e6)]
        threads.append(threading.Thread(target=reader))
        for worker in threads:
            worker.start()
        time.sleep(0.5)
        stop.set()
        for worker in threads:
            worker.join(timeout=2)
        self.assertEqual(errors, [])
        self.assertEqual(seen, sorted(seen))  # never observed moving earlier


class StreamServerClientCountTests(unittest.TestCase):
    def test_closed_viewer_leaves_the_count_without_any_broadcast(self) -> None:
        server = STREAM_SERVER.StreamServer()
        server.start()
        try:
            viewer = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            self.assertTrue(self._wait(lambda: server.client_count == 1))
            viewer.close()
            # Nothing is broadcast (e.g. during a camera wake); the count must
            # still drop so watch-driven live time ends with the viewer.
            self.assertTrue(self._wait(lambda: server.client_count == 0))
        finally:
            server.stop()

    def test_presence_records_connect_and_leave_times(self) -> None:
        server = STREAM_SERVER.StreamServer()
        server.start()
        try:
            before = time.monotonic()
            viewer = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            self.assertTrue(self._wait(lambda: server.presence().count == 1))
            self.assertGreaterEqual(server.presence().occupied_since, before)
            viewer.close()
            self.assertTrue(self._wait(lambda: server.presence().count == 0))
            self.assertGreaterEqual(
                server.presence().last_left, server.presence().occupied_since
            )
        finally:
            server.stop()

    def test_open_viewer_stays_counted(self) -> None:
        server = STREAM_SERVER.StreamServer()
        server.start()
        try:
            viewer = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            self.assertTrue(self._wait(lambda: server.client_count == 1))
            time.sleep(1.2)
            self.assertEqual(server.client_count, 1)
            viewer.close()
        finally:
            server.stop()

    @staticmethod
    def _wait(predicate, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return predicate()


if __name__ == "__main__":
    unittest.main()
