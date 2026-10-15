"""Offline regressions for a truthful Camera Awake sensor and ``live_until``.

Camera Awake must follow what the stream actually serves: on while live frames
arrive (or the initial frame grab runs), off while a wake is still pending and
while the idle still is served. No threads beyond the test, no cloud, no camera.
"""

from __future__ import annotations

import importlib
import sys
import time
import unittest
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator
STATE = importlib.import_module("custom_components.cloudplus.coordinator.state")


def load_binary_sensor() -> ModuleType:
    """Import the binary_sensor platform with the HA stub it needs."""
    stub = ModuleType("homeassistant.components.binary_sensor")
    stub.BinarySensorEntity = type("BinarySensorEntity", (), {})
    stub.BinarySensorDeviceClass = SimpleNamespace(
        MOTION="motion", RUNNING="running", BATTERY_CHARGING="battery_charging"
    )
    with patch.dict(sys.modules, {"homeassistant.components.binary_sensor": stub}):
        return importlib.import_module("custom_components.cloudplus.binary_sensor")


BINARY_SENSOR = load_binary_sensor()


class _Loop:
    """Event-loop stand-in that runs coordinator callbacks inline."""

    @staticmethod
    def is_closed() -> bool:
        return False

    @staticmethod
    def call_soon_threadsafe(cb, *args) -> None:
        cb(*args)


def snap_coordinator() -> Any:
    """A battery ("snap") camera coordinator that is never started."""
    hass = SimpleNamespace(loop=_Loop(), data={})
    device = {"deviceID": 1, "snNum": "synthetic-camera", "_category": "snap"}
    return COORDINATOR(hass, "user@example.invalid", "x", device)


class _FakeWorker:
    """A P2P worker that starts delivering frames after a simulated wake."""

    def __init__(self, coord: Any, frames_after_joins: int) -> None:
        self._coord = coord
        self._joins = 0
        self._frames_after = frames_after_joins

    @staticmethod
    def is_alive() -> bool:
        return True

    def join(self, timeout: float | None = None) -> None:
        del timeout
        self._joins += 1
        if self._joins > self._frames_after:
            self._coord._last_live_frame_time = time.monotonic()
        time.sleep(0.02)


class AwakeStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coord = snap_coordinator()
        self.history: list[bool] = []
        self.coord.register_update_callback(
            lambda: self.history.append(self.coord.camera_awake)
        )

    def test_wake_request_alone_is_not_awake(self) -> None:
        self.coord.wake_camera()
        self.coord._refresh_camera_awake()
        self.assertTrue(self.coord._session_active)  # idle still loop paused
        self.assertFalse(self.coord.camera_awake)  # no live frame yet

    def test_live_frames_turn_awake_on_and_a_long_gap_turns_it_off(self) -> None:
        self.coord.wake_camera()
        now = time.monotonic()
        self.coord._last_live_frame_time = now
        self.coord._refresh_camera_awake(now)
        self.assertTrue(self.coord.camera_awake)

        # A short source pause inside the session keeps it on.
        self.coord._refresh_camera_awake(now + 3.0)
        self.assertTrue(self.coord.camera_awake)

        gap = STATE.AWAKE_LIVE_FRAME_TIMEOUT_S
        self.coord._refresh_camera_awake(now + gap + 0.1)
        self.assertFalse(self.coord.camera_awake)

    def test_idle_still_is_not_awake_even_right_after_live(self) -> None:
        self.coord.wake_camera()
        self.coord._last_live_frame_time = time.monotonic()
        self.coord._refresh_camera_awake()
        self.assertTrue(self.coord.camera_awake)

        self.coord._set_session_active(False)  # live window over, idle still
        self.coord._refresh_camera_awake()
        self.assertFalse(self.coord.camera_awake)
        self.assertEqual(self.coord._last_live_frame_time, 0.0)

    def test_initial_frame_grab_reports_awake_only_while_it_runs(self) -> None:
        seen: list[bool] = []

        def fake_wake() -> None:
            seen.append(self.coord.camera_awake)
            self.coord._idle_frame_ready.set()

        self.coord._running = True
        self.coord._send_wake = fake_wake
        self.coord._start_streamer_once = Mock()
        self.coord._stop_streamer = Mock()
        self.assertTrue(self.coord._run_initial_frame_grab())
        self.assertEqual(seen, [True])
        self.assertFalse(self.coord.camera_awake)
        self.assertFalse(self.coord._session_active)

    def test_live_loop_reports_awake_from_first_frame_to_window_end(self) -> None:
        coord = self.coord
        coord._running = True
        coord._live_deadline = time.monotonic() + 0.6
        worker = _FakeWorker(coord, frames_after_joins=3)

        def start_streamer(**_kwargs) -> None:
            coord._stream_thread = worker

        coord._start_streamer_once = start_streamer
        coord._stream_video_stale = lambda _now: False
        coord._run_live_until_deadline()

        # Off during the simulated wake, on with frames, off at window end.
        transitions = [state for i, state in enumerate(self.history)
                       if i == 0 or state != self.history[i - 1]]
        self.assertEqual(transitions, [False, True, False])
        self.assertFalse(coord.camera_awake)
        self.assertFalse(coord._session_active)


class LiveUntilTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coord = snap_coordinator()

    def test_none_without_a_live_window(self) -> None:
        self.assertIsNone(self.coord.live_until)

    def test_tracks_the_live_deadline(self) -> None:
        self.coord.wake_camera()  # default window: the motion timeout
        self.coord._refresh_camera_awake()
        live_until = self.coord.live_until
        self.assertIsNotNone(live_until)
        remaining = (live_until - datetime.now(timezone.utc)).total_seconds()
        self.assertAlmostEqual(remaining, self.coord.motion_timeout, delta=2.0)

    def test_republished_only_when_the_end_moves(self) -> None:
        updates = Mock()
        self.coord.register_update_callback(updates)
        self.coord.wake_camera()
        self.coord._refresh_camera_awake()
        self.assertEqual(updates.call_count, 1)
        self.coord._refresh_camera_awake()
        self.assertEqual(updates.call_count, 1)
        self.coord._extend_live_deadline(self.coord.motion_timeout + 60)
        self.coord._refresh_camera_awake()
        self.assertEqual(updates.call_count, 2)

    def test_value_is_whole_seconds_and_stable_between_moves(self) -> None:
        self.coord.wake_camera()
        self.coord._refresh_camera_awake()
        first = self.coord.live_until
        self.assertEqual(first.microsecond, 0)
        for _ in range(5):  # loop ticks while nothing moves the deadline
            time.sleep(0.01)
            self.coord._refresh_camera_awake()
            self.assertEqual(self.coord.live_until, first)
        self.coord._live_deadline += 0.5  # sub-second jitter is ignored
        self.coord._refresh_camera_awake()
        self.assertEqual(self.coord.live_until, first)
        self.coord._live_deadline += 60
        self.coord._refresh_camera_awake()
        moved = (self.coord.live_until - first).total_seconds()
        self.assertAlmostEqual(moved, 60, delta=1)

    def test_cleared_when_the_live_window_ends(self) -> None:
        self.coord.wake_camera()
        self.coord._refresh_camera_awake()
        self.assertIsNotNone(self.coord.live_until)
        self.coord._set_session_active(False)
        self.coord._refresh_camera_awake()
        self.assertIsNone(self.coord.live_until)

    def test_awake_sensor_exposes_live_until(self) -> None:
        sensor = BINARY_SENSOR.CloudEdgeMeariAwakeSensor.__new__(
            BINARY_SENSOR.CloudEdgeMeariAwakeSensor
        )
        sensor._coordinator = self.coord
        self.assertEqual(sensor.extra_state_attributes, {"live_until": None})
        self.coord.wake_camera()
        self.coord._refresh_camera_awake()
        value = sensor.extra_state_attributes["live_until"]
        self.assertIsInstance(datetime.fromisoformat(value), datetime)


if __name__ == "__main__":
    unittest.main()
