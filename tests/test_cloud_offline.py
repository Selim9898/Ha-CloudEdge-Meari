"""Offline regressions: no idle wakes while the cloud says the camera is offline.

No threads beyond the test, no cloud, no camera.
"""

from __future__ import annotations

import importlib
import time
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator
OFFLINE = importlib.import_module("custom_components.cloudplus.coordinator.offline")


class _Loop:
    @staticmethod
    def is_closed() -> bool:
        return False

    @staticmethod
    def call_soon_threadsafe(cb, *args) -> None:
        cb(*args)


class _DeadWorker:
    @staticmethod
    def is_alive() -> bool:
        return False


def snap_coordinator() -> Any:
    hass = SimpleNamespace(loop=_Loop(), data={})
    device = {"deviceID": 1, "snNum": "synthetic-camera", "_category": "snap"}
    coord = COORDINATOR(hass, "user@example.invalid", "x", device)
    coord._running = True
    coord._snapshot_conversion_enabled = False
    coord._stop_streamer = Mock()
    coord._send_wake = Mock()
    coord._api = Mock()
    coord._api.get_device_status.return_value = "unknown"
    return coord


def session_reports(coord: Any, status: str) -> Mock:
    """A streamer start that ends at once with *status* from signaling."""

    def start(**_kwargs) -> None:
        coord._stream_thread = _DeadWorker()
        coord._last_p2p_diagnostics = {"device_status": status}

    coord._start_streamer_once = Mock(side_effect=start)
    return coord._start_streamer_once


class IdleGrabOfflineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.coord = snap_coordinator()

    def test_grab_stops_waking_once_signaling_says_offline(self) -> None:
        starts = session_reports(self.coord, "offline")
        began = time.monotonic()
        self.assertFalse(self.coord._run_initial_frame_grab())
        self.assertLess(time.monotonic() - began, 5.0)  # not the 45 s timeout
        self.assertEqual(starts.call_count, 1)
        self.assertEqual(self.coord._send_wake.call_count, 1)
        self.assertFalse(self.coord.camera_awake)

    def test_offline_grab_parks_the_idle_loop_without_the_retry_warning(self) -> None:
        session_reports(self.coord, "offline")
        self.coord._next_idle_grab = 0.0
        with self.assertLogs(OFFLINE._LOGGER, "WARNING") as logs:
            self.coord._maybe_grab_idle_frame(time.monotonic())
        self.assertTrue(self.coord.cloud_offline)
        self.assertIn("offline in the cloud", logs.output[0])
        wakes = self.coord._send_wake.call_count

        # The normal 20 s retry no longer applies: nothing runs until the probe.
        self.coord._start_streamer_once.reset_mock()
        now = time.monotonic()
        self.coord._next_offline_status_check = now + 3600
        self.coord._maybe_grab_idle_frame(now + 60.0)
        self.coord._maybe_grab_idle_frame(now + 200.0)
        self.coord._start_streamer_once.assert_not_called()
        self.assertEqual(self.coord._send_wake.call_count, wakes)

    def test_probe_sends_no_wake_and_backs_off_to_hours(self) -> None:
        session_reports(self.coord, "offline")
        self.coord._enter_cloud_offline(time.monotonic())
        self.coord._next_offline_status_check = time.monotonic() + 10**6
        backoffs = []
        for _ in range(8):
            self.coord._next_offline_probe = 0.0
            self.coord._maybe_grab_idle_frame(time.monotonic())
            backoffs.append(self.coord._offline_probe_s)
        self.coord._send_wake.assert_not_called()
        self.assertFalse(self.coord.camera_awake)
        self.assertEqual(backoffs[:3], [600.0, 1200.0, 2400.0])
        self.assertEqual(backoffs[-1], OFFLINE.OFFLINE_PROBE_MAX_S)
        self.assertGreaterEqual(OFFLINE.OFFLINE_PROBE_MAX_S, 3600.0)

    def test_network_failure_while_offline_stays_parked(self) -> None:
        session_reports(self.coord, "")
        self.coord._initial_grab_timeout = 0.5
        self.coord._enter_cloud_offline(time.monotonic())
        self.coord._next_offline_probe = 0.0
        self.coord._next_offline_status_check = time.monotonic() + 10**6
        self.coord._maybe_grab_idle_frame(time.monotonic())
        self.assertTrue(self.coord.cloud_offline)
        self.coord._send_wake.assert_not_called()

    def test_cloud_status_turning_online_probes_at_once(self) -> None:
        starts = session_reports(self.coord, "dormancy")
        self.coord._initial_grab_timeout = 0.5
        now = time.monotonic()
        self.coord._enter_cloud_offline(now)
        self.coord._next_offline_probe = now + 10**6
        api = self.coord._api
        api.get_device_status.return_value = "offline"
        self.coord._maybe_grab_idle_frame(now)
        starts.assert_not_called()

        api.get_device_status.return_value = "online"
        self.coord._maybe_grab_idle_frame(now + OFFLINE.OFFLINE_STATUS_CHECK_S)
        starts.assert_called()
        # Signaling saw the camera reachable: normal retries take over.
        self.assertFalse(self.coord.cloud_offline)

    def test_online_reading_without_an_offline_one_waits_for_the_backoff(self) -> None:
        starts = session_reports(self.coord, "offline")
        self.coord._enter_cloud_offline(time.monotonic())
        self.coord._api.get_device_status.return_value = "online"
        self.coord._maybe_grab_idle_frame(time.monotonic())
        starts.assert_not_called()

    def test_live_frame_from_a_user_wake_clears_offline(self) -> None:
        self.coord._enter_cloud_offline(time.monotonic())
        self.coord._remember_idle_frame(self.coord._video_codec, b"\x00\x00\x01keyframe")
        self.assertFalse(self.coord.cloud_offline)
        self.assertEqual(self.coord._offline_probe_s, OFFLINE.OFFLINE_PROBE_INITIAL_S)

    def test_quality_change_still_grabs_while_offline(self) -> None:
        starts = session_reports(self.coord, "offline")
        self.coord._enter_cloud_offline(time.monotonic())
        self.coord._idle_frame_refresh.set()
        self.coord._maybe_grab_idle_frame(time.monotonic())
        starts.assert_called_once()


if __name__ == "__main__":
    unittest.main()
