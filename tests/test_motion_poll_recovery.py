"""Offline regressions: the motion poller keeps retrying until the cloud answers.

No threads beyond the test, no cloud, no camera.
"""

from __future__ import annotations

import importlib
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
MOTION = importlib.import_module("custom_components.cloudplus.coordinator.motion")


class _ScriptedStop:
    """Stands in for the poll stop event: records waits, advances a clock."""

    def __init__(self, clock: list[float], rounds: int) -> None:
        self.clock = clock
        self.rounds = rounds
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return len(self.waits) >= self.rounds

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.clock[0] += seconds


class MotionPollRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.api = Mock()
        self.api.get_new_device_events.return_value = []
        self.listener = MOTION.MotionEventListener(self.api)
        self.received = Mock()
        self.listener.register(42, "synthetic-camera", self.received)
        self.clock = [1000.0]
        fake_time = SimpleNamespace(
            monotonic=lambda: self.clock[0], strftime=time.strftime
        )
        patcher = patch.object(MOTION, "time", fake_time)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_loop(self, rounds: int) -> _ScriptedStop:
        stop = _ScriptedStop(self.clock, rounds)
        self.listener._stop_poll = stop
        self.listener._alarm_poll_loop()
        return stop

    def test_unexpected_errors_back_off_and_never_end_the_poller(self) -> None:
        self.api.get_device_events.side_effect = TypeError("degraded reply")
        self.api.get_new_device_events.side_effect = OSError("no route")
        self.api.login.side_effect = AttributeError("'list' has no 'get'")
        with self.assertLogs(MOTION._LOGGER, "WARNING") as logs:
            stop = self.run_loop(7)
        self.assertEqual(stop.waits, [60.0, 120.0, 240.0, 480.0, 900.0, 900.0, 900.0])
        self.assertEqual(self.api.login.call_count, 7)
        # One warning for the poll and one for the login, not one per attempt.
        self.assertEqual(len(logs.output), 2)

    def test_recovery_is_logged_once_and_skips_the_outage_backlog(self) -> None:
        event = {"deviceID": "42", "eventType": 2, "msgID": "during-outage"}
        responses = iter(
            [OSError("down")] * 3 + [[event]] + [[event, dict(event, msgID="fresh")]] * 3
        )

        def events(_device, _day):
            item = next(responses)
            if isinstance(item, Exception):
                raise item
            return item

        self.api.get_device_events.side_effect = events
        self.api.get_new_device_events.side_effect = OSError("down")
        self.api.login.side_effect = OSError("down")
        with self.assertLogs(MOTION._LOGGER, "INFO") as logs:
            stop = self.run_loop(6)
        recovered = [line for line in logs.output if "recovered" in line]
        self.assertEqual(len(recovered), 1)
        self.assertIn("after 3 failed attempts", recovered[0])
        self.assertEqual(stop.waits[3:], [MOTION.ALARM_POLL_INTERVAL] * 3)
        # The backlog is seeded, the next new event fires once.
        self.received.assert_called_once_with("Motion", None)

    def test_fresh_login_retries_at_the_normal_pace_once(self) -> None:
        self.api.get_device_events.side_effect = [OSError("expired"), []]
        self.api.get_new_device_events.side_effect = OSError("expired")
        stop = self.run_loop(1)
        self.assertEqual(stop.waits, [MOTION.ALARM_POLL_INTERVAL])
        self.api.login.assert_called_once()


if __name__ == "__main__":
    unittest.main()
