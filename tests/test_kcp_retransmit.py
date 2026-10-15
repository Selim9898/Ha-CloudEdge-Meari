"""Offline checks for KCP retransmission (min age, timer, never drop).

retransmit_unacked() fires on every ICE Binding response (about once a
second) and on handshake echoes. With 25 talk frames a second in flight,
resending everything each time would duplicate audio on the camera.
"""

from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
KCP = importlib.import_module("custom_components.cloudplus.kcp_tunnel")


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class RetransmitGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sent: list[bytes] = []
        self.clock = Clock()
        patcher = patch.object(KCP.time, "monotonic", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.kcp = KCP.KcpTunnel(self.sent.append)

    def resent_sns(self) -> list[int]:
        before = len(self.sent)
        self.kcp.retransmit_unacked()
        return [KCP.parse_kcp_segment(seg)["sn"] for seg in self.sent[before:]]

    def test_fresh_segments_are_not_resent(self) -> None:
        self.kcp.send_data(b"a" * 372)
        self.clock.now += KCP.KCP_RETRANSMIT_MIN_AGE_S / 2
        self.assertEqual(self.resent_sns(), [])

    def test_old_segments_are_resent_once_per_age_window(self) -> None:
        self.kcp.send_data(b"a")
        self.clock.now += KCP.KCP_RETRANSMIT_MIN_AGE_S
        self.assertEqual(self.resent_sns(), [0])
        self.assertEqual(self.resent_sns(), [])  # a second trigger right away
        self.clock.now += KCP.KCP_RETRANSMIT_MIN_AGE_S
        self.assertEqual(self.resent_sns(), [0])

    def test_burst_of_triggers_does_not_duplicate_audio(self) -> None:
        for _ in range(4):  # 160 ms of talk frames, a trigger after each
            self.kcp.send_data(b"f" * 372)
            self.clock.now += 0.04
            self.kcp.retransmit_unacked()
        self.assertEqual(len(self.sent), 4)  # no copies inside the age window

    def test_acked_segments_are_never_resent(self) -> None:
        self.kcp.send_data(b"a")
        self.kcp.send_data(b"b")
        self.kcp._mark_remote_una(1)
        self.clock.now += 1.0
        self.assertEqual(self.resent_sns(), [1])

    def test_unacked_segments_are_never_dropped(self) -> None:
        # Dropping one would stall the camera's in-order delivery forever.
        self.kcp.send_data(b"talk frame")
        self.clock.now += 60.0
        self.assertEqual(self.resent_sns(), [0])
        self.assertIn(0, self.kcp.sent_segments)

    def test_timer_resends_without_any_ice_reply(self) -> None:
        self.kcp.send_data(b"lost")
        self.kcp.resend_due()
        self.assertEqual(len(self.sent), 1)  # too young
        self.clock.now += KCP.KCP_RESEND_TIMEOUT_S
        self.kcp.resend_due()
        self.assertEqual(len(self.sent), 2)
        self.clock.now += KCP.KCP_RESEND_CHECK_S / 2
        self.kcp.resend_due()  # checked too recently
        self.assertEqual(len(self.sent), 2)
        self.kcp._mark_remote_una(1)
        self.clock.now += 5.0
        self.kcp.resend_due()
        self.assertEqual(len(self.sent), 2)  # ACKed: nothing left to resend

    def test_acked_set_stays_bounded(self) -> None:
        for _ in range(KCP.KCP_ACKED_SNS_KEEP + 100):
            self.kcp.send_data(b"x")
        self.kcp._mark_remote_una(self.kcp.sn_send)
        self.assertLessEqual(len(self.kcp.acked_sns), KCP.KCP_ACKED_SNS_KEEP)


if __name__ == "__main__":
    unittest.main()
