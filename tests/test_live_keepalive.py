"""Offline checks that live keep-alive matches the official app's capture.

The ieGeek app (H.264 snap camera, LAN direct, 3 min 47 s of live) sends
START_LIVE once, a routed webrtc hello every ~21 s, a status query every
~120 s, and no HTTP wake while live.
"""

from __future__ import annotations

import importlib
import unittest

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
CODECS = importlib.import_module("custom_components.cloudplus.p2p_streamer.codecs")


class StartLiveKeepaliveTests(unittest.TestCase):
    def test_no_periodic_start_live_on_either_codec(self) -> None:
        for codec in (CODECS.CodecName.H264, CODECS.CodecName.HEVC):
            with self.subTest(codec=codec):
                policy = CODECS.runtime_policy_for(codec)
                self.assertEqual(policy.start_live_keepalive_s, 0.0)

    def test_reactive_start_live_retry_is_kept(self) -> None:
        policy = CODECS.runtime_policy_for(CODECS.CodecName.H264)
        self.assertGreater(policy.idle_start_live_retry_s, 0.0)


if __name__ == "__main__":
    unittest.main()
