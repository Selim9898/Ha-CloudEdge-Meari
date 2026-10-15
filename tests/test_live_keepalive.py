"""Offline checks that live keep-alive matches the official app's capture.

The ieGeek app (H.264 snap camera, LAN direct, 3 min 47 s of live) sends
START_LIVE once, a routed webrtc hello every ~21 s, a status query every
~120 s, and no HTTP wake while live.
"""

from __future__ import annotations

import base64
import importlib
import inspect
import json
import unittest

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
CODECS = importlib.import_module("custom_components.cloudplus.p2p_streamer.codecs")
KEEPALIVE = importlib.import_module("custom_components.cloudplus.p2p_streamer.keepalive")
LIVE_SESSION = importlib.import_module("custom_components.cloudplus.p2p_streamer.live_session")
ENGINE = importlib.import_module("custom_components.cloudplus.p2p_streamer.engine")
SIGNALING = importlib.import_module("custom_components.cloudplus.meari_signaling")
MSGSVR = importlib.import_module("custom_components.cloudplus.msgsvr_codec")


class RecordingSig:
    """MsgSvrClient stand-in that records which keep-alive was sent when."""

    def __init__(self, clock) -> None:
        self.clock = clock
        self.sent: list[tuple[str, float]] = []

    def send_heartbeat(self, *, read_response: bool = False) -> None:
        self.sent.append(("heartbeat", self.clock[0]))

    def send_webrtc_keepalive(self, *, read_response: bool = False) -> None:
        self.sent.append(("hello", self.clock[0]))

    def send_status_keepalive(self, device_uuid, *, read_response: bool = False) -> None:
        assert device_uuid == "device-uuid"
        self.sent.append(("status", self.clock[0]))


def run_session(seconds: float, step: float = 0.5) -> list[tuple[str, float]]:
    clock = [0.0]
    sig = RecordingSig(clock)
    keepalive = KEEPALIVE.SignalingKeepalive(
        sig, "device-uuid", heartbeat_s=30.0, start=0.0,
        spawn=lambda target, _name: target(),
    )
    while clock[0] <= seconds:
        keepalive.tick(clock[0])
        clock[0] = round(clock[0] + step, 3)
    return sig.sent


class FakeSocket:
    def __init__(self) -> None:
        self.frames: list[bytes] = []

    def sendall(self, data: bytes) -> None:
        self.frames.append(data)


class StartLiveKeepaliveTests(unittest.TestCase):
    def test_kcp_live_loop_never_resends_start_live_as_keepalive(self) -> None:
        source = inspect.getsource(LIVE_SESSION.LiveSessionMixin._stream_with_turn)
        self.assertFalse("start_live_keepalive_s" in source)
        self.assertFalse('"keepalive"' in source)

    def test_ppcs_keeps_its_30_s_resend_on_h264(self) -> None:
        policy = CODECS.runtime_policy_for(CODECS.CodecName.H264)
        self.assertEqual(policy.start_live_keepalive_s, 30.0)
        self.assertIn("start_live_keepalive_s", inspect.getsource(ENGINE.P2PStreamer))

    def test_reactive_start_live_retry_is_kept(self) -> None:
        policy = CODECS.runtime_policy_for(CODECS.CodecName.H264)
        self.assertGreater(policy.idle_start_live_retry_s, 0.0)


class SignalingKeepaliveTests(unittest.TestCase):
    def times(self, label: str, seconds: float = 245.0) -> list[float]:
        return [t for name, t in run_session(seconds) if name == label]

    def test_webrtc_hello_every_21_s_for_the_whole_session(self) -> None:
        self.assertEqual(self.times("hello"), [21.0 * n for n in range(1, 12)])

    def test_status_query_every_120_s(self) -> None:
        self.assertEqual(self.times("status"), [120.0, 240.0])

    def test_register_heartbeat_keeps_its_cadence(self) -> None:
        beats = self.times("heartbeat")
        self.assertEqual(beats[0], 20.0)
        self.assertTrue(all(b - a == 30.0 for a, b in zip(beats, beats[1:])))

    def test_one_worker_at_a_time(self) -> None:
        spawned = []
        keepalive = KEEPALIVE.SignalingKeepalive(
            RecordingSig([0.0]), "device-uuid", heartbeat_s=30.0, start=0.0,
            spawn=lambda target, _name: spawned.append(target),
        )
        self.assertEqual(keepalive.tick(21.0), ["heartbeat", "hello"])
        self.assertEqual(keepalive.tick(42.0), [])  # previous worker still busy
        spawned[0]()
        self.assertEqual(keepalive.tick(42.0), ["hello"])


class SignalingFramesTests(unittest.TestCase):
    def client(self) -> tuple[object, FakeSocket]:
        sig = SIGNALING.MsgSvrClient("127.0.0.1", 1)
        sig.sock = FakeSocket()
        sig.uuid = "client-uuid"
        sig.token = "token"
        sig.webrtcsvr = {"ip": "192.0.2.1", "port": 1, "domain": "webrtcsvr.eu", "tag": "t"}
        return sig, sig.sock

    def test_hello_is_routed_to_webrtcsvr_like_the_app(self) -> None:
        sig, sock = self.client()
        sig.send_webrtc_keepalive()
        frame = MSGSVR.parse_frame(sock.frames[0])
        self.assertEqual((frame["method"], frame["cmd"]), (MSGSVR.METHOD_ROUTED, MSGSVR.CMD_WEBRTC))
        body = frame["json"]
        self.assertEqual(list(body), ["from", "to", "content"])
        self.assertEqual(body["to"]["node"], "webrtcsvr")
        self.assertEqual(json.loads(base64.b64decode(body["content"])), {"cmd": "hello"})

    def test_status_query_matches_the_app(self) -> None:
        sig, sock = self.client()
        sig.send_status_keepalive("device-uuid")
        frame = MSGSVR.parse_frame(sock.frames[0])
        self.assertEqual((frame["method"], frame["cmd"]), (MSGSVR.METHOD_DIRECT, MSGSVR.CMD_STATUS))
        self.assertEqual(list(frame["json"]), ["action", "uuid", "sid"])
        self.assertEqual(frame["json"]["action"], "status")


if __name__ == "__main__":
    unittest.main()
