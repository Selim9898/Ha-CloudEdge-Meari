"""Talk and keep-alive checks against bytes from the official-app capture.

Fixtures in tests/fixtures/talk/ come from a capture of the ieGeek app
(6.2.3) watching an H.264 snap camera for 3 min 47 s and talking twice,
KCP over UDP, LAN direct. They are sanitized: the 32-byte VVP auth MD5 is
zeroed and no tokens, keys or account ids are included.
"""

from __future__ import annotations

import importlib
import json
import struct
import unittest
from pathlib import Path

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
CODEC = importlib.import_module("custom_components.cloudplus.p2p_streamer.codec")
PROTOCOL = importlib.import_module("custom_components.cloudplus.p2p_streamer.protocol")
KCP = importlib.import_module("custom_components.cloudplus.kcp_tunnel")

FIXTURES = Path(__file__).parent / "fixtures" / "talk"
FRAME_LEN = 0x34 + PROTOCOL.TALK_FRAME_BYTES


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def frames(name: str) -> list[bytes]:
    blob = fixture(name)
    return [blob[i : i + FRAME_LEN] for i in range(0, len(blob), FRAME_LEN)]


class TalkUplinkFrameTests(unittest.TestCase):
    def test_builder_reproduces_the_app_frames_byte_for_byte(self) -> None:
        wire_frames = frames("talk_uplink_frames_5x372.bin")
        self.assertEqual(len(wire_frames), 5)
        for index, wire in enumerate(wire_frames):
            plain = bytes(CODEC.decrypt_stream_frame(bytearray(wire)))
            parsed = CODEC.parse_stream_frame(plain)
            self.assertEqual(parsed.frame_type, PROTOCOL.STREAM_TYPE_AUDIO)
            self.assertEqual(parsed.header_size, 0x34)
            self.assertEqual(len(parsed.payload), PROTOCOL.TALK_FRAME_BYTES)
            self.assertEqual(parsed.timestamp_ms, 0)
            seq = struct.unpack_from("<I", plain, 0x08)[0]
            self.assertEqual(seq, 409 + index)  # app-wide counter
            self.assertEqual(CODEC.build_talk_audio_frame(parsed.payload, seq), wire)

    def test_encryption_covers_whole_blocks_from_0x28(self) -> None:
        wire = frames("talk_uplink_frames_5x372.bin")[0]
        plain = bytes(CODEC.decrypt_stream_frame(bytearray(wire)))
        self.assertEqual(plain[:0x28], wire[:0x28])  # header before 0x28 is plain
        self.assertEqual(plain[-4:], wire[-4:])  # trailing partial block is plain
        self.assertNotEqual(plain[0x28:0x30], wire[0x28:0x30])
        self.assertEqual(bytes(CODEC.encrypt_stream_frame(bytearray(plain))), wire)

    def test_silence_frame_is_all_ff(self) -> None:
        wire = frames("talk_uplink_frames_5x372.bin")[1]
        plain = bytes(CODEC.decrypt_stream_frame(bytearray(wire)))
        self.assertEqual(plain[0x34:], b"\xff" * 320)
        self.assertEqual(CODEC.pcm16_to_ulaw([0] * 320), b"\xff" * 320)

    def test_camera_downlink_audio_uses_the_same_layout(self) -> None:
        for wire in frames("camera_audio_frames_2x372.bin"):
            parsed = CODEC.parse_stream_frame(bytes(CODEC.decrypt_stream_frame(bytearray(wire))))
            self.assertEqual(parsed.header_size, 0x34)
            self.assertEqual(len(parsed.payload), 320)

    def test_datagram_carries_three_raw_kcp_segments(self) -> None:
        segs = KCP.parse_kcp_segments(fixture("talk_udp_datagram_3seg.bin"))
        self.assertEqual([s["cmd"] for s in segs], [KCP.KCP_CMD_PUSH] * 3)
        self.assertEqual([s["sn"] for s in segs], [92, 93, 94])
        for seg in segs:
            self.assertEqual(seg["frg"], 0)
            self.assertEqual(seg["len"], FRAME_LEN)
            # Talk audio is NOT wrapped in IVA; the KCP payload is the frame.
            self.assertEqual(seg["data"][:4], b"\x00\x00\x01\xfa")
        self.assertLessEqual(FRAME_LEN, KCP.KCP_MSS)  # one segment per frame


class UlawEncoderTests(unittest.TestCase):
    def test_reference_points(self) -> None:
        self.assertEqual(CODEC.pcm16_to_ulaw([0, 32767, -32768, 1000, -1000]),
                         bytes([0xFF, 0x80, 0x00, 0xCE, 0x4E]))

    def test_sign_symmetry(self) -> None:
        for sample in (8, 100, 4000, 20000):
            positive = CODEC.pcm16_to_ulaw([sample])[0]
            negative = CODEC.pcm16_to_ulaw([-sample])[0]
            self.assertEqual(positive ^ negative, 0x80)


class TalkControlTests(unittest.TestCase):
    def vvp(self, name: str) -> bytes:
        parsed = KCP.parse_iva_frame(fixture(name))
        self.assertIsNotNone(parsed)
        marker, _, _, vvp = parsed
        self.assertEqual(marker, KCP.IVA_TYPE_DATA)
        self.assertEqual(len(vvp), PROTOCOL.VVP_HEADER_SIZE)
        return vvp

    def assert_matches_builder(self, vvp: bytes, cmd: int) -> None:
        seq = struct.unpack_from(">I", vvp, 0x08)[0]
        built = PROTOCOL.build_vvp_packet(
            cmd, seq, "0" * 48, param=8, channel=0, stream_id=0, stream_flag=0
        )
        # Everything except the 32-byte auth md5 (zeroed in the fixture).
        self.assertEqual(built[:0x10], vvp[:0x10])
        self.assertEqual(built[0x30:], vvp[0x30:])

    def test_open_close_and_heartbeat_layout(self) -> None:
        self.assertEqual((PROTOCOL.VVP_CMD_TALK_OPEN, PROTOCOL.VVP_CMD_TALK_CLOSE),
                         (0x1350, 0x1351))
        self.assert_matches_builder(self.vvp("vvp_1350_talk_open.bin"), PROTOCOL.VVP_CMD_TALK_OPEN)
        self.assert_matches_builder(self.vvp("vvp_1351_talk_close.bin"), PROTOCOL.VVP_CMD_TALK_CLOSE)
        self.assert_matches_builder(self.vvp("vvp_888e_heartbeat.bin"), PROTOCOL.VVP_CMD_HEARTBEAT)

    def test_start_live_uses_stream_id_100_and_flag_0(self) -> None:
        vvp = self.vvp("vvp_11ff_start_live.bin")
        self.assertEqual(struct.unpack_from(">I", vvp, 0x0C)[0], PROTOCOL.VVP_CMD_START_LIVE)
        self.assertEqual((vvp[0x38], vvp[0x39]), (100, 0))

    def test_iva_heartbeat_is_echoed(self) -> None:
        sent = KCP.parse_iva_frame(fixture("iva_7012_heartbeat.bin"))
        echo = KCP.parse_iva_frame(fixture("iva_7012_camera_echo.bin"))
        self.assertEqual(sent[0], KCP.IVA_TYPE_HANDSHAKE)
        self.assertEqual(sent[1:3], echo[1:3])


class AppKeepaliveScheduleTests(unittest.TestCase):
    def test_cadence(self) -> None:
        schedule = json.loads(fixture("app_control_schedule.json"))
        iva = [e["t"] for e in schedule if e["msg"] == "IVA 0x7012"]
        hb = [e["t"] for e in schedule if e["msg"] == "VVP 0x888e"]
        starts = [e for e in schedule if e["msg"] == "VVP 0x11ff"]
        gaps = [b - a for a, b in zip(iva[1:], iva[2:])]
        self.assertTrue(all(3.0 <= g <= 3.25 for g in gaps))
        hb_gaps = [b - a for a, b in zip(hb, hb[1:]) if b - a < 11]
        self.assertTrue(all(9.9 <= g <= 10.2 for g in hb_gaps))
        # The app never re-sends START_LIVE as a keep-alive (only the quality switch).
        self.assertEqual(len(starts), 2)
        self.assertLess(max(e["t"] for e in starts), 11.0)
        self.assertGreater(max(iva), 200.0)

    def test_talk_frames_follow_open_and_precede_close(self) -> None:
        schedule = json.loads(fixture("app_control_schedule.json"))
        opened = next(e["t"] for e in schedule if e["msg"] == "VVP 0x1350")
        closed = next(e["t"] for e in schedule if e["msg"] == "VVP 0x1351")
        audio = [e["t"] for e in schedule if e["msg"] == "talk audio"]
        self.assertTrue(all(opened < t < closed for t in audio))


if __name__ == "__main__":
    unittest.main()
