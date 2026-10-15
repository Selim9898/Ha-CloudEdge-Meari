"""Offline checks for the talk queue, pacing, auto-stop and coordinator flow."""

from __future__ import annotations

import asyncio
import importlib
import struct
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator
TALK = importlib.import_module("custom_components.cloudplus.p2p_streamer.talk")
CODEC = importlib.import_module("custom_components.cloudplus.p2p_streamer.codec")
COORD_TALK = importlib.import_module("custom_components.cloudplus.coordinator.talk")

FRAME = TALK.TALK_FRAME_BYTES
INTERVAL = TALK.TALK_FRAME_INTERVAL_S


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Wire:
    """Records what the live loop would put on the wire, with timestamps."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.events: list[tuple[str, float, bytes]] = []

    def open(self) -> None:
        self.events.append(("open", self.clock.now, b""))

    def close(self) -> None:
        self.events.append(("close", self.clock.now, b""))

    def frame(self, data: bytes) -> None:
        self.events.append(("frame", self.clock.now, data))

    def kinds(self) -> list[str]:
        return [kind for kind, _, _ in self.events]

    def frame_times(self) -> list[float]:
        return [t for kind, t, _ in self.events if kind == "frame"]

    def seqs(self) -> list[int]:
        out = []
        for kind, _, data in self.events:
            if kind == "frame":
                plain = bytes(CODEC.decrypt_stream_frame(bytearray(data)))
                out.append(struct.unpack_from("<I", plain, 0x08)[0])
        return out


class TalkChannelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.talk = TALK.TalkChannel(clock=self.clock)
        self.wire = Wire(self.clock)

    def drain(self) -> int:
        return self.talk.drain(self.wire.open, self.wire.close, self.wire.frame)

    def run_loop(self, seconds: float, tick: float = 0.01) -> None:
        end = self.clock.now + seconds
        while self.clock.now < end:
            self.drain()
            self.clock.now = round(self.clock.now + tick, 6)

    def test_open_then_frames_paced_at_40_ms(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 25)  # 1 s of audio pushed at once
        self.run_loop(2.0)
        kinds = self.wire.kinds()
        self.assertEqual(kinds[0], "open")
        self.assertEqual(kinds.count("frame"), 25)
        times = self.wire.frame_times()
        self.assertGreaterEqual(times[0] - self.wire.events[0][1], TALK.TALK_OPEN_LEAD_S - 1e-9)
        gaps = [b - a for a, b in zip(times, times[1:])]
        self.assertTrue(all(gap >= INTERVAL - 0.011 for gap in gaps), gaps)
        self.assertAlmostEqual(times[-1] - times[0], 24 * INTERVAL, delta=0.02)

    def test_a_late_loop_resyncs_instead_of_bursting(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 50)
        self.drain()  # open
        self.clock.now += 1.0  # the loop stalled for a second
        self.assertLessEqual(self.drain(), TALK.TALK_MAX_FRAMES_PER_DRAIN)

    def test_sequence_continues_across_talk_sessions(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 3)
        self.drain()  # open on the wire
        self.talk.stop()
        self.run_loop(1.0)
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 2)
        self.drain()
        self.talk.stop()
        self.run_loop(1.0)
        self.assertEqual(self.wire.seqs(), [0, 1, 2, 3, 4])
        self.assertEqual(self.wire.kinds().count("open"), 2)
        self.assertEqual(self.wire.kinds().count("close"), 2)

    def test_stop_after_open_plays_due_audio_then_closes_once(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 4)
        self.drain()  # open
        self.talk.stop()
        self.run_loop(1.0)
        self.assertEqual(self.wire.kinds(), ["open"] + ["frame"] * 4 + ["close"])

    def test_stop_before_open_clears_the_queue(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 4)
        self.talk.stop()
        self.run_loop(1.0)
        self.assertEqual(self.wire.kinds(), [])

    def test_leftover_audio_never_reopens_talk_later(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 50)
        self.run_loop(0.4)  # open + a few frames
        self.talk.session_ended(self.wire.close)  # camera went away mid-talk
        self.talk.stop()
        self.clock.now += 3600  # an hour later a motion wake starts live
        before = len(self.wire.events)
        self.run_loop(1.0)
        self.assertEqual(self.wire.events[before:], [])
        self.assertEqual(self.talk.queued_s, 0.0)

    def test_stale_audio_is_dropped_before_it_is_sent(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 10)  # due within 0.4 s
        self.clock.now += 1.5  # the live loop is not running (reconnecting)
        self.talk.push(b"\x00" * FRAME * 5)  # fresh audio, keeps talk alive
        self.clock.now += 1.0  # first batch is now > 2 s past due
        self.run_loop(1.0)
        self.assertEqual(self.wire.kinds().count("frame"), 5)

    def test_queue_full_reports_dropped_frames(self) -> None:
        self.talk.begin()
        result = self.talk.push(b"\x00" * FRAME * (TALK.TALK_QUEUE_MAX_FRAMES + 7))
        self.assertEqual(result, (TALK.TALK_QUEUE_MAX_FRAMES, 7))
        self.assertEqual(self.talk.push(b"\x00" * FRAME), (0, 1))

    def test_idle_auto_stop_after_2_s_without_audio(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 2)
        self.run_loop(1.0)
        self.assertTrue(self.talk.active)
        self.run_loop(TALK.TALK_IDLE_TIMEOUT_S + 0.1)
        self.assertFalse(self.talk.active)
        self.assertEqual(self.talk.stop_reason, "idle")
        self.assertEqual(self.wire.kinds()[-1], "close")

    def test_hard_max_of_60_s(self) -> None:
        self.talk.begin()
        for _ in range(int(TALK.TALK_MAX_S) + 2):
            if not self.talk.active:
                break
            self.talk.push(b"\x00" * FRAME * 25)
            self.run_loop(1.0, tick=0.02)
        self.assertFalse(self.talk.active)
        self.assertEqual(self.talk.stop_reason, "max duration")
        self.assertLessEqual(len(self.wire.frame_times()), TALK.TALK_MAX_S / INTERVAL + 1)
        self.run_loop(0.1)
        self.assertEqual(self.wire.kinds()[-1], "close")
        with self.assertRaises(RuntimeError):
            self.talk.push(b"\x00" * FRAME)

    def test_push_pads_partial_frames_with_silence(self) -> None:
        self.talk.begin()
        self.assertEqual(self.talk.push(b"\x00" * (FRAME + 10)), (2, 0))
        self.drain()
        self.talk.stop()
        self.run_loop(1.0)
        last = [d for k, _, d in self.wire.events if k == "frame"][-1]
        plain = bytes(CODEC.decrypt_stream_frame(bytearray(last)))
        self.assertEqual(plain[0x34 + 10:], b"\xff" * (FRAME - 10))

    def test_session_end_closes_and_the_next_session_reopens(self) -> None:
        self.talk.begin()
        self.talk.push(b"\x00" * FRAME * 50)
        self.run_loop(0.5)
        self.talk.session_ended(self.wire.close)
        self.talk.session_ended(self.wire.close)  # only once
        self.assertEqual(self.wire.kinds().count("close"), 1)
        self.run_loop(0.1)
        self.assertEqual(self.wire.kinds().count("open"), 2)

    def test_tone_is_whole_frames(self) -> None:
        tone = TALK.tone_ulaw(800, 2.0, 0.5)
        self.assertEqual(len(tone), 2 * TALK.TALK_SAMPLE_RATE)
        self.assertEqual(len(tone) % FRAME, 0)
        self.assertNotEqual(set(tone), {0xFF})
        self.assertEqual(len(TALK.tone_ulaw(800, 0.01)), FRAME)


class _Loop:
    @staticmethod
    def is_closed() -> bool:
        return False

    @staticmethod
    def call_soon_threadsafe(cb, *args) -> None:
        cb(*args)


def coordinator(device: dict | None = None) -> object:
    hass = SimpleNamespace(loop=_Loop(), data={})
    base = {"deviceID": 1, "snNum": "synthetic-camera", "_category": "snap"}
    coord = COORDINATOR(hass, "user@example.invalid", "x", {**base, **(device or {})})
    coord._running = True
    coord._send_wake = Mock()
    return coord


class CoordinatorTalkTests(unittest.IsolatedAsyncioTestCase):
    async def test_wakes_and_waits_for_live_before_opening(self) -> None:
        coord = coordinator()
        coord.wake_camera = Mock()
        asyncio.get_running_loop().call_later(0.3, setattr, coord, "_camera_awake", True)
        await coord.async_talk_start(timeout_s=5)
        coord.wake_camera.assert_called_once_with()
        self.assertTrue(coord.talk_active)

    async def test_no_wake_when_already_live(self) -> None:
        coord = coordinator()
        coord._camera_awake = True
        coord.wake_camera = Mock()
        await coord.async_talk_start()
        coord.wake_camera.assert_not_called()
        self.assertGreater(coord._live_deadline, time.monotonic())

    async def test_gives_up_if_the_camera_never_goes_live(self) -> None:
        coord = coordinator()
        coord.wake_camera = Mock()
        with self.assertRaises(COORD_TALK.TalkError):
            await coord.async_talk_start(timeout_s=0.3)
        self.assertFalse(coord.talk_active)

    async def test_ppcs_and_vtk_0_are_refused(self) -> None:
        for device in ({"deviceP2P": "ppcs"}, {"capability": '{"caps": {"vtk": 0}}'}):
            with self.subTest(device=device):
                coord = coordinator(device)
                self.assertFalse(coord.supports_talk)
                with self.assertRaises(COORD_TALK.TalkError):
                    await coord.async_talk_start()

    def test_unknown_or_talk_capable_vtk_is_allowed(self) -> None:
        for device in ({}, {"capability": '{"caps": {"vtk": 4}}'}):
            with self.subTest(device=device):
                self.assertTrue(coordinator(device).supports_talk)

    async def test_audio_keeps_live_open_and_is_charged_to_the_budget(self) -> None:
        coord = coordinator()
        coord._camera_awake = True
        await coord.async_talk_start()
        used_before = coord._watch.used(time.monotonic())
        coord.talk_push(b"\x00" * FRAME * 125)  # 5 s of audio
        now = time.monotonic()
        self.assertGreaterEqual(coord._live_deadline, now + 5.0)
        self.assertGreater(coord._watch.used(now), used_before + 4.0)

    def test_push_without_a_session_is_an_error(self) -> None:
        with self.assertRaises(COORD_TALK.TalkError):
            coordinator().talk_push(b"\x00" * FRAME)

    async def test_stop_on_coordinator_stop(self) -> None:
        coord = coordinator()
        coord._camera_awake = True
        await coord.async_talk_start()
        coord._talk.stop("coordinator stopped")
        self.assertFalse(coord.talk_active)


class TalkLimitTests(unittest.IsolatedAsyncioTestCase):
    async def live(self) -> object:
        coord = coordinator()
        coord._camera_awake = True
        coord._set_session_active(True)  # a live session (seq 1)
        return coord

    async def test_refused_when_the_watch_budget_is_used_up(self) -> None:
        coord = await self.live()
        now = time.monotonic()
        coord._watch.grant(now - coord._watch_budget_s(), now)
        with self.assertRaises(COORD_TALK.TalkError) as ctx:
            await coord.async_talk_start()
        self.assertIn("budget", str(ctx.exception))
        self.assertFalse(coord.talk_active)

    async def test_refused_after_the_hourly_talk_allowance(self) -> None:
        coord = await self.live()
        coord._watch.add_talk(time.monotonic(), COORD_TALK.TALK_HOURLY_S)
        with self.assertRaises(COORD_TALK.TalkError):
            await coord.async_talk_start()

    async def test_push_stops_talk_at_the_hourly_allowance(self) -> None:
        coord = await self.live()
        await coord.async_talk_start()
        coord._watch.add_talk(time.monotonic(), COORD_TALK.TALK_HOURLY_S - 0.1)
        with self.assertRaises(COORD_TALK.TalkError):
            coord.talk_push(b"\x00" * FRAME * 25)
        self.assertFalse(coord.talk_active)

    async def test_sent_audio_is_charged_to_the_talk_allowance(self) -> None:
        coord = await self.live()
        await coord.async_talk_start()
        coord.talk_push(b"\x00" * FRAME * 3)
        wire = Wire(Clock())
        coord._talk._next_send = 0.0
        coord._talk.drain(wire.open, wire.close, wire.frame)
        time.sleep(TALK.TALK_OPEN_LEAD_S + 0.15)
        coord._talk.drain(wire.open, wire.close, wire.frame)
        self.assertAlmostEqual(
            coord._watch.talk_used(time.monotonic()), 3 * INTERVAL, places=6
        )

    async def test_restart_in_the_same_live_view_keeps_the_60_s_window(self) -> None:
        coord = await self.live()
        await coord.async_talk_start()
        first_start = coord._talk.started_at
        coord.talk_stop()
        await asyncio.sleep(0.05)
        await coord.async_talk_start()
        self.assertEqual(coord._talk.started_at, first_start)
        coord.talk_stop()
        # The window is used up: a third start in the same live view is refused.
        coord._talk_window = (coord._live_session_seq, time.monotonic() - TALK.TALK_MAX_S)
        with self.assertRaises(COORD_TALK.TalkError):
            await coord.async_talk_start()
        # A new live session gets a fresh window.
        coord._set_session_active(False)
        coord._set_session_active(True)
        coord._camera_awake = True
        await coord.async_talk_start()
        self.assertGreater(coord._talk.started_at, first_start)

    async def test_wait_for_live_ends_promptly_on_unload(self) -> None:
        coord = coordinator()
        coord.wake_camera = Mock()
        asyncio.get_running_loop().call_later(0.2, setattr, coord, "_running", False)
        started = time.monotonic()
        with self.assertRaises(COORD_TALK.TalkError) as ctx:
            await coord.async_talk_start(timeout_s=30)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertIn("stopped", str(ctx.exception))
        self.assertFalse(coord.talk_active)


if __name__ == "__main__":
    unittest.main()
