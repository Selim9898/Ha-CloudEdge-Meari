"""Offline tests for the non-blocking TURN relay probe."""

from __future__ import annotations

import importlib
import socket
import threading
import time
import unittest
from unittest.mock import call, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
RELAY_PROBE = importlib.import_module(
    "custom_components.cloudplus.p2p_streamer.relay_probe"
)
# Synthetic tag; no camera captures, credentials or device identifiers.
TAG = "synthetic01"
# Short timeouts keep the silent-relay case fast; the logic is unchanged.
FAST_TIMEOUT_S = 0.05
PROBE_FRAME = bytes.fromhex("e6a1b1c6d1e6100073796e746865746963300400000000000f9d")


class _FakeRelay:
    """Local TCP relay that records probe frames and optionally replies."""

    def __init__(self, *, replies: bool) -> None:
        self.replies = replies
        self.frames: list[bytes] = []
        self.connections = 0
        self._stop = threading.Event()
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(8)
        self._server.settimeout(FAST_TIMEOUT_S)
        self.port = self._server.getsockname()[1]
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.connections += 1
            self._serve(conn)

    def _serve(self, conn: socket.socket) -> None:
        with conn:
            conn.settimeout(FAST_TIMEOUT_S)
            pending = bytearray()
            while not self._stop.is_set():
                try:
                    data = conn.recv(64)
                except socket.timeout:
                    continue
                except OSError:
                    return
                if not data:
                    return
                pending.extend(data)
                while len(pending) >= len(PROBE_FRAME):
                    self.frames.append(bytes(pending[:len(PROBE_FRAME)]))
                    del pending[:len(PROBE_FRAME)]
                    if self.replies:
                        conn.sendall(bytes(18) + b"\x82" + bytes(5))

    def close(self) -> None:
        """Stop accepting and serving connections."""
        self._stop.set()
        self._server.close()
        self._thread.join(1)
        if self._thread.is_alive():
            raise RuntimeError("Fake relay did not stop")


class RelayProbeTests(unittest.TestCase):
    """The probe must keep its wire frames but never block live start."""

    def setUp(self):
        self._saved = (RELAY_PROBE._PROBE_PORT, RELAY_PROBE._PROBE_TIMEOUT_S)

    def tearDown(self):
        RELAY_PROBE._PROBE_PORT, RELAY_PROBE._PROBE_TIMEOUT_S = self._saved

    def _use_relay(self, relay: _FakeRelay, timeout_s: float | None = None) -> None:
        self.addCleanup(relay.close)
        RELAY_PROBE._PROBE_PORT = relay.port
        if timeout_s is not None:
            RELAY_PROBE._PROBE_TIMEOUT_S = timeout_s

    def test_silent_relay_does_not_block_start(self):
        """A relay that never answers must not delay the caller."""
        relay = _FakeRelay(replies=False)
        # Two hosts x four receive timeouts: the synchronous probe waited
        # 8 x timeout here (9.6 s with the production 1.2 s timeout).
        self._use_relay(relay, 0.2)
        results: list[bool] = []

        started = time.monotonic()
        thread = RELAY_PROBE.start_relay_probe(
            ["127.0.0.1", "localhost"], TAG, on_done=results.append
        )
        blocked = time.monotonic() - started
        still_probing = thread.is_alive()
        thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertLess(blocked, 0.1)
        self.assertTrue(still_probing)
        self.assertEqual(results, [False])

    def test_silent_relay_still_receives_native_frames(self):
        """Backgrounding keeps the exact probe frames the native SDK sends."""
        relay = _FakeRelay(replies=False)
        self._use_relay(relay, FAST_TIMEOUT_S)
        results: list[bool] = []

        thread = RELAY_PROBE.start_relay_probe(
            ["127.0.0.1"], TAG, on_done=results.append
        )
        thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [False])
        self.assertEqual(relay.frames, [PROBE_FRAME] * RELAY_PROBE._PROBE_COUNT)

    def test_answering_relay_reports_success(self):
        """A valid relay reply is still recognised."""
        relay = _FakeRelay(replies=True)
        self._use_relay(relay, FAST_TIMEOUT_S)
        results: list[bool] = []

        with self.assertLogs("custom_components.cloudplus.p2p_streamer.engine", level="DEBUG") as logs:
            thread = RELAY_PROBE.start_relay_probe(
                ["127.0.0.1"], TAG, on_done=results.append
            )
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [True])
        self.assertIn("TURN relay probe ok for", logs.output[-1])
        self.assertIn("background", logs.output[-1])

    def test_no_hosts_starts_nothing(self):
        """Missing relay hosts are skipped without spawning a thread."""
        self.assertIsNone(RELAY_PROBE.start_relay_probe(["", "  "], TAG))

    def test_hosts_stop_at_first_success(self):
        """Sequential probing stops once one host answers."""
        relay = _FakeRelay(replies=True)
        self._use_relay(relay, FAST_TIMEOUT_S)

        self.assertTrue(
            RELAY_PROBE.probe_relay_hosts(["127.0.0.1", "localhost"], TAG)
        )
        self.assertEqual(relay.connections, 1)

    def test_failed_thread_start_does_not_abort_media(self):
        """An optional probe cannot prevent streaming if threads are exhausted."""
        with patch.object(threading.Thread, "start", side_effect=RuntimeError("no threads")):
            with self.assertLogs("custom_components.cloudplus.p2p_streamer.engine", level="WARNING"):
                self.assertIsNone(RELAY_PROBE.start_relay_probe(["127.0.0.1"], TAG))

    def test_hosts_continue_after_failure(self):
        """A failed probe still tries the next host in order."""
        with patch.object(RELAY_PROBE, "probe_relay", side_effect=[False, True]) as probe:
            self.assertTrue(RELAY_PROBE.probe_relay_hosts(["first", "second"], TAG))
        self.assertEqual(probe.call_args_list, [call("first", TAG), call("second", TAG)])


if __name__ == "__main__":
    unittest.main()
