"""Offline tests for the non-blocking TURN relay probe."""

from __future__ import annotations

import importlib
import socket
import threading
import time
import unittest

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
RELAY_PROBE = importlib.import_module(
    "custom_components.cloudplus.p2p_streamer.relay_probe"
)
# Synthetic tag; no camera captures, credentials or device identifiers.
TAG = "synthetic01"
# Short timeouts keep the silent-relay case fast; the logic is unchanged.
FAST_TIMEOUT_S = 0.05


class _FakeRelay:
    """Local TCP relay that records probe frames and optionally replies."""

    def __init__(self, *, replies: bool) -> None:
        self.replies = replies
        self.frames: list[bytes] = []
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(8)
        self.port = self._server.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        with conn:
            while True:
                try:
                    data = conn.recv(64)
                except OSError:
                    return
                if not data:
                    return
                self.frames.append(data)
                if self.replies:
                    conn.sendall(bytes(18) + b"\x82" + bytes(5))

    def close(self) -> None:
        """Stop accepting connections."""
        self._server.close()


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

        self.assertEqual(results, [False])
        self.assertTrue(relay.frames)
        expected = RELAY_PROBE._frame(TAG)
        self.assertTrue(all(frame == expected for frame in relay.frames))

    def test_answering_relay_reports_success(self):
        """A valid relay reply is still recognised."""
        relay = _FakeRelay(replies=True)
        self._use_relay(relay, FAST_TIMEOUT_S)
        results: list[bool] = []

        thread = RELAY_PROBE.start_relay_probe(
            ["127.0.0.1"], TAG, on_done=results.append
        )
        thread.join(5)

        self.assertEqual(results, [True])

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


if __name__ == "__main__":
    unittest.main()
