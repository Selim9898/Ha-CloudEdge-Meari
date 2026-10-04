"""TURN-side relay probe used by the native SDK before media starts."""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Callable, Sequence

_LOGGER = logging.getLogger(__name__)

_MAGIC = 0xE6
_TAIL = 0x9D
_PROBE_PORT = 9371
_PROBE_TIMEOUT_S = 1.2
_PROBE_COUNT = 4


def _frame(tag: str, code: int = 0x04) -> bytes:
    payload = tag.encode("ascii", errors="ignore")[:10].ljust(10, b"\x00")
    payload += bytes((code, 0, 0, 0, 0, 0))
    header = bytes((_MAGIC, 0xA1, 0xB1, 0xC6, 0xD1, _MAGIC, len(payload), 0))
    return header + payload + bytes((sum(payload) & 0xFF, _TAIL))


def probe_relay(host: str, tag: str) -> bool:
    """Mirror the SDK's short TCP probe to the TURN host."""
    host = str(host or "").strip()
    tag = str(tag or "").strip()
    if not host or not tag:
        return False

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(_PROBE_TIMEOUT_S)
    try:
        sock.connect((host, _PROBE_PORT))
        packet = _frame(tag)
        ok = False
        for _ in range(_PROBE_COUNT):
            sock.sendall(packet)
            try:
                response = sock.recv(64)
            except socket.timeout:
                continue
            ok = ok or (len(response) >= 19 and response[18] in (0x82, 0x84))
        return ok
    except OSError as err:
        _LOGGER.debug("TURN relay probe failed for %s:%d: %s", host, _PROBE_PORT, err)
        return False
    finally:
        try:
            sock.close()
        except OSError:
            pass


def probe_relay_hosts(hosts: Sequence[str], tag: str) -> bool:
    """Probe each relay host in order and stop at the first positive reply."""
    for host in hosts:
        if probe_relay(host, tag):
            return True
    return False


def start_relay_probe(
    hosts: Sequence[str],
    tag: str,
    *,
    on_done: Callable[[bool], None] | None = None,
) -> threading.Thread | None:
    """Send the relay probe in the background instead of blocking live start.

    The probe result never gates the media path: TURN permissions, channel
    binds and ``START_LIVE`` proceed regardless of it. When the relay accepts
    the TCP connection but never answers, a synchronous probe waits out every
    receive timeout on each host (IP + hostname of the same relay), which
    delays the first video frame by ~10 s. The same probe frames are still
    sent; only the wait is taken off the startup path.
    """
    hosts = [str(host).strip() for host in hosts if str(host or "").strip()]
    if not hosts:
        return None

    def _run() -> None:
        started = time.monotonic()
        probe_ok = False
        try:
            probe_ok = probe_relay_hosts(hosts, tag)
        finally:
            _LOGGER.debug(
                "TURN relay probe %s for %s tag=%s (%.1fs, background)",
                "ok" if probe_ok else "failed",
                ", ".join(hosts),
                tag,
                time.monotonic() - started,
            )
            if on_done is not None:
                on_done(probe_ok)

    thread = threading.Thread(target=_run, name="cloudplus-relay-probe", daemon=True)
    thread.start()
    return thread
