"""Small app-style keepalive helpers for P2P viewing sessions."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

_LOGGER = logging.getLogger(__name__)

SNAP_KEEPALIVE_CODES = ("1007", "1010", "1013")
SNAP_KEEPALIVE_INTERVAL_S = 10.0
# MsgSvr cadence of the official ieGeek app while live (capture, LAN direct):
# a routed webrtc hello every ~21 s and a device status query every ~120 s,
# besides the register-advertised heartbeat (30 s).
SIGNAL_HELLO_INTERVAL_S = 21.0
SIGNAL_STATUS_INTERVAL_S = 120.0


class SnapDeviceKeepalive:
    """Poll the same lightweight IoT values the official app reads while live."""

    def __init__(
        self,
        api: Any,
        sn_num: str,
        *,
        enabled: bool,
        interval_s: float = SNAP_KEEPALIVE_INTERVAL_S,
    ) -> None:
        self._api = api
        self._sn_num = sn_num
        self._enabled = enabled
        self._interval_s = interval_s
        self._next_at = 0.0
        self._busy = False
        self._lock = threading.Lock()

    def tick(self, now: float | None = None) -> None:
        """Schedule one non-blocking keepalive poll when due."""
        if not self._enabled:
            return
        now = time.monotonic() if now is None else now
        with self._lock:
            if self._busy or now < self._next_at:
                return
            self._busy = True
            self._next_at = now + self._interval_s
        threading.Thread(
            target=self._poll,
            name=f"cloudplus_snap_keepalive_{self._sn_num}",
            daemon=True,
        ).start()

    def _poll(self) -> None:
        try:
            fetch = getattr(self._api, "get_device_iot_values", None)
            if fetch is None:
                return
            fetch(self._sn_num, SNAP_KEEPALIVE_CODES)
        except (OSError, RuntimeError, ValueError, KeyError):
            _LOGGER.debug("Snap live keepalive failed", exc_info=True)
        finally:
            with self._lock:
                self._busy = False


@dataclass
class _SignalTask:
    label: str
    interval_s: float
    next_at: float
    send: Callable[[], None]


class SignalingKeepalive:
    """Keep the MsgSvr / webrtcsvr session warm while live, like the app.

    The webrtcsvr session is what tells the camera someone is viewing: when
    the app's session ends (logout) the camera reports dormancy within ~2 s.
    Sending only the register heartbeat let a live session die after ~2 min.
    All sends run on one short-lived worker so the media loop never blocks
    on the TCP signaling socket.
    """

    def __init__(
        self,
        sig: Any,
        device_uuid: str,
        *,
        heartbeat_s: float,
        start: float,
        name: str = "cloudplus_signal_hb",
        spawn: Callable[[Callable[[], None], str], None] | None = None,
    ) -> None:
        heartbeat_s = max(5.0, float(heartbeat_s or 30.0))
        self._tasks = [
            _SignalTask(
                "heartbeat",
                heartbeat_s,
                start + max(5.0, heartbeat_s * 2.0 / 3.0),
                lambda: sig.send_heartbeat(read_response=True),
            ),
            _SignalTask(
                "hello",
                SIGNAL_HELLO_INTERVAL_S,
                start + SIGNAL_HELLO_INTERVAL_S,
                lambda: sig.send_webrtc_keepalive(read_response=True),
            ),
            _SignalTask(
                "status",
                SIGNAL_STATUS_INTERVAL_S,
                start + SIGNAL_STATUS_INTERVAL_S,
                lambda: sig.send_status_keepalive(device_uuid, read_response=True),
            ),
        ]
        self._name = name
        self._spawn = spawn or _spawn_daemon
        self._busy = False
        self._lock = threading.Lock()

    def tick(self, now: float) -> list[str]:
        """Start one worker for every task that is due; return their labels."""
        with self._lock:
            if self._busy:
                return []
            due = [task for task in self._tasks if now >= task.next_at]
            if not due:
                return []
            for task in due:
                task.next_at = now + task.interval_s
            self._busy = True
        self._spawn(lambda: self._run(due), self._name)
        return [task.label for task in due]

    def _run(self, tasks: list[_SignalTask]) -> None:
        try:
            for task in tasks:
                try:
                    task.send()
                except (OSError, ValueError):
                    if task.label == "heartbeat":
                        _LOGGER.debug("Signaling heartbeat failed", exc_info=True)
                    else:
                        _LOGGER.debug("Signaling %s failed", task.label, exc_info=True)
        finally:
            with self._lock:
                self._busy = False


def _spawn_daemon(target: Callable[[], None], name: str) -> None:
    threading.Thread(target=target, name=name, daemon=True).start()
