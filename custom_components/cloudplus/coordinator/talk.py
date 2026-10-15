"""Coordinator side of talk: get the camera live, then feed the talk queue.

Talk only works inside a running live session (the official app enforces
the same), so ``async_talk_start`` wakes the camera if needed and waits until
live frames arrive. While talk runs the live window is kept open a few
seconds past the queued audio, and that live time is charged to the
wake-when-watched budget like watching (an explicit user action, so it is
not blocked by a capped session, but it does use up the hour's budget).
"""

from __future__ import annotations

import asyncio
import time

from ..p2p_streamer.talk import TalkChannel, TalkPushResult

TALK_LIVE_TIMEOUT_S = 45.0
TALK_LIVE_LINGER_S = 5.0
TALK_CAPABLE_VTK = (1, 3, 4)  # speaker only, half duplex, full duplex


class TalkError(RuntimeError):
    """Talk cannot start or continue."""


class TalkMixin:
    """Coordinator mixin: talk session lifecycle on top of the live window."""

    def _init_talk(self) -> None:
        self._talk = TalkChannel()

    @property
    def supports_talk(self) -> bool:
        """KCP-path cameras whose capability does not rule talk out."""
        if str(self._device.get("deviceP2P") or "").lower() == "ppcs":
            return False  # different talk framing; no capture yet
        vtk = self.iot_capability("vtk")
        return vtk is None or vtk in TALK_CAPABLE_VTK

    @property
    def talk_active(self) -> bool:
        return self._talk.active

    async def async_talk_start(self, timeout_s: float = TALK_LIVE_TIMEOUT_S) -> None:
        """Make sure the camera is live, then open a talk session."""
        if not self.supports_talk:
            raise TalkError("This camera does not support talk")
        if not self._running:
            raise TalkError("Camera is not running")
        if self._talk.active:
            self._talk.begin()  # refresh the idle timer
            self._keep_live_for_talk()
            return
        if self.camera_awake:
            self._keep_live_for_talk()
        else:
            self.wake_camera()  # the normal wake window
        deadline = time.monotonic() + timeout_s
        while not self.camera_awake:
            if time.monotonic() >= deadline:
                raise TalkError("Camera did not go live")
            await asyncio.sleep(0.25)
        self._talk.begin()
        self._keep_live_for_talk()

    def talk_push(self, ulaw: bytes) -> TalkPushResult:
        """Queue G.711 u-law audio (8 kHz mono) for the camera speaker."""
        try:
            result = self._talk.push(ulaw)
        except RuntimeError as exc:
            raise TalkError(str(exc)) from exc
        self._keep_live_for_talk()
        return result

    def talk_stop(self) -> None:
        self._talk.stop()

    def _keep_live_for_talk(self) -> None:
        """Hold the live window past the queued audio; charge the budget."""
        now = time.monotonic()
        target = now + self._talk.queued_s + TALK_LIVE_LINGER_S
        grant_from = max(self._live_deadline, now)
        if target > grant_from:
            self._watch.grant(grant_from, target)
            self._live_deadline = target
