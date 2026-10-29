"""Cloud-offline gate for battery cameras: no idle-still wakes while offline.

When the signaling server reports a snap camera ``offline``, an idle-still
grab cannot succeed, yet every attempt sent wakes for up to 45 s and flipped
Camera Awake. This mixin parks the idle still instead:

- a grab that ends with signaling ``status=offline`` marks the camera offline;
- while offline, the idle loop only sends a probe (a P2P attempt without
  coordinator wakes) on a hard backoff, 5 min doubling to 2 h;
- the cloud status (OpenAPI, no camera traffic) is checked every 5 min, and a
  turn from ``offline`` to ``online``/``dormancy`` probes at once;
- any received keyframe (live view, motion wake) clears the state.

User wakes, motion wakes and wake-when-watched are never gated here.
"""

from __future__ import annotations

import logging

_LOGGER = logging.getLogger(__name__)

OFFLINE_PROBE_INITIAL_S = 300.0
OFFLINE_PROBE_MAX_S = 7200.0
OFFLINE_STATUS_CHECK_S = 300.0
REACHABLE_STATUSES = frozenset({"online", "dormancy"})


class CloudOfflineMixin:
    """Coordinator mixin tracking whether the cloud reports the camera offline."""

    def _init_cloud_offline(self) -> None:
        self._cloud_offline = False
        self._offline_probe_s = OFFLINE_PROBE_INITIAL_S
        self._next_offline_probe = 0.0
        self._next_offline_status_check = 0.0
        self._offline_api_said_offline = False

    @property
    def cloud_offline(self) -> bool:
        """True while the cloud reports the camera offline (idle wakes paused)."""
        return self._cloud_offline

    def _signaling_device_status(self) -> str:
        """Device status the last finished P2P session got from signaling."""
        return str(self._last_p2p_diagnostics.get("device_status") or "")

    def _note_idle_grab_failed(self, now: float) -> bool:
        """Classify a failed idle grab; True when the offline backoff owns it."""
        status = self._signaling_device_status()
        if status == "offline":
            self._enter_cloud_offline(now)
            return True
        if status in REACHABLE_STATUSES:
            self._leave_cloud_offline(f"signaling reports {status}")
            return False
        if self._cloud_offline:
            # No status at all (network down, signaling unreachable): stay
            # parked rather than fall back to waking every few minutes.
            self._enter_cloud_offline(now)
            return True
        return False

    def _enter_cloud_offline(self, now: float) -> None:
        if not self._cloud_offline:
            self._cloud_offline = True
            self._offline_probe_s = OFFLINE_PROBE_INITIAL_S
            self._offline_api_said_offline = False
            self._next_offline_status_check = now
            _LOGGER.warning(
                "Camera %s is offline in the cloud; idle still wakes paused, "
                "next probe in %.0f min",
                self._sn_num,
                self._offline_probe_s / 60.0,
            )
        else:
            self._offline_probe_s = min(
                OFFLINE_PROBE_MAX_S, self._offline_probe_s * 2.0
            )
            _LOGGER.debug(
                "Camera %s still offline; next probe in %.0f min",
                self._sn_num,
                self._offline_probe_s / 60.0,
            )
        self._next_offline_probe = now + self._offline_probe_s

    def _leave_cloud_offline(self, reason: str) -> None:
        if not self._cloud_offline:
            return
        self._cloud_offline = False
        self._offline_probe_s = OFFLINE_PROBE_INITIAL_S
        _LOGGER.info(
            "Camera %s is reachable again (%s); idle still resumes",
            self._sn_num,
            reason,
        )

    def _offline_probe_due(self, now: float) -> bool:
        """True when a parked camera should get one probe now."""
        if now >= self._next_offline_probe:
            return True
        if now < self._next_offline_status_check:
            return False
        self._next_offline_status_check = now + OFFLINE_STATUS_CHECK_S
        status = self._cloud_api_status()
        if status == "offline":
            self._offline_api_said_offline = True
            return False
        if status in REACHABLE_STATUSES and self._offline_api_said_offline:
            # Only a turn from offline counts: if this endpoint disagrees with
            # signaling, the backoff alone decides.
            _LOGGER.info(
                "Cloud reports camera %s %s; probing for the idle still",
                self._sn_num,
                status,
            )
            self._offline_api_said_offline = False
            return True
        return False

    def _cloud_api_status(self) -> str:
        api = self._api
        if api is None:
            return ""
        return str(api.get_device_status(self._sn_num) or "").lower()
