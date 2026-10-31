"""The device status a session reports is the strongest any server gave.

Two synthetic signaling servers, no sockets, no cloud, no camera.
"""

from __future__ import annotations

import importlib
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
ENGINE = importlib.import_module("custom_components.cloudplus.p2p_streamer.engine")

SERVERS = [("192.0.2.1", 9000), ("192.0.2.2", 9000)]


def fake_signaling(statuses: dict[str, str]) -> type:
    """MsgSvrClient stand-in answering a fixed device status per server."""

    class _Sig:
        def __init__(self, ip: str, _port: int, **_kwargs) -> None:
            self.ip = ip

        def connect(self, **_kwargs) -> None:
            pass

        def register(self, **_kwargs) -> None:
            pass

        def webrtc_hello_full(self) -> None:
            pass

        def query_device_status(self, _uuid: str) -> dict:
            return {"status": statuses[self.ip]}

        def request_coturn(self, _uuid: str) -> dict:
            raise OSError("no relay")

        def send_logout(self, _uuid: str) -> None:
            pass

        def close(self) -> None:
            pass

    return _Sig


def run_against(statuses: dict[str, str]) -> str:
    api = SimpleNamespace(
        platform_domain=None,
        openapi_server=None,
        api_server=None,
        app_profile="cloudedge",
        country_code="DE",
        wake_device=Mock(),
    )
    device = {"deviceID": 1, "snNum": "synthetic-camera", "_category": "snap"}
    streamer = ENGINE.P2PStreamer(api, device)
    with (
        patch.object(ENGINE, "MsgSvrClient", fake_signaling(statuses)),
        patch.object(
            ENGINE, "_resolve_signaling_server_candidates", return_value=SERVERS
        ),
        patch.object(ENGINE, "_cache_signaling_endpoint"),
        patch.object(ENGINE, "_forget_signaling_endpoint"),
        patch.object(ENGINE, "_get_local_ips", return_value=[]),
        patch.object(
            streamer, "_request_dormant_coturn", side_effect=OSError("wake failed")
        ),
    ):
        streamer.run_session()
    return streamer.diagnostics["device_status"]


class SignalingStatusTests(unittest.TestCase):
    def test_offline_from_one_server_survives_unknown_from_the_next(self) -> None:
        status = run_against({"192.0.2.1": "offline", "192.0.2.2": "unknown"})
        self.assertEqual(status, "offline")

    def test_dormancy_survives_offline_from_a_wrong_server(self) -> None:
        # The right server knows the camera but the connect fails; a server
        # that doesn't know it then says offline. The camera is reachable.
        status = run_against({"192.0.2.1": "dormancy", "192.0.2.2": "offline"})
        self.assertEqual(status, "dormancy")

    def test_online_survives_offline_after_it(self) -> None:
        status = run_against({"192.0.2.1": "online", "192.0.2.2": "offline"})
        self.assertEqual(status, "online")

    def test_unknown_is_kept_when_no_server_knows_more(self) -> None:
        status = run_against({"192.0.2.1": "unknown", "192.0.2.2": "unknown"})
        self.assertEqual(status, "unknown")


if __name__ == "__main__":
    unittest.main()
