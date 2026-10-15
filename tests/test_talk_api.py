"""Offline checks for the admin-only talk websocket commands and tone service."""

from __future__ import annotations

import base64
import importlib
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

_bootstrap_integration_modules()
TALK_API = importlib.import_module("custom_components.cloudplus.talk_api")
TALK = importlib.import_module("custom_components.cloudplus.p2p_streamer.talk")
COORD_TALK = importlib.import_module("custom_components.cloudplus.coordinator.talk")

FRAME = TALK.TALK_FRAME_BYTES
ENTITY = "camera.front_door"


def fake_coordinator() -> Mock:
    coord = Mock()
    coord.async_talk_start = AsyncMock()
    coord.talk_push = Mock(side_effect=lambda audio: len(audio) // FRAME)
    return coord


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.coord = fake_coordinator()
        self.find = lambda _hass, entity_id: self.coord if entity_id == ENTITY else None

    async def test_start_opens_talk_and_reports_the_format(self) -> None:
        result = await TALK_API.talk_start(None, ENTITY, self.find)
        self.coord.async_talk_start.assert_awaited_once_with()
        self.assertEqual(
            (result["codec"], result["sample_rate"], result["frame_bytes"], result["frame_ms"]),
            ("mulaw", 8000, 320, 40),
        )
        self.assertEqual((result["idle_timeout_s"], result["max_s"]), (2.0, 60.0))

    async def test_audio_is_queued(self) -> None:
        audio = base64.b64encode(b"\xff" * FRAME * 3).decode()
        result = await TALK_API.talk_audio(None, ENTITY, audio, self.find)
        self.assertEqual(result, {"queued_frames": 3})

    async def test_unknown_camera(self) -> None:
        with self.assertRaises(COORD_TALK.TalkError):
            await TALK_API.talk_start(None, "camera.other", self.find)

    async def test_stop(self) -> None:
        await TALK_API.talk_stop(None, ENTITY, self.find)
        self.coord.talk_stop.assert_called_once_with()

    def test_audio_validation(self) -> None:
        for bad in ("not base64!", "", base64.b64encode(b"\xff" * 100).decode(),
                    base64.b64encode(b"\xff" * (FRAME * 26)).decode()):
            with self.subTest(length=len(bad)):
                with self.assertRaises(ValueError):
                    TALK_API.decode_audio(bad)
        self.assertEqual(len(TALK_API.decode_audio(base64.b64encode(b"\xff" * FRAME * 25).decode())), 8000)

    async def test_tone_opens_queues_and_stops(self) -> None:
        await TALK_API.play_tone(self.coord, 800, 2.0, 0.5)
        self.coord.async_talk_start.assert_awaited_once_with()
        pushed = self.coord.talk_push.call_args.args[0]
        self.assertEqual(len(pushed), 2 * 8000)
        self.coord.talk_stop.assert_called_once_with()

    async def test_tone_is_capped_at_5_s(self) -> None:
        await TALK_API.play_tone(self.coord, 800, 30.0)
        self.assertEqual(len(self.coord.talk_push.call_args.args[0]), 5 * 8000)

    async def test_tone_stops_even_if_push_fails(self) -> None:
        self.coord.talk_push.side_effect = COORD_TALK.TalkError("no talk session")
        with self.assertRaises(COORD_TALK.TalkError):
            await TALK_API.play_tone(self.coord)
        self.coord.talk_stop.assert_called_once_with()


def ha_stubs() -> dict[str, ModuleType]:
    """Just enough of voluptuous / websocket_api / helpers to register."""
    vol = ModuleType("voluptuous")
    vol.Required = vol.Optional = lambda key, **_kw: key
    vol.All = lambda *validators: validators
    vol.Coerce = vol.Range = lambda *a, **kw: (a, kw)
    vol.Schema = lambda schema, **_kw: schema
    vol.ALLOW_EXTRA = object()

    ws = ModuleType("homeassistant.components.websocket_api")
    ws.registered = []

    def require_admin(func):
        func.admin_only = True
        return func

    def websocket_command(schema):
        def wrap(func):
            func.schema = schema
            return func
        return wrap

    ws.require_admin = require_admin
    ws.websocket_command = websocket_command
    ws.async_response = lambda func: func
    ws.async_register_command = lambda _hass, func: ws.registered.append(func)

    cv = ModuleType("homeassistant.helpers.config_validation")
    cv.entity_id = cv.entity_ids = str
    service = ModuleType("homeassistant.helpers.service")
    service.admin_services = []
    service.async_register_admin_service = (
        lambda _hass, domain, name, handler, schema: service.admin_services.append((domain, name))
    )
    return {
        "voluptuous": vol,
        "homeassistant.components.websocket_api": ws,
        "homeassistant.helpers.config_validation": cv,
        "homeassistant.helpers.service": service,
    }


class RegistrationTests(unittest.TestCase):
    def test_admin_only_commands_and_service_registered_once(self) -> None:
        stubs = ha_stubs()
        hass = SimpleNamespace(data={})
        with patch.dict(sys.modules, stubs):
            TALK_API.async_register_talk_api(hass)
            TALK_API.async_register_talk_api(hass)  # second camera entry
        ws = stubs["homeassistant.components.websocket_api"]
        types = [func.schema["type"] for func in ws.registered]
        self.assertEqual(types, ["cloudplus/talk/start", "cloudplus/talk/audio", "cloudplus/talk/stop"])
        self.assertTrue(all(getattr(func, "admin_only", False) for func in ws.registered))
        self.assertEqual(
            stubs["homeassistant.helpers.service"].admin_services, [("cloudplus", "talk_tone")]
        )


if __name__ == "__main__":
    unittest.main()
