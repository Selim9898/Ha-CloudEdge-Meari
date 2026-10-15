"""Offline regressions for the Lamp switch when the lamp code arrives after setup."""

from __future__ import annotations

import importlib
import sys
import unittest
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from debug_tools.bootstrap import _bootstrap_integration_modules

MODULES = _bootstrap_integration_modules()
COORDINATOR = MODULES["coordinator"].CloudEdgeMeariCoordinator


def load_switch() -> ModuleType:
    """Import the switch platform with the HA stubs it needs beyond the bootstrap."""
    switch_stub = ModuleType("homeassistant.components.switch")
    switch_stub.SwitchEntity = type("SwitchEntity", (), {})
    const_stub = ModuleType("homeassistant.const")
    const_stub.EntityCategory = SimpleNamespace(CONFIG="config")
    with patch.dict(
        sys.modules,
        {
            "homeassistant.components.switch": switch_stub,
            "homeassistant.const": const_stub,
        },
    ):
        return importlib.import_module("custom_components.cloudplus.switch")


SWITCH = load_switch()


def coordinator(has_lamp: bool) -> Any:
    """Coordinator state for a mains-powered floodlight camera, without threads or cloud."""
    coord = COORDINATOR.__new__(COORDINATOR)
    coord._capabilities = {}
    coord._device = {}
    coord._iot_data = {}
    coord._sn_num = "synthetic-camera"
    coord._device_name = "Synthetic floodlight"
    coord._device_category = "ipc"
    coord._is_snap = False
    coord._available = True
    coord._has_lamp = has_lamp
    coord._lamp_on = False
    coord._update_callbacks = []
    return coord


def fire_update(coord: Any) -> None:
    """Run the update callbacks as the coordinator's event-loop hop would."""
    for cb in list(coord._update_callbacks):
        cb()


def lamp_switches(add_entities: Mock) -> list:
    return [
        entity
        for call in add_entities.call_args_list
        for entity in call.args[0]
        if isinstance(entity, SWITCH.CloudEdgeMeariLampSwitch)
    ]


class LampSwitchSetupTests(unittest.IsolatedAsyncioTestCase):
    """The Lamp switch must not depend on the lamp code being in the setup-time read."""

    async def setup(self, coord: Any) -> tuple[Mock, Mock]:
        entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
        hass = SimpleNamespace(data={SWITCH.DOMAIN: {entry.entry_id: coord}})
        add_entities = Mock()
        await SWITCH.async_setup_entry(hass, entry, add_entities)
        return entry, add_entities

    async def test_lamp_known_at_setup(self):
        coord = coordinator(has_lamp=True)
        _, add_entities = await self.setup(coord)
        self.assertEqual(len(lamp_switches(add_entities)), 1)
        self.assertEqual(coord._update_callbacks, [])

    async def test_lamp_reported_after_setup_adds_switch_once(self):
        coord = coordinator(has_lamp=False)
        _, add_entities = await self.setup(coord)
        self.assertEqual(lamp_switches(add_entities), [])

        fire_update(coord)  # an update without the lamp code changes nothing
        self.assertEqual(lamp_switches(add_entities), [])

        coord._has_lamp = True  # the status poll saw IoT code 167
        fire_update(coord)
        switches = lamp_switches(add_entities)
        self.assertEqual(len(switches), 1)
        # the same unique ID as a setup-time switch, so it keeps its entity ID
        self.assertEqual(switches[0]._attr_unique_id, "synthetic-camera_lamp")

        fire_update(coord)  # later updates must not add a second switch
        self.assertEqual(len(lamp_switches(add_entities)), 1)
        self.assertEqual(coord._update_callbacks, [])

    async def test_unload_before_lamp_reported_stops_waiting(self):
        coord = coordinator(has_lamp=False)
        entry, add_entities = await self.setup(coord)
        self.assertEqual(len(coord._update_callbacks), 1)

        for unload in (call.args[0] for call in entry.async_on_unload.call_args_list):
            unload()
        self.assertEqual(coord._update_callbacks, [])

        coord._has_lamp = True
        fire_update(coord)
        self.assertEqual(lamp_switches(add_entities), [])


if __name__ == "__main__":
    unittest.main()
