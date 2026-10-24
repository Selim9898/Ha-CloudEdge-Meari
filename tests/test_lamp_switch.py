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
        self.assertEqual(len(coord._update_callbacks), 1)  # only the Siren waits

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
        self.assertEqual(len(coord._update_callbacks), 1)  # only the Siren waits

    async def test_unload_before_lamp_reported_stops_waiting(self):
        coord = coordinator(has_lamp=False)
        entry, add_entities = await self.setup(coord)
        self.assertEqual(len(coord._update_callbacks), 2)  # Lamp and Siren

        for unload in (call.args[0] for call in entry.async_on_unload.call_args_list):
            unload()
        self.assertEqual(coord._update_callbacks, [])

        coord._has_lamp = True
        fire_update(coord)
        self.assertEqual(lamp_switches(add_entities), [])

    async def test_floodlight_capability_adds_lamp_at_setup(self):
        coord = coordinator(has_lamp=False)
        coord._capabilities = {"flt": 1}  # floodlight camera, lamp code never reported
        _, add_entities = await self.setup(coord)
        self.assertEqual(len(lamp_switches(add_entities)), 1)
        self.assertEqual(len(coord._update_callbacks), 1)  # only the Siren waits

    async def test_floodlight_linkage_report_adds_lamp_at_setup(self):
        coord = coordinator(has_lamp=False)
        coord._iot_data = {SWITCH.FLIGHT_LINK_LIGHTING_ENABLE: 1}  # caps leave out flt
        _, add_entities = await self.setup(coord)
        self.assertEqual(len(lamp_switches(add_entities)), 1)

    async def test_floodlight_linkage_reported_after_setup_adds_lamp(self):
        coord = coordinator(has_lamp=False)
        _, add_entities = await self.setup(coord)
        self.assertEqual(lamp_switches(add_entities), [])

        coord._iot_data = {SWITCH.FLIGHT_LINK_LIGHTING_ENABLE: 1}  # status poll
        fire_update(coord)
        self.assertEqual(len(lamp_switches(add_entities)), 1)


def iot_switches(add_entities: Mock) -> dict[int, Any]:
    return {
        entity._spec.code: entity
        for call in add_entities.call_args_list
        for entity in call.args[0]
        if isinstance(entity, SWITCH.CloudEdgeMeariIotSwitch)
    }


class SirenSwitchTests(unittest.IsolatedAsyncioTestCase):
    """The manual Siren switch (IoT 182) is usable before the camera reports it."""

    async def test_siren_usable_before_reported(self):
        coord = coordinator(has_lamp=False)
        coord._capabilities = {"sir": 1, "sla": 8}
        entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
        hass = SimpleNamespace(data={SWITCH.DOMAIN: {entry.entry_id: coord}})
        add_entities = Mock()
        await SWITCH.async_setup_entry(hass, entry, add_entities)

        switches = iot_switches(add_entities)
        siren = switches[SWITCH.SOUND_LIGHT_ENABLE]
        self.assertEqual(siren._attr_name, "Siren")
        self.assertTrue(siren.available)
        self.assertFalse(siren.is_on)
        # settings still wait for a reported value
        self.assertFalse(switches[SWITCH.FLIGHT_LINK_SIREN_ENABLE].available)

        coord._available = False
        self.assertFalse(siren.available)

    async def test_siren_from_reported_alarm_type(self):
        coord = coordinator(has_lamp=False)
        coord._iot_data = {SWITCH.SOUND_LIGHT_TYPE: 1}  # caps leave out sla
        entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
        hass = SimpleNamespace(data={SWITCH.DOMAIN: {entry.entry_id: coord}})
        add_entities = Mock()
        await SWITCH.async_setup_entry(hass, entry, add_entities)
        self.assertIn(SWITCH.SOUND_LIGHT_ENABLE, iot_switches(add_entities))

    async def test_siren_added_when_alarm_type_reported_after_setup(self):
        coord = coordinator(has_lamp=True)
        entry = SimpleNamespace(entry_id="test", async_on_unload=Mock())
        hass = SimpleNamespace(data={SWITCH.DOMAIN: {entry.entry_id: coord}})
        add_entities = Mock()
        await SWITCH.async_setup_entry(hass, entry, add_entities)
        self.assertNotIn(SWITCH.SOUND_LIGHT_ENABLE, iot_switches(add_entities))

        coord._iot_data = {SWITCH.SOUND_LIGHT_TYPE: 1}  # status poll
        fire_update(coord)
        fire_update(coord)
        sirens = [
            entity
            for call in add_entities.call_args_list
            for entity in call.args[0]
            if getattr(entity, "_spec", None) is not None
            and entity._spec.code == SWITCH.SOUND_LIGHT_ENABLE
        ]
        self.assertEqual(len(sirens), 1)
        self.assertEqual(coord._update_callbacks, [])


if __name__ == "__main__":
    unittest.main()
