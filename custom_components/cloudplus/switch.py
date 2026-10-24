"""Switch platform for CloudEdge / Meari — wake toggles, lamp and IoT switches."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_WATCH_WAKE, DOMAIN
from .coordinator import CloudEdgeMeariCoordinator
from .entity import CloudEdgeMeariEntity, CloudEdgeMeariIotEntity
from .meari_commands import (
    ABNORMAL_NOISE_ENABLE,
    ANTI_JAMMING,
    AUTO_UPDATE,
    CRY_DET_ENABLE,
    FACE_RECOGNITION_SWITCH,
    FLIGHT_LINK_LIGHTING_ENABLE,
    FLIGHT_LINK_SIREN_ENABLE,
    HOMEKIT_ENABLE,
    HUMAN_DET_ENABLE,
    HUMAN_TRACK_ENABLE,
    LASER_SWITCH,
    LED_ENABLE,
    LOGO_SWITCH,
    MOTION_DET_ENABLE,
    ONVIF_ENABLE,
    OSD_ENABLE,
    PET_ALARM_ENABLE,
    PET_THROW_WARNING,
    PIR_DET_ENABLE,
    PTZ_PATROL,
    RECORD_SWITCH,
    RGB_LIGHT_SWITCH,
    SLEEP_MODE,
    SOUND_DET_ENABLE,
    SOUND_LIGHT_ENABLE,
    SOUND_LIGHT_TYPE,
    SOUND_SWITCH,
)

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IotSwitchSpec:
    """Declarative spec for an IoT-backed switch entity."""

    feature: str | None
    code: int
    name: str
    icon: str | None = None
    # False for commands a sleeping camera may never have reported: the
    # switch stays usable while the coordinator is up, so a write can reach it.
    needs_report: bool = True
    # Another code whose report shows the camera has this feature, for
    # cameras whose capability list leaves it out.
    sibling: int | None = None


IOT_SWITCHES: tuple[IotSwitchSpec, ...] = (
    IotSwitchSpec("status_led", LED_ENABLE, "Status LED", "mdi:led-on"),
    IotSwitchSpec(
        "motion_det",
        MOTION_DET_ENABLE,
        "Motion Detection",
        "mdi:motion-sensor",
    ),
    IotSwitchSpec(
        "person_det",
        HUMAN_DET_ENABLE,
        "Person Detection",
        "mdi:account-search",
    ),
    IotSwitchSpec(
        "human_track",
        HUMAN_TRACK_ENABLE,
        "Human Tracking",
        "mdi:target-account",
    ),
    IotSwitchSpec("noise_det", SOUND_DET_ENABLE, "Sound Detection", "mdi:ear-hearing"),
    IotSwitchSpec(
        "cry_det",
        CRY_DET_ENABLE,
        "Crying Detection",
        "mdi:baby-face-outline",
    ),
    IotSwitchSpec("onvif", ONVIF_ENABLE, "ONVIF", "mdi:network-outline"),
    IotSwitchSpec("sd_card", RECORD_SWITCH, "SD Recording", "mdi:sd"),
    IotSwitchSpec("pir", PIR_DET_ENABLE, "PIR Sensor", "mdi:motion-sensor"),
    IotSwitchSpec(
        "floodlight",
        FLIGHT_LINK_LIGHTING_ENABLE,
        "Floodlight Linkage",
        "mdi:link-variant",
    ),
    IotSwitchSpec(
        "siren",
        FLIGHT_LINK_SIREN_ENABLE,
        "Siren Linkage",
        "mdi:alarm-light",
    ),
    IotSwitchSpec(
        "siren_alarm",
        SOUND_LIGHT_ENABLE,
        "Siren",
        "mdi:alarm-light",
        needs_report=False,
        sibling=SOUND_LIGHT_TYPE,
    ),
    IotSwitchSpec(
        "face_det",
        FACE_RECOGNITION_SWITCH,
        "Face Recognition",
        "mdi:face-recognition",
    ),
    IotSwitchSpec("sleep_mode", SLEEP_MODE, "Sleep Mode", "mdi:sleep"),
    IotSwitchSpec("rgb_light", RGB_LIGHT_SWITCH, "RGB Light", "mdi:palette"),
    IotSwitchSpec("anti_jamming", ANTI_JAMMING, "Anti-Jamming", "mdi:shield-check"),
    IotSwitchSpec(
        "abnormal_noise",
        ABNORMAL_NOISE_ENABLE,
        "Abnormal Noise",
        "mdi:alert-decagram",
    ),
    IotSwitchSpec("ptz_patrol", PTZ_PATROL, "PTZ Patrol", "mdi:pan-horizontal"),
    IotSwitchSpec("laser", LASER_SWITCH, "Laser Toy", "mdi:laser-pointer"),
    IotSwitchSpec("pet_alarm", PET_ALARM_ENABLE, "Pet Alarm", "mdi:bell-alert"),
    IotSwitchSpec(
        "pet_alarm",
        PET_THROW_WARNING,
        "Pet Throw Warning",
        "mdi:bell-alert",
    ),
    IotSwitchSpec("homekit", HOMEKIT_ENABLE, "HomeKit", "mdi:home-automation"),
    IotSwitchSpec("auto_update", AUTO_UPDATE, "Auto Update", "mdi:update"),
    IotSwitchSpec(None, OSD_ENABLE, "OSD Watermark", "mdi:watermark"),
    IotSwitchSpec(None, LOGO_SWITCH, "Brand Logo", "mdi:label-outline"),
    IotSwitchSpec(None, SOUND_SWITCH, "Sound", "mdi:volume-high"),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up CloudEdge / Meari switches from a config entry."""
    coord: CloudEdgeMeariCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SwitchEntity] = []
    if coord.is_battery_camera:
        entities.append(CloudEdgeMeariMotionWakeSwitch(coord, entry))
        entities.append(CloudEdgeMeariWatchWakeSwitch(coord, entry))
    if _has_lamp(coord):
        entities.append(CloudEdgeMeariLampSwitch(coord, entry))
    else:
        _add_switch_when_reported(
            coord,
            entry,
            async_add_entities,
            partial(_has_lamp, coord),
            partial(CloudEdgeMeariLampSwitch, coord, entry),
        )
    for spec in IOT_SWITCHES:
        if _supports_iot_switch(coord, spec):
            entities.append(CloudEdgeMeariIotSwitch(coord, entry, spec))
        elif spec.sibling is not None:
            _add_switch_when_reported(
                coord,
                entry,
                async_add_entities,
                partial(_supports_iot_switch, coord, spec),
                partial(CloudEdgeMeariIotSwitch, coord, entry, spec),
            )
    async_add_entities(entities)


def _has_lamp(coord: CloudEdgeMeariCoordinator) -> bool:
    """Return whether the camera has a floodlight the Lamp switch can drive.

    A sleeping camera may never have reported the lamp code itself, and some
    leave floodlight out of their capability list but report the linkage.
    """
    return (
        coord.has_lamp
        or coord.supports_iot("floodlight")
        or coord.has_iot_code(FLIGHT_LINK_LIGHTING_ENABLE)
    )


def _supports_iot_switch(coord: CloudEdgeMeariCoordinator, spec: IotSwitchSpec) -> bool:
    return bool(
        (spec.feature and coord.supports_iot(spec.feature))
        or coord.has_iot_code(spec.code)
        or (spec.sibling is not None and coord.has_iot_code(spec.sibling))
    )


def _add_switch_when_reported(
    coord: CloudEdgeMeariCoordinator,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    ready: Callable[[], bool],
    make: Callable[[], SwitchEntity],
) -> None:
    """Add a switch once the camera first reports what shows it has the feature.

    The IoT config read during setup can come back without those codes (or
    fail), even on a floodlight camera; the coordinator's status poll picks
    them up later, but by then the platform has been set up. Without this the
    switch would stay missing until the config entry is reloaded.
    """
    unsub: Callable[[], None] | None = None

    @callback
    def _check() -> None:
        nonlocal unsub
        if unsub is None or not ready():
            return
        unsub()
        unsub = None
        switch = make()
        _LOGGER.debug(
            "Feature reported for %s after setup; adding %s",
            coord.device_name,
            type(switch).__name__,
        )
        async_add_entities([switch])

    @callback
    def _stop_waiting() -> None:
        nonlocal unsub
        if unsub is not None:
            unsub()
            unsub = None

    unsub = coord.register_update_callback(_check)
    entry.async_on_unload(_stop_waiting)


class CloudEdgeMeariIotSwitch(CloudEdgeMeariIotEntity, SwitchEntity):
    """Switch entity backed by a Meari IoT value."""

    def __init__(
        self,
        coordinator: CloudEdgeMeariCoordinator,
        entry: ConfigEntry,
        spec: IotSwitchSpec,
    ) -> None:
        super().__init__(coordinator, entry, spec)
        self._attr_unique_id = f"{coordinator.device_uuid}_iot_switch_{spec.code}"

    @property
    def available(self) -> bool:
        if self._spec.needs_report:
            return super().available
        return self._coordinator.available

    @property
    def is_on(self) -> bool:
        value = self._iot_value
        try:
            return int(value) == 1
        except (TypeError, ValueError):
            return False

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn the camera IoT value on."""
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            1,
        )
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn the camera IoT value off."""
        await self.hass.async_add_executor_job(
            self._coordinator.set_iot_value,
            self._spec.code,
            0,
        )
        self.async_write_ha_state()


class CloudEdgeMeariMotionWakeSwitch(CloudEdgeMeariEntity, SwitchEntity):
    """Switch to enable / disable automatic wake on motion."""

    _attr_name = "Wake on Motion"
    _attr_icon = "mdi:motion-sensor"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_motion_wake"

    @property
    def is_on(self) -> bool:
        return self._coordinator.motion_wake_enabled

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Enable wake on motion."""
        self._coordinator.set_motion_wake_enabled(True)
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Disable wake on motion."""
        self._coordinator.set_motion_wake_enabled(False)
        self.async_write_ha_state()


class CloudEdgeMeariWatchWakeSwitch(CloudEdgeMeariEntity, SwitchEntity):
    """Switch to wake the camera while a client watches its stream.

    Stored in the camera entry's options so it survives restarts; the cap
    per viewing session is set in the camera's options.
    """

    _attr_name = "Wake when watched"
    _attr_icon = "mdi:eye-arrow-right-outline"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_watch_wake"

    @property
    def is_on(self) -> bool:
        return self._coordinator.watch_wake_enabled

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Enable wake when watched."""
        self._set(True)

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Disable wake when watched."""
        self._set(False)

    def _set(self, enabled: bool) -> None:
        self._coordinator.set_watch_wake_enabled(enabled)
        options = dict(self._entry.options)
        options[CONF_WATCH_WAKE] = enabled
        self.hass.config_entries.async_update_entry(self._entry, options=options)
        self.async_write_ha_state()


class CloudEdgeMeariLampSwitch(CloudEdgeMeariEntity, SwitchEntity):
    """Switch to control the camera's built-in lamp / LED light."""

    _attr_name = "Lamp"
    _attr_icon = "mdi:lightbulb"

    def __init__(
        self, coordinator: CloudEdgeMeariCoordinator, entry: ConfigEntry
    ) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{coordinator.device_uuid}_lamp"

    @property
    def is_on(self) -> bool:
        return self._coordinator.lamp_on

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn on the lamp."""
        await self.hass.async_add_executor_job(self._coordinator.set_lamp, True)
        self.async_write_ha_state()

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn off the lamp."""
        await self.hass.async_add_executor_job(self._coordinator.set_lamp, False)
        self.async_write_ha_state()
