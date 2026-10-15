"""Home Assistant surface for talk: websocket commands and a test tone service.

All of it is **admin-only**: the websocket commands use ``require_admin`` and
the tone service is registered as an admin service, because it drives a
speaker at the user's home.

Websocket (keyed by the camera ``entity_id``):

- ``cloudplus/talk/start`` wakes the camera if needed, waits for live video
  and opens talk. Returns the audio format.
- ``cloudplus/talk/audio`` with ``audio``: base64 G.711 u-law, 8 kHz mono,
  any non-zero multiple of 320 bytes (40 ms frames), at most 1 s per message.
- ``cloudplus/talk/stop`` ends talk; queued audio still plays.

Talk stops by itself after 2 s without audio and after 60 s at most.

Service ``cloudplus.talk_tone`` plays a sine tone (default 800 Hz, 2 s, max
5 s) for supervised testing without a client.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from typing import Any, Callable

from .const import DOMAIN
from .coordinator.talk import TalkError
from .p2p_streamer.protocol import (
    TALK_FRAME_BYTES,
    TALK_FRAME_INTERVAL_S,
    TALK_SAMPLE_RATE,
)
from .p2p_streamer.talk import TALK_IDLE_TIMEOUT_S, TALK_MAX_S, tone_ulaw

_LOGGER = logging.getLogger(__name__)

WS_TALK_START = "cloudplus/talk/start"
WS_TALK_AUDIO = "cloudplus/talk/audio"
WS_TALK_STOP = "cloudplus/talk/stop"
SERVICE_TALK_TONE = "talk_tone"
AUDIO_MAX_BYTES_PER_MESSAGE = TALK_SAMPLE_RATE  # 1 s of u-law
TONE_DEFAULT_HZ = 800.0
TONE_DEFAULT_S = 2.0
TONE_MAX_S = 5.0
TONE_DEFAULT_VOLUME = 0.5
TONE_TAIL_S = 1.0  # open lead + margin before stopping
_REGISTERED_KEY = "_talk_api_registered"

Finder = Callable[[Any, str], Any]


def find_coordinator(hass: Any, entity_id: str) -> Any:
    """Map a cloudplus camera entity_id to its running coordinator."""
    # pylint: disable-next=import-outside-toplevel
    from homeassistant.helpers import entity_registry as er

    from .coordinator import CloudEdgeMeariCoordinator  # pylint: disable=import-outside-toplevel

    entry = er.async_get(hass).async_get(entity_id)
    if entry is None:
        return None
    for value in hass.data.get(DOMAIN, {}).values():
        if (
            isinstance(value, CloudEdgeMeariCoordinator)
            and entry.unique_id == f"{value.device_uuid}_camera"
        ):
            return value
    return None


def decode_audio(audio_b64: str) -> bytes:
    """Validate one websocket audio chunk."""
    try:
        audio = base64.b64decode(audio_b64, validate=True)
    except (binascii.Error, ValueError, TypeError) as exc:
        raise ValueError("audio must be base64") from exc
    if not audio or len(audio) % TALK_FRAME_BYTES:
        raise ValueError(f"audio must be a non-zero multiple of {TALK_FRAME_BYTES} bytes")
    if len(audio) > AUDIO_MAX_BYTES_PER_MESSAGE:
        raise ValueError(f"at most {AUDIO_MAX_BYTES_PER_MESSAGE} bytes per message")
    return audio


def _coordinator(hass: Any, entity_id: str, find: Finder) -> Any:
    coord = find(hass, entity_id)
    if coord is None:
        raise TalkError(f"{entity_id} is not a CloudEdge / Meari camera")
    return coord


async def talk_start(hass: Any, entity_id: str, find: Finder = find_coordinator) -> dict:
    coord = _coordinator(hass, entity_id, find)
    await coord.async_talk_start()
    return {
        "codec": "mulaw",
        "sample_rate": TALK_SAMPLE_RATE,
        "channels": 1,
        "frame_bytes": TALK_FRAME_BYTES,
        "frame_ms": int(TALK_FRAME_INTERVAL_S * 1000),
        "idle_timeout_s": TALK_IDLE_TIMEOUT_S,
        "max_s": TALK_MAX_S,
    }


async def talk_audio(
    hass: Any, entity_id: str, audio_b64: str, find: Finder = find_coordinator
) -> dict:
    coord = _coordinator(hass, entity_id, find)
    result = coord.talk_push(decode_audio(audio_b64))
    return {"queued_frames": result.queued, "dropped_frames": result.dropped}


async def talk_stop(hass: Any, entity_id: str, find: Finder = find_coordinator) -> dict:
    _coordinator(hass, entity_id, find).talk_stop()
    return {}


async def play_tone(
    coord: Any,
    frequency_hz: float = TONE_DEFAULT_HZ,
    duration_s: float = TONE_DEFAULT_S,
    volume: float = TONE_DEFAULT_VOLUME,
) -> None:
    """Open talk, play a tone, then stop.

    Waits for the tone to play before stopping: a stop before talk opens on
    the wire clears the queue.
    """
    duration_s = max(0.04, min(TONE_MAX_S, float(duration_s)))
    await coord.async_talk_start()
    try:
        coord.talk_push(tone_ulaw(frequency_hz, duration_s, volume))
        await asyncio.sleep(duration_s + TONE_TAIL_S)
    finally:
        coord.talk_stop()


def async_register_talk_api(hass: Any, find: Finder = find_coordinator) -> None:
    """Register the admin-only websocket commands and the tone service once."""
    domain_data = hass.data.setdefault(DOMAIN, {})
    if domain_data.get(_REGISTERED_KEY):
        return
    domain_data[_REGISTERED_KEY] = True

    # pylint: disable=import-outside-toplevel
    import voluptuous as vol
    from homeassistant.components import websocket_api
    from homeassistant.helpers import config_validation as cv
    from homeassistant.helpers.service import async_register_admin_service

    # pylint: enable=import-outside-toplevel

    async def _respond(connection: Any, msg: dict, call) -> None:
        try:
            result = await call
        except (TalkError, ValueError) as exc:
            connection.send_error(msg["id"], "talk_failed", str(exc))
            return
        connection.send_result(msg["id"], result)

    @websocket_api.require_admin
    @websocket_api.websocket_command(
        {vol.Required("type"): WS_TALK_START, vol.Required("entity_id"): cv.entity_id}
    )
    @websocket_api.async_response
    async def ws_talk_start(hass_: Any, connection: Any, msg: dict) -> None:
        await _respond(connection, msg, talk_start(hass_, msg["entity_id"], find))

    @websocket_api.require_admin
    @websocket_api.websocket_command(
        {
            vol.Required("type"): WS_TALK_AUDIO,
            vol.Required("entity_id"): cv.entity_id,
            vol.Required("audio"): str,
        }
    )
    @websocket_api.async_response
    async def ws_talk_audio(hass_: Any, connection: Any, msg: dict) -> None:
        await _respond(
            connection, msg, talk_audio(hass_, msg["entity_id"], msg["audio"], find)
        )

    @websocket_api.require_admin
    @websocket_api.websocket_command(
        {vol.Required("type"): WS_TALK_STOP, vol.Required("entity_id"): cv.entity_id}
    )
    @websocket_api.async_response
    async def ws_talk_stop(hass_: Any, connection: Any, msg: dict) -> None:
        await _respond(connection, msg, talk_stop(hass_, msg["entity_id"], find))

    for command in (ws_talk_start, ws_talk_audio, ws_talk_stop):
        websocket_api.async_register_command(hass, command)

    async def _handle_tone(call: Any) -> None:
        entity_ids = call.data.get("entity_id", [])
        if isinstance(entity_ids, str):
            entity_ids = [entity_ids]
        for entity_id in entity_ids:
            coord = find(hass, entity_id)
            if coord is None:
                _LOGGER.warning("talk_tone: no camera for %s", entity_id)
                continue
            try:
                await play_tone(
                    coord,
                    call.data.get("frequency", TONE_DEFAULT_HZ),
                    call.data.get("duration", TONE_DEFAULT_S),
                    call.data.get("volume", TONE_DEFAULT_VOLUME),
                )
            except TalkError as exc:
                _LOGGER.warning("talk_tone on %s failed: %s", entity_id, exc)

    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_TALK_TONE,
        _handle_tone,
        vol.Schema(
            {
                vol.Required("entity_id"): cv.entity_ids,
                vol.Optional("frequency", default=TONE_DEFAULT_HZ): vol.All(
                    vol.Coerce(float), vol.Range(min=100, max=4000)
                ),
                vol.Optional("duration", default=TONE_DEFAULT_S): vol.All(
                    vol.Coerce(float), vol.Range(min=0.1, max=TONE_MAX_S)
                ),
                vol.Optional("volume", default=TONE_DEFAULT_VOLUME): vol.All(
                    vol.Coerce(float), vol.Range(min=0.0, max=1.0)
                ),
            },
            extra=vol.ALLOW_EXTRA,
        ),
    )
