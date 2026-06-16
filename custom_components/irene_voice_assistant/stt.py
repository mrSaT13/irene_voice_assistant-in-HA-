# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterable

import aiohttp

# Импортируем актуальные классы для HA 2023.5+
from homeassistant.components.stt import (
    AudioEncoding,
    AudioFormat,
    SpeechMetadata,
    SpeechResult,
    SpeechResultState,
    SpeechToTextEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DOMAIN,
    API_WEBSOCKET,
    MSG_NEGOTIATE_REQUEST,
    MSG_NEGOTIATE_AGREE,
    MSG_IN_STT_SERVERSIDE_READY,
    MSG_IN_STT_SERVERSIDE_RECOGNIZED,
    PROTOCOL_IN_STT_SERVERSIDE,
)
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up STT platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]

    async_add_entities([
        IreneSTTEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT."""

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the STT entity."""
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"

    @property
    def supported_languages(self) -> list[str]:
        """Return supported languages."""
        return ["ru", "en"]

    @property
    def supported_formats(self) -> list[AudioFormat]:
        """Return supported audio formats (упаковываем все параметры в AudioFormat)."""
        return [
            AudioFormat(
                codec=AudioEncoding.WAV,
                bit_rate=16000,
                sample_size=16,
                channels=1,
            ),
            AudioFormat(
                codec=AudioEncoding.LINEAR16,
                bit_rate=16000,
                sample_size=16,
                channels=1,
            ),
        ]

    # ВАЖНО: Порядок аргументов теперь (metadata, stream)
    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream and return recognized text."""
        try:
            _LOGGER.info("Starting STT processing")

            # Подключаемся к WebSocket Ирины
            ws_url = f"{self.coordinator.ws_base_url}{API_WEBSOCKET}"
            session = async_get_clientsession(self.hass, verify_ssl=False)

            async with session.ws_connect(
                ws_url,
                timeout=15.0,
                ssl=self.coordinator._ssl_context if self.coordinator._ssl_context else False,
            ) as ws:
                # Negotiate STT protocol
                negotiate_msg = {
                    "type": MSG_NEGOTIATE_REQUEST,
                    "protocols": [
                        [PROTOCOL_IN_STT_SERVERSIDE],
                    ],
                }
                await ws.send_json(negotiate_msg)

                # Ждём готовность сервера
                msg = await asyncio.wait_for(ws.receive_json(), timeout=10.0)
                if msg.get("type") != MSG_NEGOTIATE_AGREE:
                    _LOGGER.error(f"STT negotiate failed: {msg}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                # Ждём READY
                msg = await asyncio.wait_for(ws.receive_json(), timeout=10.0)
                if msg.get("type") != MSG_IN_STT_SERVERSIDE_READY:
                    _LOGGER.error(f"STT not ready: {msg}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                _LOGGER.info("STT server ready, sending audio")

                # Отправляем аудио чанками
                async for chunk in stream:
                    if chunk:
                        await ws.send_bytes(chunk)

                # Отправляем конец потока
                await ws.send_bytes(b"")

                # Ждём распознанный текст
                try:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=15.0)
                    if msg.get("type") == MSG_IN_STT_SERVERSIDE_RECOGNIZED:
                        text = msg.get("text", "")
                        _LOGGER.info(f"STT recognized: {text}")
                        # Используем SpeechResultState.SUCCESS
                        return SpeechResult(text, SpeechResultState.SUCCESS)
                    else:
                        _LOGGER.error(f"STT error: {msg}")
                        return SpeechResult(None, SpeechResultState.ERROR)
                except asyncio.TimeoutError:
                    _LOGGER.error("STT timeout waiting for recognition")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
