# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterable

import aiohttp

# ✅ Актуальные импорты для современных версий Home Assistant
from homeassistant.components.stt import (
    AudioBitRates,
    AudioChannels,
    AudioCodecs,
    AudioFormats,
    AudioSampleRates,
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
    def supported_formats(self) -> list[AudioFormats]:
        """Return supported audio formats."""
        return [AudioFormats.WAV]
    
    @property
    def supported_codecs(self) -> list[AudioCodecs]:
        """Return supported audio codecs."""
        return [AudioCodecs.PCM]
    
    @property
    def supported_bit_rates(self) -> list[AudioBitRates]:
        """Return supported bit rates."""
        return [AudioBitRates.BITRATE_16]
    
    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]:
        """Return supported sample rates."""
        return [AudioSampleRates.SAMPLERATE_16000]
    
    @property
    def supported_channels(self) -> list[AudioChannels]:
        """Return supported channels."""
        return [AudioChannels.CHANNEL_MONO]

    # ✅ ВАЖНО: Порядок аргументов теперь (metadata, stream)
    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream and return recognized text."""
        try:
            _LOGGER.info("Starting STT processing")
            
            ws_url = f"{self.coordinator.ws_base_url}{API_WEBSOCKET}"
            session = async_get_clientsession(self.hass, verify_ssl=False)
            
            async with session.ws_connect(
                ws_url,
                timeout=15.0,
                ssl=self.coordinator._ssl_context if self.coordinator._ssl_context else False,
            ) as ws:
                # 1. Negotiate STT protocol
                negotiate_msg = {
                    "type": MSG_NEGOTIATE_REQUEST,
                    "protocols": [[PROTOCOL_IN_STT_SERVERSIDE]],
                }
                await ws.send_json(negotiate_msg)
                
                # 2. Ждём готовность сервера
                msg = await asyncio.wait_for(ws.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"STT negotiate failed, expected TEXT, got {msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                    
                data = msg.json()
                if data.get("type") != MSG_NEGOTIATE_AGREE:
                    _LOGGER.error(f"STT negotiate failed: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                # 3. Ждём READY
                msg = await asyncio.wait_for(ws.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"STT not ready, expected TEXT, got {msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                    
                data = msg.json()
                if data.get("type") != MSG_IN_STT_SERVERSIDE_READY:
                    _LOGGER.error(f"STT not ready: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                _LOGGER.info("STT server ready, sending audio")
                
                # 4. Отправляем аудио чанками
                async for chunk in stream:
                    if chunk:
                        await ws.send_bytes(chunk)
                
                # ✅ ВАЖНО: НЕ отправляем b"" в конце, это ломает протокол Ирины (ошибка 4500)
                _LOGGER.info("Audio stream finished, waiting for recognition result...")
                
                # 5. Ждём распознанный текст в цикле, корректно обрабатывая закрытие соединения
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            if data.get("type") == MSG_IN_STT_SERVERSIDE_RECOGNIZED:
                                text = data.get("text", "").strip()
                                _LOGGER.info(f"STT recognized: '{text}'")
                                return SpeechResult(text, SpeechResultState.SUCCESS)
                            else:
                                _LOGGER.debug(f"Received other STT message: {data}")
                                
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            _LOGGER.error(f"STT server closed connection. Code: {msg.data}, Extra: {msg.extra}")
                            return SpeechResult(None, SpeechResultState.ERROR)
                            
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                            _LOGGER.error(f"STT connection lost or error: {msg.type}")
                            return SpeechResult(None, SpeechResultState.ERROR)
                            
                except asyncio.TimeoutError:
                    _LOGGER.error("STT timeout waiting for recognition result")
                    return SpeechResult(None, SpeechResultState.ERROR)
            
        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
