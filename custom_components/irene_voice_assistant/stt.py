# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterable

import aiohttp

from homeassistant.components.stt import (
    AudioBitRates, AudioChannels, AudioCodecs, AudioFormats, AudioSampleRates,
    SpeechMetadata, SpeechResult, SpeechResultState, SpeechToTextEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DOMAIN, API_WEBSOCKET, MSG_NEGOTIATE_REQUEST, MSG_NEGOTIATE_AGREE,
    MSG_IN_STT_SERVERSIDE_READY, MSG_IN_STT_SERVERSIDE_RECOGNIZED, PROTOCOL_IN_STT_SERVERSIDE,
)
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneSTTEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")

class IreneSTTEntity(SpeechToTextEntity):
    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"
    
    @property
    def supported_languages(self) -> list[str]: return ["ru", "en"]
    @property
    def supported_formats(self) -> list[AudioFormats]: return [AudioFormats.WAV]
    @property
    def supported_codecs(self) -> list[AudioCodecs]: return [AudioCodecs.PCM]
    @property
    def supported_bit_rates(self) -> list[AudioBitRates]: return [AudioBitRates.BITRATE_16]
    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]: return [AudioSampleRates.SAMPLERATE_16000]
    @property
    def supported_channels(self) -> list[AudioChannels]: return [AudioChannels.CHANNEL_MONO]

    async def async_process_audio_stream(self, metadata: SpeechMetadata, stream: AsyncIterable[bytes]) -> SpeechResult:
        try:
            _LOGGER.info("Starting STT processing (Stage 1: Main WebSocket)")
            ws_url = f"{self.coordinator.ws_base_url}{API_WEBSOCKET}"
            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False
            
            # 1. Подключаемся к ОСНОВНОМУ сокету для управления
            async with session.ws_connect(ws_url, timeout=15.0, ssl=ssl_ctx) as ws_main:
                await ws_main.send_json({
                    "type": MSG_NEGOTIATE_REQUEST,
                    "protocols": [[PROTOCOL_IN_STT_SERVERSIDE]],
                })
                
                msg = await asyncio.wait_for(ws_main.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT or msg.json().get("type") != MSG_NEGOTIATE_AGREE:
                    _LOGGER.error(f"STT negotiate failed: {msg}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                # 2. Ждем сообщение READY с путем для аудио
                msg = await asyncio.wait_for(ws_main.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                data = msg.json()
                if data.get("type") != MSG_IN_STT_SERVERSIDE_READY:
                    _LOGGER.error(f"STT not ready: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                path = data.get("path")
                if not path:
                    _LOGGER.error("STT READY message missing 'path' field")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                # 3. Подключаемся к НОВОМУ сокету для передачи аудио
                audio_ws_url = f"{self.coordinator.ws_base_url}{path}?sample_rate=16000"
                _LOGGER.info(f"Connecting to STT audio socket: {audio_ws_url}")
                
                ws_audio = await session.ws_connect(audio_ws_url, timeout=15.0, ssl=ssl_ctx)
                try:
                    _LOGGER.info("Sending audio stream...")
                    async for chunk in stream:
                        if chunk:
                            await ws_audio.send_bytes(chunk)
                finally:
                    # ✅ КРИТИЧЕСКИ ВАЖНО: Закрываем аудио-сокет!
                    # Сервер Ирины понимает, что речь закончилась, только когда сокет закрывается.
                    await ws_audio.close()
                    _LOGGER.info("Audio socket closed. Waiting for recognition result on main socket...")
                
                # 4. Ждем результат распознавания в ОСНОВНОМ сокете
                try:
                    while True:
                        msg = await asyncio.wait_for(ws_main.receive(), timeout=15.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            if data.get("type") == MSG_IN_STT_SERVERSIDE_RECOGNIZED:
                                text = data.get("text", "").strip()
                                _LOGGER.info(f"✅ STT recognized: '{text}'")
                                return SpeechResult(text, SpeechResultState.SUCCESS)
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            _LOGGER.error(f"Main socket closed while waiting for STT result: {msg.type}")
                            return SpeechResult(None, SpeechResultState.ERROR)
                except asyncio.TimeoutError:
                    _LOGGER.error("STT timeout waiting for recognition result")
                    return SpeechResult(None, SpeechResultState.ERROR)
            
        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
