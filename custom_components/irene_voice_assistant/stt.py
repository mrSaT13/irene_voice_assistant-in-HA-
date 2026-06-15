# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using simple WebSocket."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterable

import aiohttp

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

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up STT platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneSTTEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using simple WebSocket /wsmic."""

    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"

    @property
    def supported_languages(self) -> list[str]:
        return ["ru", "en"]

    @property
    def supported_formats(self) -> list[AudioFormats]:
        return [AudioFormats.WAV]

    @property
    def supported_codecs(self) -> list[AudioCodecs]:
        return [AudioCodecs.PCM]

    @property
    def supported_bit_rates(self) -> list[AudioBitRates]:
        return [AudioBitRates.BITRATE_16]

    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]:
        return [AudioSampleRates.SAMPLERATE_16000]

    @property
    def supported_channels(self) -> list[AudioChannels]:
        return [AudioChannels.CHANNEL_MONO]

    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream using simple /wsmic WebSocket."""
        try:
            # Используем простой endpoint /wsmic
            ws_url = f"{self.coordinator.ws_base_url}/wsmic"
            _LOGGER.info(f"🎤 [STT] Connecting to: {ws_url}")
            
            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False

            async with session.ws_connect(ws_url, timeout=15.0, ssl=ssl_ctx) as ws:
                _LOGGER.info("🎤 [STT] Connected, sending audio...")
                
                # Отправляем аудио-чанки
                chunk_count = 0
                async for chunk in stream:
                    if chunk:
                        await ws.send_bytes(chunk)
                        chunk_count += 1

                _LOGGER.info(f"🎤 [STT] Sent {chunk_count} chunks. Sending EOF...")
                
                # Отправляем сигнал конца аудио
                await ws.send_str('{"eof" : 1}')

                # Ждём результат
                _LOGGER.info("🎤 [STT] Waiting for recognition result...")
                msg = await asyncio.wait_for(ws.receive(), timeout=30.0)
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    import json
                    data = json.loads(msg.data)
                    text = data.get("text", "").strip()
                    
                    if text:
                        _LOGGER.info(f"✅ [STT] Recognized: '{text}'")
                        return SpeechResult(text, SpeechResultState.SUCCESS)
                    else:
                        _LOGGER.warning(f"⚠️ [STT] Empty recognition result: {data}")
                        return SpeechResult(None, SpeechResultState.NO_SPEECH_FOUND)
                else:
                    _LOGGER.error(f"❌ [STT] Unexpected message type: {msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except asyncio.TimeoutError:
            _LOGGER.error("❌ [STT] Timeout waiting for recognition")
            return SpeechResult(None, SpeechResultState.ERROR)
        except Exception as err:
            _LOGGER.error(f"❌ [STT] Error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
