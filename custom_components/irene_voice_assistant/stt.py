# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using serverside WebSocket."""
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
    """Irene STT entity using serverside WebSocket protocol."""

    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        """Initialize the STT entity."""
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"

    @property
    def supported_languages(self) -> list[str]:
        """Return the list of supported languages."""
        return ["ru", "en"]

    @property
    def supported_formats(self) -> list[AudioFormats]:
        """Return the list of supported formats."""
        return [AudioFormats.WAV]

    @property
    def supported_codecs(self) -> list[AudioCodecs]:
        """Return the list of supported codecs."""
        return [AudioCodecs.PCM]

    @property
    def supported_bit_rates(self) -> list[AudioBitRates]:
        """Return the list of supported bit rates."""
        return [AudioBitRates.BITRATE_16]

    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]:
        """Return the list of supported sample rates."""
        return [AudioSampleRates.SAMPLERATE_16000]

    @property
    def supported_channels(self) -> list[AudioChannels]:
        """Return the list of supported channels."""
        return [AudioChannels.CHANNEL_MONO]

    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream using serverside WebSocket protocol."""
        try:
            # 1. Ждем получения пути для STT от сервера
            _LOGGER.info("🎤 [STT] Waiting for STT ready path from server...")
            for _ in range(50):  # Ждем максимум 5 секунд
                if self.coordinator.stt_ready_path:
                    break
                await asyncio.sleep(0.1)
            
            if not self.coordinator.stt_ready_path:
                _LOGGER.error("❌ [STT] Server did not provide STT path. Is 'in.stt.serverside' protocol agreed?")
                return SpeechResult(None, SpeechResultState.ERROR)

            path = self.coordinator.stt_ready_path
            # ✅ Правильный URL для аудио потока согласно документации
            ws_url = f"{self.coordinator.ws_base_url}{path}?sample_rate=16000"
            _LOGGER.info(f"🎤 [STT] Connecting to audio stream: {ws_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False

            # 2. Создаем Future для ожидания результата распознавания
            loop = asyncio.get_event_loop()
            self.coordinator.stt_result_future = loop.create_future()

            # 3. Подключаемся и отправляем аудио
            async with session.ws_connect(ws_url, timeout=15.0, ssl=ssl_ctx) as ws:
                _LOGGER.info("🎤 [STT] Connected, sending audio chunks...")
                chunk_count = 0
                async for chunk in stream:
                    if chunk:
                        await ws.send_bytes(chunk)
                        chunk_count += 1
                _LOGGER.info(f"🎤 [STT] Sent {chunk_count} chunks. Waiting for result...")

                # 4. Ждем результат из основного WS (обработчик в coordinator.py)
                try:
                    text = await asyncio.wait_for(self.coordinator.stt_result_future, timeout=30.0)
                    if text:
                        _LOGGER.info(f"✅ [STT] Recognized: '{text}'")
                        return SpeechResult(text, SpeechResultState.SUCCESS)
                    else:
                        _LOGGER.warning("⚠️ [STT] Empty recognition result")
                        return SpeechResult(None, SpeechResultState.NO_SPEECH_FOUND)
                except asyncio.TimeoutError:
                    _LOGGER.error("❌ [STT] Timeout waiting for recognition result")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"❌ [STT] Error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
        finally:
            self.coordinator.stt_result_future = None
