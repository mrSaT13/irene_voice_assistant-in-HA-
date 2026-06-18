# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT via WebSocket."""

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
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneSTTEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT via WebSocket."""

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
        try:
            _LOGGER.info("Starting STT processing")

            await self.coordinator.ensure_websocket_connected()

            if "in.stt.serverside" not in self.coordinator.agreed_protocols:
                _LOGGER.warning("STT protocol not agreed by server")
                return SpeechResult(None, SpeechResultState.ERROR)

            if not self.coordinator._stt_serverside_path:
                try:
                    await asyncio.wait_for(
                        self.coordinator._stt_session_ready.wait(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.error("STT server-side not ready")
                    return SpeechResult(None, SpeechResultState.ERROR)

            if not self.coordinator._stt_serverside_path:
                _LOGGER.error("STT path not received")
                return SpeechResult(None, SpeechResultState.ERROR)

            # ✅ ИСПРАВЛЕНО: добавляем ?sample_rate=16000
            # Без этого параметра сервер думает, что мы шлём 44100 Гц, и выдаёт "кашу"
            stt_ws_url = f"{self.coordinator.ws_base_url}{self.coordinator._stt_serverside_path}?sample_rate=16000"
            _LOGGER.info(f"Connecting to STT WS: {stt_ws_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False

            # Подготавливаем future ДО отправки аудио (чтобы не пропустить ответ)
            await self.coordinator.prepare_stt_result()

            async with session.ws_connect(stt_ws_url, timeout=15.0, ssl=ssl_ctx) as stt_ws:
                _LOGGER.info("STT WS connected, sending audio")

                chunk_count = 0
                async for chunk in stream:
                    # ✅ ИСПРАВЛЕНО: если Ирина сама говорит (играет TTS), она шлёт in.mute/mute.
                    # Мы не должны отправлять звук с микрофона, чтобы не было эха!
                    if self.coordinator.is_muted:
                        continue

                    if chunk:
                        await stt_ws.send_bytes(chunk)
                        chunk_count += 1

                _LOGGER.info(f"Sent {chunk_count} audio chunks")
                await stt_ws.send_bytes(b"")

                text = await self.coordinator.wait_stt_result(timeout=20.0)

                if text is not None and len(text.strip()) > 0:
                    _LOGGER.info(f"STT recognized: '{text}'")
                    return SpeechResult(text, SpeechResultState.SUCCESS)
                else:
                    _LOGGER.warning("STT no text recognized")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
