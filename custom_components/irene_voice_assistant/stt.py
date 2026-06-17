# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using /api/willow HTTP endpoint."""

from __future__ import annotations

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

    async_add_entities([
        IreneSTTEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using /api/willow HTTP endpoint."""

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
        """Process audio stream via /api/willow HTTP endpoint."""
        try:
            _LOGGER.info("Starting STT processing via /api/willow")

            audio_data = b""
            async for chunk in stream:
                if chunk:
                    audio_data += chunk

            _LOGGER.info(f"Collected {len(audio_data)} bytes of audio")

            if len(audio_data) == 0:
                _LOGGER.warning("No audio data received")
                return SpeechResult(None, SpeechResultState.ERROR)

            url = f"{self.coordinator.base_url}/api/willow"
            headers = {
                "x-audio-sample-rate": "16000",
                "x-audio-channel": "1",
                "x-audio-bits": "16",
                "x-audio-codec": "pcm",
                "Content-Type": "multipart/form-data",
            }

            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.post(
                url,
                headers=headers,
                data=audio_data,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status == 200:
                    text = (await response.text()).strip().strip('"')
                    _LOGGER.info(f"STT recognized: '{text}'")
                    if len(text) > 0:
                        return SpeechResult(text, SpeechResultState.SUCCESS)
                    return SpeechResult(None, SpeechResultState.ERROR)
                else:
                    _LOGGER.error(f"STT error: HTTP {response.status}")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
