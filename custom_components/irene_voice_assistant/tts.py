# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import aiohttp

from homeassistant.components.tts import TextToSpeechEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, API_TTS_WAV
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up TTS platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]

    async_add_entities([
        IreneTTSEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity.

    Использует HTTP-эндпоинт /ttsWav сервера Ирины для получения WAV-аудио.
    """

    _attr_name = None

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the TTS entity."""
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_tts"
        self._attr_name = f"{coordinator.name} TTS"

    @property
    def default_language(self) -> str:
        """Return the default language."""
        return "ru"

    @property
    def supported_languages(self) -> list[str]:
        """Return the list of supported languages."""
        return ["ru", "en"]

    @property
    def supported_options(self) -> list[str]:
        """Return list of supported options."""
        return []

    async def async_get_tts_audio(
        self,
        message: str,
        language: str,
        options: dict[str, Any]
    ) -> tuple[str | None, bytes | None]:
        """Load TTS from Irene via HTTP /ttsWav endpoint."""
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            encoded_text = quote(message, safe="")
            url = f"{self.coordinator.base_url}{API_TTS_WAV}?text={encoded_text}"
            _LOGGER.info(f"TTS HTTP request: {url}")

            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 200:
                    content_type = response.headers.get("Content-Type", "")
                    audio_bytes = await response.read()

                    if len(audio_bytes) < 100:
                        _LOGGER.warning(
                            f"TTS response too small ({len(audio_bytes)} bytes), "
                            "likely not audio"
                        )
                        return None, None

                    _LOGGER.info(
                        f"TTS audio received: {len(audio_bytes)} bytes, "
                        f"content-type={content_type}"
                    )

                    if "wav" in content_type or audio_bytes[:4] == b"RIFF":
                        return "wav", audio_bytes
                    elif "mpeg" in content_type or audio_bytes[:2] == b'\xff\xfb':
                        return "mp3", audio_bytes
                    else:
                        return "wav", audio_bytes
                else:
                    body = await response.text()
                    _LOGGER.error(
                        f"TTS HTTP error: {response.status} - {body[:200]}"
                    )
                    return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None
