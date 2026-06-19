# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant using simple HTTP endpoint."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from homeassistant.components.tts import TextToSpeechEntity
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
    """Set up TTS platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneTTSEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity using simple /ttsWav endpoint.

    ✅ Простой и надёжный путь: GET /ttsWav?text=<сообщение> → сервер Ирины
    сразу отдаёт WAV в response body. Без WebSocket, без playback-request,
    без 404, без гонок. Возвращает ("wav", bytes) для HA-платформы tts.
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
        """Load TTS from Irene using /ttsWav endpoint."""
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")

            # ✅ Простой HTTP endpoint /ttsWav — сервер сам генерит WAV
            # и сразу отдаёт байты. Без WebSocket и без гонок с файлами.
            url = f"{self.coordinator.base_url}/ttsWav?text={message}"
            _LOGGER.info(f"Downloading TTS audio from: {url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    audio_bytes = await response.read()
                    _LOGGER.info(f"TTS audio downloaded successfully: {len(audio_bytes)} bytes")
                    return "wav", audio_bytes
                else:
                    error_text = await response.text()
                    _LOGGER.error(f"Failed to download TTS audio: HTTP {response.status}. Response: {error_text[:200]}")
                    return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None
