# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant."""

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
    
    async_add_entities([
        IreneTTSEntity(hass, coordinator, config_entry),
    ])
    
    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity."""
    
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
        """Load TTS from Irene."""
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")
            
            # Получаем URL WAV файла от Ирины через WebSocket
            audio_url = await self.coordinator._get_tts_audio_url(message, timeout=15.0)
            
            if not audio_url:
                _LOGGER.warning("Failed to get TTS audio URL from Irene")
                return None, None
            
            # Формируем полный URL
            full_url = f"{self.coordinator.base_url}{audio_url}"
            _LOGGER.info(f"Downloading TTS audio from: {full_url}")
            
            # Скачиваем WAV файл
            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.get(full_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    audio_bytes = await response.read()
                    _LOGGER.info(f"TTS audio downloaded: {len(audio_bytes)} bytes")
                    return "wav", audio_bytes
                else:
                    _LOGGER.error(f"Failed to download TTS audio: HTTP {response.status}")
                    return None, None
            
        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None
