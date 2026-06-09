# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.tts import TextToSpeechEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
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
        IreneTTSEntity(coordinator, config_entry),
    ])
    
    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity."""
    
    _attr_name = None
    
    def __init__(
        self,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the TTS entity."""
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
        return ["voice", "speed"]
    
    async def async_get_tts_audio(
        self, 
        message: str, 
        language: str, 
        options: dict[str, Any]
    ) -> tuple[str | None, bytes | None]:
        """Load TTS from Irene."""
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")
            
            # Отправляем текст на сервер Ирины для озвучки
            await self.coordinator.tts_say(message)
            
            # Возвращаем пустой WAV чтобы HA не ругался
            return "wav", b""
            
        except Exception as err:
            _LOGGER.error(f"TTS error: {err}")
            return None, None