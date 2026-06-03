# custom_components/irene_voice_assistant/notify.py
"""Notify platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.notify import NotifyEntity
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
    """Set up notify entities."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    
    async_add_entities([
        IreneNotifyEntity(coordinator, config_entry),
    ])


class IreneNotifyEntity(NotifyEntity):
    """Implement notification entity for Irene."""
    
    def __init__(
        self,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the entity."""
        self.coordinator = coordinator
        self._attr_name = coordinator.name
        self._attr_unique_id = f"{config_entry.entry_id}_notify"
        # ✅ Убрали _attr_supported_features — он не обязателен для TTS
    
    async def async_send_message(self, message: str, title: str | None = None, **kwargs: Any) -> None:
        """Send a notification via Irene TTS."""
        text_to_say = f"{title}: {message}" if title else message
        
        try:
            await self.coordinator.tts_say(text_to_say)
            _LOGGER.info(f"TTS message sent: {text_to_say}")
        except Exception as err:
            _LOGGER.error(f"Error sending TTS notification: {err}")