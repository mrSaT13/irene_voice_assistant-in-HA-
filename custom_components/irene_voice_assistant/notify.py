# custom_components/irene_voice_assistant/notify.py
"""Notify platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.notify import NotifyEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
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
    
    # ✅ Слушаем события от Irene и создаем HA уведомления
    @callback
    def _handle_irene_message(event):
        """Handle messages from Irene."""
        data = event.data
        msg_type = data.get("type", "")
        content = data.get("content", "")
        alt_text = data.get("alt_text", "")
        
        if msg_type == "text" and content:
            _LOGGER.info(f"Irene message -> HA notification: {content}")
            # Создаем HA событие для уведомлений
            hass.bus.async_fire(
                "irene_notification",
                {
                    "message": content,
                    "title": "Irene",
                }
            )
        elif msg_type == "audio":
            _LOGGER.info(f"Irene audio message: {alt_text or data.get('url', '')}")
            hass.bus.async_fire(
                "irene_notification",
                {
                    "message": alt_text or "[Аудио сообщение]",
                    "title": "Irene",
                    "type": "audio",
                }
            )
    
    # Регистрируем слушатель
    config_entry.async_on_unload(
        hass.bus.async_listen(f"{DOMAIN}_message", _handle_irene_message)
    )


class IreneNotifyEntity(NotifyEntity):
    """Implement notification entity for Irene."""
    
    def __init__(
        self,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize."""
        self.coordinator = coordinator
        self._attr_name = coordinator.name
        self._attr_unique_id = f"{config_entry.entry_id}_notify"
        self._attr_icon = "mdi:robot"
    
    async def async_send_message(self, message: str, title: str | None = None, **kwargs: Any) -> None:
        """Send notification via Irene TTS."""
        text_to_say = f"{title}: {message}" if title else message
        
        try:
            await self.coordinator.tts_say(text_to_say)
            _LOGGER.info(f"TTS message sent: {text_to_say}")
        except Exception as err:
            _LOGGER.error(f"Error sending TTS notification: {err}")