# custom_components/irene_voice_assistant/notify.py
"""Notify platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.notify import (
    ATTR_TITLE,
    BaseNotificationService,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_get_service(
    hass: HomeAssistant,
    config: Any,
    discovery_info: Any = None,
) -> BaseNotificationService | None:
    """Get the Irene notification service."""
    if discovery_info is None:
        return None
    
    coordinator: IreneCoordinator = hass.data[DOMAIN][discovery_info["entry_id"]]
    
    return IreneNotificationService(coordinator)


class IreneNotificationService(BaseNotificationService):
    """Implement notification service for Irene."""
    
    def __init__(self, coordinator: IreneCoordinator) -> None:
        """Initialize the service."""
        self.coordinator = coordinator
    
    async def async_send_message(
        self,
        message: str = "",
        title: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Send a notification via Irene TTS."""
        text_to_say = f"{title}: {message}" if title else message
        
        try:
            await self.coordinator.tts_say(text_to_say)
            _LOGGER.info(f"TTS message sent: {text_to_say}")
        except Exception as err:
            _LOGGER.error(f"Error sending TTS notification: {err}")