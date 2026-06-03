# custom_components/irene_voice_assistant/services.py
"""Services for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


def async_setup_services(hass: HomeAssistant) -> None:
    """Set up services for Irene."""
    
    async def handle_send_command(call: ServiceCall) -> None:
        """Handle send command service."""
        entry_id = call.data.get("entry_id")
        command = call.data["command"]
        
        coordinator: IreneCoordinator = hass.data[DOMAIN][entry_id]
        await coordinator.send_text_command(command)
    
    async def handle_send_raw_text(call: ServiceCall) -> None:
        """Handle send raw text service."""
        entry_id = call.data.get("entry_id")
        text = call.data["text"]
        
        coordinator: IreneCoordinator = hass.data[DOMAIN][entry_id]
        await coordinator.send_raw_text(text)
    
    async def handle_tts_say(call: ServiceCall) -> None:
        """Handle TTS say service."""
        entry_id = call.data.get("entry_id")
        text = call.data["text"]
        
        coordinator: IreneCoordinator = hass.data[DOMAIN][entry_id]
        await coordinator.tts_say(text)
    
    async def handle_get_chat_history(call: ServiceCall) -> ServiceResponse:
        """Handle get chat history service."""
        entry_id = call.data.get("entry_id")
        limit = call.data.get("limit", 50)
        
        coordinator: IreneCoordinator = hass.data[DOMAIN][entry_id]
        history = coordinator.get_chat_history(limit)
        
        return {
            "history": history,
        }
    
    async def handle_clear_chat_history(call: ServiceCall) -> None:
        """Handle clear chat history service."""
        entry_id = call.data.get("entry_id")
        
        coordinator: IreneCoordinator = hass.data[DOMAIN][entry_id]
        coordinator.clear_history()
    
    # Register services
    hass.services.async_register(
        DOMAIN,
        "send_command",
        handle_send_command,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Required("command"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN,
        "send_raw_text",
        handle_send_raw_text,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Required("text"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN,
        "tts_say",
        handle_tts_say,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Required("text"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN,
        "get_chat_history",
        handle_get_chat_history,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Optional("limit", default=50): cv.positive_int,
        }),
        supports_response=SupportsResponse.ONLY,
    )
    
    hass.services.async_register(
        DOMAIN,
        "clear_chat_history",
        handle_clear_chat_history,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
        }),
    )


def async_unload_services(hass: HomeAssistant) -> None:
    """Unload Irene services."""
    hass.services.async_remove(DOMAIN, "send_command")
    hass.services.async_remove(DOMAIN, "send_raw_text")
    hass.services.async_remove(DOMAIN, "tts_say")
    hass.services.async_remove(DOMAIN, "get_chat_history")
    hass.services.async_remove(DOMAIN, "clear_chat_history")