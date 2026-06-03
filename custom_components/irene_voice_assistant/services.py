# custom_components/irene_voice_assistant/services.py
"""Services for Irene Voice Assistant."""

from __future__ import annotations

import logging

import voluptuous as vol
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


def _get_coordinator(hass: HomeAssistant, entry_id: str | None) -> IreneCoordinator:
    """Get coordinator by entry_id or first available."""
    if entry_id:
        if entry_id not in hass.data[DOMAIN]:
            raise HomeAssistantError(f"Irene entry {entry_id} not found")
        return hass.data[DOMAIN][entry_id]
    
    coordinators = [v for v in hass.data[DOMAIN].values() if isinstance(v, IreneCoordinator)]
    if not coordinators:
        raise HomeAssistantError("No Irene Voice Assistant configured")
    return coordinators[0]


def async_setup_services(hass: HomeAssistant) -> None:
    """Set up services for Irene."""
    
    async def handle_send_command(call: ServiceCall) -> None:
        """Handle send command service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        await coordinator.send_text_command(call.data["command"])
    
    async def handle_tts_say(call: ServiceCall) -> None:
        """Handle TTS say service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        await coordinator.tts_say(call.data["text"])
    
    async def handle_get_chat_history(call: ServiceCall) -> dict:
        """Handle get chat history service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        return {"history": coordinator.get_chat_history(call.data.get("limit", 50))}
    
    async def handle_clear_chat_history(call: ServiceCall) -> None:
        """Handle clear chat history service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        coordinator.clear_history()
    
    async def handle_get_configs(call: ServiceCall) -> dict:
        """Handle get configs service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        configs = await coordinator.get_configs()
        return {"configs": configs}
    
    async def handle_get_plugins(call: ServiceCall) -> dict:
        """Handle get plugins service."""
        coordinator = _get_coordinator(hass, call.data.get("entry_id"))
        plugins = await coordinator.get_plugins()
        return {"plugins": plugins}
    
    # Register services
    hass.services.async_register(
        DOMAIN, "send_command", handle_send_command,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Required("command"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN, "tts_say", handle_tts_say,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Required("text"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN, "get_chat_history", handle_get_chat_history,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
            vol.Optional("limit", default=50): cv.positive_int,
        }),
    )
    
    hass.services.async_register(
        DOMAIN, "clear_chat_history", handle_clear_chat_history,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN, "get_configs", handle_get_configs,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN, "get_plugins", handle_get_plugins,
        schema=vol.Schema({
            vol.Optional("entry_id"): cv.string,
        }),
    )


def async_unload_services(hass: HomeAssistant) -> None:
    """Unload Irene services."""
    for service in ["send_command", "tts_say", "get_chat_history", 
                    "clear_chat_history", "get_configs", "get_plugins"]:
        if hass.services.has_service(DOMAIN, service):
            hass.services.async_remove(DOMAIN, service)