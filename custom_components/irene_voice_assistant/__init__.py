# custom_components/irene_voice_assistant/__init__.py
"""Irene Voice Assistant integration for Home Assistant."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_SSL,
    CONF_NAME,
    Platform,
)
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN,
    CONF_RETURN_FORMAT,
    DEFAULT_RETURN_FORMAT,
    DEFAULT_PORT,
    DEFAULT_NAME,
)
from .coordinator import IreneCoordinator
from .services import async_setup_services, async_unload_services

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.CONVERSATION,
    Platform.NOTIFY,
    Platform.SENSOR,
]


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the Irene Voice Assistant component."""
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Irene Voice Assistant from a config entry."""
    
    host = entry.data[CONF_HOST]
    port = entry.data.get(CONF_PORT, DEFAULT_PORT)
    use_ssl = entry.data.get(CONF_SSL, False)
    name = entry.data.get(CONF_NAME, DEFAULT_NAME)
    
    # Build base URL
    protocol = "https" if use_ssl else "http"
    base_url = f"{protocol}://{host}:{port}"
    
    _LOGGER.info(f"Setting up Irene Voice Assistant at {base_url}")
    
    # Create session
    session = async_get_clientsession(hass, verify_ssl=False)
    
    # Create coordinator
    coordinator = IreneCoordinator(
        hass=hass,
        session=session,
        base_url=base_url,
        name=name,
        return_format=entry.options.get(CONF_RETURN_FORMAT, DEFAULT_RETURN_FORMAT),
    )
    
    # Test connection
    try:
        await coordinator.async_config_entry_first_refresh()
        _LOGGER.info("Successfully connected to Irene Voice Assistant")
    except Exception as err:
        _LOGGER.error(f"Failed to connect to Irene: {err}")
        raise ConfigEntryNotReady(f"Failed to connect to Irene: {err}") from err
    
    # Store coordinator
    hass.data[DOMAIN][entry.entry_id] = coordinator
    
    # Setup platforms
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    # Register services
    async_setup_services(hass)
    
    # Register update listener
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    
    _LOGGER.info(f"Irene Voice Assistant '{name}' setup complete")
    
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info(f"Unloading Irene Voice Assistant '{entry.data.get(CONF_NAME, DEFAULT_NAME)}'")
    
    # Unload platforms
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    
    if unload_ok:
        # Remove coordinator
        coordinator: IreneCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        
        # Disconnect WebSocket if connected
        try:
            await coordinator.disconnect_websocket()
        except Exception as err:
            _LOGGER.warning(f"Error disconnecting WebSocket: {err}")
        
        # Unload services if no more entries
        if not hass.data[DOMAIN]:
            async_unload_services(hass)
    
    return unload_ok


async def async_migrate_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Migrate old entry."""
    _LOGGER.debug(f"Migrating from version {config_entry.version}")
    
    if config_entry.version > 1:
        return False
    
    if config_entry.version == 0:
        # Migrate from version 0 to 1
        new_data = {**config_entry.data}
        if CONF_NAME not in new_data:
            new_data[CONF_NAME] = DEFAULT_NAME
        
        config_entry.version = 1
        hass.config_entries.async_update_entry(config_entry, data=new_data)
        _LOGGER.info("Migration to version 1 successful")
    
    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(entry.entry_id)