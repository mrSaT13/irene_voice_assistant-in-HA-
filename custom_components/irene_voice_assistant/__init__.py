# custom_components/irene_voice_assistant/__init__.py
"""Irene Voice Assistant integration for Home Assistant."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_SSL, CONF_NAME, Platform
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    DOMAIN,
    CONF_RETURN_FORMAT,
    DEFAULT_RETURN_FORMAT,
    DEFAULT_PORT,
    DEFAULT_NAME,
)
from .coordinator import IreneCoordinator, clean_host
from .services import async_setup_services, async_unload_services

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.CONVERSATION,
    Platform.NOTIFY,
    Platform.SENSOR,
]


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the Irene Voice Assistant component."""
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Irene Voice Assistant from a config entry."""
    
    host = clean_host(entry.data[CONF_HOST])
    port = entry.data.get(CONF_PORT, DEFAULT_PORT)
    use_ssl = entry.data.get(CONF_SSL, False)
    name = entry.data.get(CONF_NAME, DEFAULT_NAME)
    
    protocol = "https" if use_ssl else "http"
    base_url = f"{protocol}://{host}:{port}"
    
    _LOGGER.info(f"Setting up Irene at {base_url}")
    
    session = async_get_clientsession(hass, verify_ssl=False)
    
    coordinator = IreneCoordinator(
        hass=hass,
        session=session,
        base_url=base_url,
        name=name,
        return_format=entry.options.get(CONF_RETURN_FORMAT, DEFAULT_RETURN_FORMAT),
    )
    
    try:
        await coordinator.async_config_entry_first_refresh()
    except Exception as err:
        _LOGGER.error(f"Failed to connect to Irene: {err}")
        raise ConfigEntryNotReady(f"Failed to connect to Irene: {err}") from err
    
    hass.data[DOMAIN][entry.entry_id] = coordinator
    
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    
    async_setup_services(hass)
    
    # ✅ Запускаем WebSocket подключение и слушатель событий
    hass.async_create_task(coordinator.ensure_websocket_connected())
    
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    
    _LOGGER.info(f"Irene '{name}' setup complete")
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info(f"Unloading Irene entry: {entry.entry_id}")
    
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    
    if unload_ok:
        coordinator: IreneCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        try:
            await coordinator.disconnect_websocket()
        except Exception as err:
            _LOGGER.warning(f"Error disconnecting: {err}")
        
        # Unload services only if no more coordinators
        coordinators = [v for v in hass.data[DOMAIN].values() if isinstance(v, IreneCoordinator)]
        if not coordinators:
            async_unload_services(hass)
            # Clear panel flag
            hass.data[DOMAIN].pop("_panel_registered", None)
    
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    await hass.config_entries.async_reload(entry.entry_id)