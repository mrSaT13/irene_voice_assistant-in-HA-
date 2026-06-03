# custom_components/irene_voice_assistant/config_flow.py
"""Config flow for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_NAME, CONF_PORT, CONF_SSL
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    API_CONFIGS,
    CONF_RETURN_FORMAT,
    CONF_REFRESH_INTERVAL,
    DEFAULT_PORT,
    DEFAULT_RETURN_FORMAT,
    DEFAULT_NAME,
    DEFAULT_REFRESH_INTERVAL,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)


def clean_host(host: str) -> str:
    """Clean host from protocol and slashes."""
    host = host.strip()
    if host.startswith("https://"):
        host = host[8:]
    elif host.startswith("http://"):
        host = host[7:]
    host = host.rstrip("/")
    if ":" in host:
        host = host.split(":")[0]
    return host


async def _test_connection(hass: HomeAssistant, host: str, port: int, use_ssl: bool) -> bool:
    """Test connection to Irene."""
    clean = clean_host(host)
    protocol = "https" if use_ssl else "http"
    url = f"{protocol}://{clean}:{port}{API_CONFIGS}"
    
    _LOGGER.info(f"Testing connection to: {url}")
    
    session = async_get_clientsession(hass, verify_ssl=False)
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                _LOGGER.info(f"Successfully connected to Irene at {url}")
                return True
            else:
                _LOGGER.warning(f"Connection test returned status {response.status}")
                return False
    except Exception as err:
        _LOGGER.error(f"Connection test failed: {err}")
        return False


class IreneVoiceAssistantConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Irene Voice Assistant."""
    
    VERSION = 1
    
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ):
        """Handle the initial step."""
        errors = {}
        
        if user_input is not None:
            try:
                success = await _test_connection(
                    self.hass,
                    host=user_input[CONF_HOST],
                    port=user_input[CONF_PORT],
                    use_ssl=user_input.get(CONF_SSL, False),
                )
                
                if success:
                    user_input[CONF_HOST] = clean_host(user_input[CONF_HOST])
                    
                    return self.async_create_entry(
                        title=user_input[CONF_NAME],
                        data=user_input,
                        options={
                            CONF_RETURN_FORMAT: DEFAULT_RETURN_FORMAT,
                            CONF_REFRESH_INTERVAL: DEFAULT_REFRESH_INTERVAL,
                        },
                    )
                else:
                    errors["base"] = "cannot_connect"
                    
            except Exception as err:
                _LOGGER.exception(f"Unexpected exception: {err}")
                errors["base"] = "unknown"
        
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_HOST, default="localhost"): str,
                    vol.Required(CONF_PORT, default=DEFAULT_PORT): int,
                    vol.Optional(CONF_SSL, default=False): bool,
                    vol.Required(CONF_NAME, default=DEFAULT_NAME): str,
                }
            ),
            errors=errors,
        )
    
    @staticmethod
    def async_get_options_flow(config_entry):
        """Get options flow."""
        return IreneOptionsFlow(config_entry)


class IreneOptionsFlow(config_entries.OptionsFlow):
    """Handle options flow."""
    
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry
    
    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ):
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_RETURN_FORMAT,
                        default=self.config_entry.options.get(
                            CONF_RETURN_FORMAT, DEFAULT_RETURN_FORMAT
                        ),
                    ): vol.In({
                        "text": "Текст",
                        "audio": "Аудио",
                    }),
                    vol.Optional(
                        CONF_REFRESH_INTERVAL,
                        default=self.config_entry.options.get(
                            CONF_REFRESH_INTERVAL, DEFAULT_REFRESH_INTERVAL
                        ),
                    ): vol.All(vol.Coerce(int), vol.Range(min=30, max=3600)),
                }
            ),
            description_placeholders={
                "interval_hint": "Интервал обновления статуса Ирины (в секундах)",
            },
        )