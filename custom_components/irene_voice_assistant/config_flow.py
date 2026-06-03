# custom_components/irene_voice_assistant/config_flow.py
"""Config flow for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol
from aiohttp import ClientError
from homeassistant import config_entries
from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    CONF_PORT,
    CONF_SSL,
)
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_RETURN_FORMAT,
    DEFAULT_PORT,
    DEFAULT_RETURN_FORMAT,
    DEFAULT_NAME,
    DOMAIN,
    RETURN_FORMATS,
)

_LOGGER = logging.getLogger(__name__)


def _get_schema(step: str, user_input: dict | None = None) -> vol.Schema:
    """Get schema for config flow step."""
    if user_input is None:
        user_input = {}
    
    if step == "user":
        return vol.Schema(
            {
                vol.Required(
                    CONF_HOST,
                    default=user_input.get(CONF_HOST, ""),
                ): str,
                vol.Required(
                    CONF_PORT,
                    default=user_input.get(CONF_PORT, DEFAULT_PORT),
                ): int,
                vol.Optional(
                    CONF_SSL,
                    default=user_input.get(CONF_SSL, False),
                ): bool,
                vol.Required(
                    CONF_NAME,
                    default=user_input.get(CONF_NAME, DEFAULT_NAME),
                ): str,
            }
        )
    elif step == "options":
        return vol.Schema(
            {
                vol.Required(
                    CONF_RETURN_FORMAT,
                    default=user_input.get(CONF_RETURN_FORMAT, DEFAULT_RETURN_FORMAT),
                ): vol.In(RETURN_FORMATS),
            }
        )
    
    return vol.Schema({})


class IreneVoiceAssistantConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Irene Voice Assistant."""
    
    VERSION = 1
    
    def __init__(self) -> None:
        """Initialize the flow."""
        self._errors: dict[str, str] = {}
    
    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}
        
        if user_input is not None:
            # Validate connection
            try:
                await self._async_test_connection(
                    host=user_input[CONF_HOST],
                    port=user_input[CONF_PORT],
                    use_ssl=user_input[CONF_SSL],
                )
                
                # Create entry
                return self.async_create_entry(
                    title=user_input[CONF_NAME],
                    data=user_input,
                    options={
                        CONF_RETURN_FORMAT: DEFAULT_RETURN_FORMAT,
                    },
                )
                
            except ClientError:
                errors["base"] = "cannot_connect"
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
        
        return self.async_show_form(
            step_id="user",
            data_schema=_get_schema("user", user_input),
            errors=errors,
        )
    
    async def async_step_import(self, import_config: dict) -> FlowResult:
        """Import a config entry from configuration."""
        return await self.async_step_user(import_config)
    
    async def _async_test_connection(
        self,
        host: str,
        port: int,
        use_ssl: bool,
    ) -> None:
        """Test connection to Irene."""
        protocol = "https" if use_ssl else "http"
        url = f"{protocol}://{host}:{port}/"
        
        session = async_get_clientsession(self.hass, verify_ssl=False)
        
        try:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    raise ClientError(f"HTTP {response.status}")
        finally:
            await session.close()


class IreneOptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for Irene."""
    
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry
    
    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> FlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        
        return self.async_show_form(
            step_id="init",
            data_schema=_get_schema("options", self.config_entry.options),
        )


async def async_get_options_flow(
    config_entry: config_entries.ConfigEntry,
) -> IreneOptionsFlow:
    """Get the options flow for this handler."""
    return IreneOptionsFlow(config_entry)