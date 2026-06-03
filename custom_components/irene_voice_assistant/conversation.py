# custom_components/irene_voice_assistant/conversation.py
"""Conversation platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components import conversation
from homeassistant.components.conversation.const import DOMAIN as CONVERSATION_DOMAIN
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.core import (
    HomeAssistant,
    IntentResponse,
    ServiceResponse,
    SupportsResponse,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import intent

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: Any,
) -> None:
    """Set up conversation platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    
    async def async_process(
        user_input: conversation.ConversationInput,
    ) -> conversation.ConversationResult:
        """Process user input."""
        try:
            # Send to Irene
            response_text = await coordinator.send_text_command(user_input.text)
            
            # Create response
            intent_response = IntentResponse(language=user_input.language)
            intent_response.async_set_speech(response_text)
            
            return conversation.ConversationResult(
                response=intent_response,
                conversation_id=user_input.conversation_id,
            )
            
        except Exception as err:
            _LOGGER.error(f"Error processing conversation: {err}")
            raise HomeAssistantError(f"Error communicating with Irene: {err}") from err
    
    # Register as conversation agent
    conversation.async_set_agent(
        hass,
        config_entry,
        async_process,
    )


async def async_unload_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
) -> None:
    """Unload conversation platform."""
    conversation.async_unset_agent(hass, config_entry)