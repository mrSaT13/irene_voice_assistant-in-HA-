# custom_components/irene_voice_assistant/conversation.py
"""Conversation platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.core import HomeAssistant
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
    
    agent = IreneConversationAgent(hass, coordinator)
    conversation.async_set_agent(hass, config_entry, agent)


async def async_unload_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
) -> None:
    """Unload conversation platform."""
    conversation.async_unset_agent(hass, config_entry)


class IreneConversationAgent(conversation.AbstractConversationAgent):
    """Irene conversation agent."""
    
    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator) -> None:
        """Initialize the agent."""
        self.hass = hass
        self.coordinator = coordinator
    
    @property
    def supported_languages(self) -> list[str]:
        """Return supported languages."""
        return [MATCH_ALL, "ru"]
    
    async def async_process(
        self,
        user_input: conversation.ConversationInput,
    ) -> conversation.ConversationResult:
        """Process user input."""
        try:
            _LOGGER.debug(f"Processing conversation input: {user_input.text}")
            
            # Send to Irene
            response_text = await self.coordinator.send_text_command(user_input.text)
            
            # Create response
            intent_response = intent.IntentResponse(language=user_input.language)
            intent_response.async_set_speech(response_text)
            
            return conversation.ConversationResult(
                response=intent_response,
                conversation_id=user_input.conversation_id,
            )
            
        except Exception as err:
            _LOGGER.error(f"Error processing conversation: {err}")
            intent_response = intent.IntentResponse(language=user_input.language)
            intent_response.async_set_speech(f"Ошибка связи с Ириной: {err}")
            return conversation.ConversationResult(
                response=intent_response,
                conversation_id=user_input.conversation_id,
            )