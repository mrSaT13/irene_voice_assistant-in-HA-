# custom_components/irene_voice_assistant/coordinator.py
"""Data coordinator for Irene Voice Assistant."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class IreneCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator to manage fetching data from Irene."""
    
    def __init__(
        self,
        hass: HomeAssistant,
        session: aiohttp.ClientSession,
        base_url: str,
        name: str,
        return_format: str = "saywav",
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"Irene Voice Assistant ({name})",
            update_interval=timedelta(seconds=30),
        )
        
        self.session = session
        self.base_url = base_url
        self.name = name
        self.return_format = return_format
        
        # Chat history
        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100
        
        # WebSocket connection
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        
    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from Irene."""
        try:
            # Check if Irene is alive
            async with self.session.get(
                f"{self.base_url}/",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                if response.status == 200:
                    return {
                        "available": True,
                        "last_update": dt_util.utcnow(),
                    }
        except asyncio.TimeoutError:
            pass
        except aiohttp.ClientError:
            pass
        
        raise UpdateFailed("Failed to communicate with Irene")
    
    async def send_text_command(self, text: str) -> str:
        """Send text command to Irene and get response."""
        try:
            url = f"{self.base_url}/sendTxtCmd"
            params = {
                "cmd": text,
                "returnFormat": self.return_format,
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.text()
                    
                    # Add to history
                    self._add_to_history("user", text)
                    self._add_to_history("assistant", result)
                    
                    return result
                else:
                    raise UpdateFailed(f"API error: {response.status}")
                    
        except Exception as err:
            _LOGGER.error(f"Error sending command: {err}")
            raise
    
    async def send_raw_text(self, text: str) -> dict[str, Any]:
        """Send raw text (may include assistant name)."""
        try:
            url = f"{self.base_url}/sendRawTxt"
            params = {
                "rawtxt": text,
                "returnFormat": self.return_format,
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    # Try to parse as JSON first
                    content_type = response.headers.get("Content-Type", "")
                    if "application/json" in content_type:
                        result = await response.json()
                    else:
                        result = await response.text()
                    
                    # Add to history
                    self._add_to_history("user", text)
                    self._add_to_history("assistant", str(result))
                    
                    return result
                else:
                    raise UpdateFailed(f"API error: {response.status}")
                    
        except Exception as err:
            _LOGGER.error(f"Error sending raw text: {err}")
            raise
    
    async def tts_say(self, text: str) -> None:
        """Make Irene say text."""
        try:
            url = f"{self.base_url}/ttsSay"
            params = {"text": text}
            
            async with self.session.get(url, params=params):
                pass  # Ignore response
                
        except Exception as err:
            _LOGGER.error(f"Error in TTS say: {err}")
            raise
    
    async def get_tts_wav(self, text: str) -> bytes:
        """Get TTS audio as WAV bytes."""
        try:
            url = f"{self.base_url}/ttsWav"
            params = {"text": text}
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    raise UpdateFailed(f"TTS error: {response.status}")
                    
        except Exception as err:
            _LOGGER.error(f"Error getting TTS: {err}")
            raise
    
    async def connect_websocket(self) -> None:
        """Establish WebSocket connection for real-time communication."""
        if self.ws_connected:
            return
        
        try:
            ws_url = f"{self.base_url.replace('http', 'ws').replace('https', 'wss')}/wsrawtext"
            self.ws_connection = await self.session.ws_connect(ws_url)
            self.ws_connected = True
            _LOGGER.info("WebSocket connected to Irene")
        except Exception as err:
            _LOGGER.error(f"Failed to connect WebSocket: {err}")
            self.ws_connected = False
            raise
    
    async def disconnect_websocket(self) -> None:
        """Disconnect WebSocket."""
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
            self.ws_connected = False
            _LOGGER.info("WebSocket disconnected")
    
    async def send_websocket_command(self, text: str) -> None:
        """Send command via WebSocket."""
        if not self.ws_connected:
            await self.connect_websocket()
        
        try:
            import json
            message = {
                "txt": text,
                "returnFormat": self.return_format,
            }
            await self.ws_connection.send_json(message)
        except Exception as err:
            _LOGGER.error(f"Error sending WebSocket message: {err}")
            raise
    
    def _add_to_history(self, role: str, content: str) -> None:
        """Add message to chat history."""
        self.chat_history.append({
            "role": role,
            "content": content,
            "timestamp": dt_util.utcnow().isoformat(),
        })
        
        # Trim history if needed
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history:]
    
    def get_chat_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get chat history."""
        return self.chat_history[-limit:]
    
    def clear_history(self) -> None:
        """Clear chat history."""
        self.chat_history = []