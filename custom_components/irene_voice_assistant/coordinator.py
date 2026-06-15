"""Coordinator for Irene Voice Assistant."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import aiohttp

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class IreneCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Irene data."""

    def __init__(
        self,
        hass: HomeAssistant,
        name: str,
        base_url: str,
        ws_base_url: str,
        api_key: str | None = None,
        media_player_entity: str | None = None,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
        )
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.ws_base_url = ws_base_url
        self.api_key = api_key
        self.media_player_entity = media_player_entity
        self._ssl_context = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_lock = asyncio.Lock()
        self._history: list[dict[str, Any]] = []
        self._max_history = 50

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from API endpoint."""
        return {
            "name": self.name,
            "base_url": self.base_url,
            "connected": await self.test_connection(),
        }

    async def test_connection(self) -> bool:
        """Test connection to Irene."""
        try:
            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.get(
                f"{self.base_url}/",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                return response.status == 200
        except Exception as err:
            _LOGGER.error(f"Connection test failed: {err}")
            return False

    async def connect(self) -> bool:
        """Connect to Irene WebSocket."""
        async with self._ws_lock:
            if self._ws and not self._ws.closed:
                return True

            try:
                session = async_get_clientsession(self.hass, verify_ssl=False)
                ws_url = f"{self.ws_base_url}/api"
                
                self._ws = await session.ws_connect(
                    ws_url,
                    timeout=10.0,
                    ssl=self._ssl_context,
                )
                _LOGGER.info(f"✅ Connected to Irene WebSocket: {ws_url}")
                
                # Start message listener
                asyncio.create_task(self._listen_messages())
                return True

            except Exception as err:
                _LOGGER.error(f"❌ Failed to connect to Irene: {err}")
                return False

    async def disconnect(self) -> None:
        """Disconnect from Irene WebSocket."""
        async with self._ws_lock:
            if self._ws and not self._ws.closed:
                await self._ws.close()
                _LOGGER.info("Disconnected from Irene WebSocket")

    async def _listen_messages(self) -> None:
        """Listen for messages from Irene."""
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    _LOGGER.error(f"WebSocket error: {self._ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    _LOGGER.warning("WebSocket connection closed")
                    break
        except Exception as err:
            _LOGGER.error(f"Error in message listener: {err}", exc_info=True)

    async def _handle_message(self, message: str) -> None:
        """Handle incoming message from Irene."""
        try:
            data = json.loads(message)
            
            # Handle different message types
            if "rest_text" in data:
                # Assistant response
                text = data["rest_text"]
                _LOGGER.info(f"🤖 Assistant: {text}")
                self._add_to_history("assistant", text)
                
                # If media_player is configured, play TTS
                if self.media_player_entity:
                    await self.tts_to_media_player(text)
                    
            elif "stt" in data:
                # User speech recognized
                text = data["stt"]
                _LOGGER.info(f"🎤 User: {text}")
                self._add_to_history("user", text)

        except json.JSONDecodeError:
            _LOGGER.warning(f"Failed to parse message: {message}")
        except Exception as err:
            _LOGGER.error(f"Error handling message: {err}", exc_info=True)

    async def send_text(self, text: str) -> bool:
        """Send text command to Irene."""
        try:
            if not await self.connect():
                _LOGGER.error("Not connected to Irene")
                return False

            message = {"rest_text": text}
            await self._ws.send_str(json.dumps(message))
            _LOGGER.info(f"📤 Sent: {text}")
            self._add_to_history("user", text)
            return True

        except Exception as err:
            _LOGGER.error(f"Error sending text: {err}", exc_info=True)
            return False

    async def send_audio(self, audio_data: bytes) -> bool:
        """Send audio data to Irene."""
        try:
            if not await self.connect():
                _LOGGER.error("Not connected to Irene")
                return False

            await self._ws.send_bytes(audio_data)
            return True

        except Exception as err:
            _LOGGER.error(f"Error sending audio: {err}", exc_info=True)
            return False

    async def _get_tts_audio_url(self, message: str, timeout: float = 15.0) -> Optional[str]:
        """Получить URL WAV файла через простой HTTP endpoint /ttsWav.
        
        Returns:
            URL WAV файла (например, /ttsWav?text=привет)
            или None если не удалось получить.
        """
        try:
            # Используем простой HTTP endpoint /ttsWav
            url = f"{self.base_url}/ttsWav?text={message}"
            _LOGGER.info(f"TTS audio URL: {url}")
            return url
        except Exception as err:
            _LOGGER.error(f"Error getting TTS audio URL: {err}", exc_info=True)
            return None

    async def tts_say(self, message: str) -> bool:
        """Send TTS command to Irene server."""
        try:
            session = async_get_clientsession(self.hass, verify_ssl=False)
            url = f"{self.base_url}/tts?text={message}"
            
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    _LOGGER.info(f"✅ TTS sent to server: {message}")
                    self._add_to_history("assistant", f"[TTS] {message}")
                    return True
                else:
                    _LOGGER.error(f"❌ TTS failed: HTTP {response.status}")
                    return False

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return False

    async def tts_to_media_player(
        self,
        message: str,
        media_player_entity: str | None = None,
        timeout: float = 15.0,
    ) -> bool:
        """Озвучить текст на колонке через TTS Ирины.

        Использует простой HTTP endpoint /ttsWav и отправляет WAV на колонку
        через media_player.play_media.

        Returns:
            True если успешно, False если не удалось.
        """
        media_player = media_player_entity or self.media_player_entity

        if not media_player:
            _LOGGER.warning("No media_player configured for TTS")
            return False

        try:
            # 1. Получаем URL WAV файла от Ирины
            audio_url = await self._get_tts_audio_url(message, timeout=timeout)

            if not audio_url:
                _LOGGER.warning("Failed to get TTS audio URL, falling back to server TTS")
                await self.tts_say(message)
                return False

            # 2. Формируем полный URL
            full_url = audio_url if audio_url.startswith("http") else f"{self.base_url}{audio_url}"
            _LOGGER.info(f"TTS audio URL: {full_url}")

            # 3. Отправляем на колонку через media_player.play_media
            await self.hass.services.async_call(
                "media_player", "play_media",
                {
                    "entity_id": media_player,
                    "media_content_id": full_url,
                    "media_content_type": "audio/wav",
                },
                blocking=False,
            )

            _LOGGER.info(f"TTS sent to media_player: {media_player}")
            self._add_to_history("assistant", f"[TTS→{media_player}] {message}")
            return True

        except Exception as err:
            _LOGGER.error(f"TTS to media_player error: {err}", exc_info=True)
            # Fallback на серверную озвучку
            try:
                await self.tts_say(message)
            except Exception:
                pass
            return False

    def _add_to_history(self, role: str, text: str) -> None:
        """Add message to history."""
        import datetime
        self._history.append({
            "role": role,
            "text": text,
            "timestamp": datetime.datetime.now().isoformat(),
        })
        
        # Keep only last N messages
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_history(self) -> list[dict[str, Any]]:
        """Get conversation history."""
        return self._history.copy()

    def clear_history(self) -> None:
        """Clear conversation history."""
        self._history.clear()
        _LOGGER.info("History cleared")
