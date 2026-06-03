# custom_components/irene_voice_assistant/coordinator.py
"""Data coordinator for Irene Voice Assistant."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
from datetime import timedelta
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    API_CONFIGS,
    API_NOTIFY,
    API_WEBSOCKET,
    MSG_NEGOTIATE_AGREE,
    MSG_NEGOTIATE_REQUEST,
    MSG_IN_TEXT_DIRECT_TEXT,
    MSG_OUT_TEXT_PLAIN_TEXT,
    MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST,
    MSG_OUT_AUDIO_LINK_PLAYBACK_PROGRESS,
    MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
    PROTOCOL_IN_TEXT_DIRECT,
    PROTOCOL_OUT_TEXT_PLAIN,
    PROTOCOL_OUT_AUDIO_LINK,
    PROTOCOL_OUT_TTS_SERVERSIDE,
)

_LOGGER = logging.getLogger(__name__)


def clean_host(host: str) -> str:
    """Убирает протокол и слеши из host."""
    host = host.strip()
    # Убираем протокол если есть
    host = re.sub(r'^https?://', '', host)
    # Убираем trailing slash
    host = host.rstrip('/')
    # Убираем port если указан (он будет добавлен отдельно)
    if ':' in host:
        host = host.split(':')[0]
    return host


class IreneCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator to manage fetching data from Irene."""
    
    def __init__(
        self,
        hass: HomeAssistant,
        session: aiohttp.ClientSession,
        base_url: str,
        name: str,
        return_format: str = "text",
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"Irene Voice Assistant ({name})",
            update_interval=timedelta(seconds=30),
        )
        
        self.hass = hass
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.return_format = return_format
        
        # Определяем WS URL
        self.ws_base_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        
        # Chat history
        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100
        
        # WebSocket connection
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.ws_lock = asyncio.Lock()
        self.agreed_protocols: list[str] = []
        
        # Pending responses
        self._pending_responses: dict[str, asyncio.Future] = {}
        self._response_counter = 0
        self._ws_listener_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        
        # Reconnect settings
        self._reconnect_delay = 5
        self._max_reconnect_delay = 60
        self._current_reconnect_delay = self._reconnect_delay
        
        # SSL context for self-signed certs
        self._ssl_context: ssl.SSLContext | bool = False
        if self.base_url.startswith("https://"):
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
    
    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from Irene."""
        try:
            url = f"{self.base_url}{API_CONFIGS}"
            async with self.session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    configs = await response.json()
                    # Проверяем WebSocket
                    ws_status = "connected" if self.ws_connected else "disconnected"
                    return {
                        "available": True,
                        "last_update": dt_util.utcnow(),
                        "configs_count": len(configs) if isinstance(configs, list) else 0,
                        "ws_status": ws_status,
                        "agreed_protocols": self.agreed_protocols,
                    }
                else:
                    _LOGGER.warning(f"API returned status {response.status}")
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout connecting to Irene")
        except aiohttp.ClientError as err:
            _LOGGER.warning(f"Client error: {err}")
        except Exception as err:
            _LOGGER.error(f"Unexpected error: {err}")
        
        raise UpdateFailed("Failed to communicate with Irene")
    
    async def ensure_websocket_connected(self) -> None:
        """Ensure WebSocket is connected and protocols are negotiated."""
        async with self.ws_lock:
            if self.ws_connected and self.ws_connection and not self.ws_connection.closed:
                return
            
            try:
                ws_url = f"{self.ws_base_url}{API_WEBSOCKET}"
                
                _LOGGER.info(f"Connecting to WebSocket: {ws_url}")
                
                self.ws_connection = await self.session.ws_connect(
                    ws_url,
                    timeout=15.0,
                    ssl=self._ssl_context if self._ssl_context else False,
                    heartbeat=30.0,  # ✅ Heartbeat для keep-alive
                    autoping=True,
                )
                
                # Negotiate protocols
                await self._negotiate_protocols()
                
                # Start listener
                if self._ws_listener_task is None or self._ws_listener_task.done():
                    self._ws_listener_task = self.hass.async_create_task(
                        self._ws_listener()
                    )
                
                self.ws_connected = True
                self._current_reconnect_delay = self._reconnect_delay
                _LOGGER.info(f"WebSocket connected. Agreed protocols: {self.agreed_protocols}")
                
            except Exception as err:
                _LOGGER.error(f"Failed to connect WebSocket: {err}", exc_info=True)
                self.ws_connected = False
                raise
    
    async def _negotiate_protocols(self) -> None:
        """Negotiate protocols with the server."""
        # По документации: каждый элемент - массив альтернатив
        negotiate_msg = {
            "type": MSG_NEGOTIATE_REQUEST,
            "protocols": [
                [PROTOCOL_IN_TEXT_DIRECT, PROTOCOL_IN_TEXT_INDIRECT],
                [PROTOCOL_OUT_TEXT_PLAIN],
                [PROTOCOL_OUT_AUDIO_LINK],
                [PROTOCOL_OUT_TTS_SERVERSIDE],
            ]
        }
        
        _LOGGER.debug(f"Sending negotiate: {negotiate_msg}")
        await self.ws_connection.send_json(negotiate_msg)
        
        # Wait for agreement
        try:
            msg = await asyncio.wait_for(
                self.ws_connection.receive_json(),
                timeout=10.0
            )
            
            _LOGGER.debug(f"Negotiate response: {msg}")
            
            if msg.get("type") == MSG_NEGOTIATE_AGREE:
                self.agreed_protocols = msg.get("protocols", [])
                _LOGGER.info(f"Protocols agreed: {self.agreed_protocols}")
                
                if not self.agreed_protocols:
                    _LOGGER.warning("No protocols agreed! Check server configuration.")
            else:
                _LOGGER.warning(f"Unexpected negotiate response: {msg}")
                self.agreed_protocols = []
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout waiting for protocol negotiation")
            self.agreed_protocols = []
    
    async def _ws_listener(self) -> None:
        """Listen for WebSocket messages."""
        try:
            while self.ws_connected and self.ws_connection and not self.ws_connection.closed:
                try:
                    msg = await self.ws_connection.receive(timeout=60.0)
                    
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            await self._handle_ws_message(data)
                        except json.JSONDecodeError as err:
                            _LOGGER.warning(f"Invalid JSON: {err}")
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        _LOGGER.debug("Received binary message")
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await self.ws_connection.pong(msg.data)
                    elif msg.type == aiohttp.WSMsgType.PONG:
                        _LOGGER.debug("Received pong")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        _LOGGER.info(f"WebSocket closing: {msg.data}")
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        _LOGGER.error(f"WebSocket error: {self.ws_connection.exception()}")
                        break
                        
                except asyncio.TimeoutError:
                    # Send ping to keep alive
                    try:
                        if self.ws_connection and not self.ws_connection.closed:
                            await self.ws_connection.ping()
                    except Exception:
                        break
                    continue
                except Exception as err:
                    _LOGGER.error(f"Error in WS listener: {err}")
                    break
        finally:
            was_connected = self.ws_connected
            self.ws_connected = False
            if was_connected:
                _LOGGER.info("WebSocket listener stopped, scheduling reconnect")
                # Schedule reconnect
                self.hass.async_create_task(self._reconnect())
    
    async def _reconnect(self) -> None:
        """Attempt to reconnect WebSocket."""
        while not self.ws_connected:
            _LOGGER.info(f"Reconnecting in {self._current_reconnect_delay}s...")
            await asyncio.sleep(self._current_reconnect_delay)
            
            try:
                await self.ensure_websocket_connected()
                _LOGGER.info("Reconnected successfully")
                return
            except Exception as err:
                _LOGGER.warning(f"Reconnect failed: {err}")
                self._current_reconnect_delay = min(
                    self._current_reconnect_delay * 2,
                    self._max_reconnect_delay
                )
    
    async def _handle_ws_message(self, data: dict[str, Any]) -> None:
        """Handle incoming WebSocket message."""
        msg_type = data.get("type", "")
        
        _LOGGER.debug(f"WS message: {msg_type}")
        
        if msg_type == MSG_OUT_TEXT_PLAIN_TEXT:
            text = data.get("text", "")
            _LOGGER.info(f"Received text response: {text}")
            self._add_to_history("assistant", text)
            
            # Resolve ALL pending futures
            for future in list(self._pending_responses.values()):
                if not future.done():
                    future.set_result({"type": "text", "text": text})
        
        elif msg_type == MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST:
            url = data.get("url", "")
            playback_id = data.get("playbackId", "")
            alt_text = data.get("altText", "")
            _LOGGER.info(f"Audio playback request: {url} (id: {playback_id})")
            
            # Add to history
            self._add_to_history("assistant", f"[Аудио] {alt_text or url}")
            
            # Resolve pending futures
            for future in list(self._pending_responses.values()):
                if not future.done():
                    future.set_result({
                        "type": "audio",
                        "url": url,
                        "playback_id": playback_id,
                        "alt_text": alt_text,
                    })
            
            # Send playback-done (мы не можем реально воспроизвести)
            if playback_id:
                try:
                    await self.ws_connection.send_json({
                        "type": MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
                        "playbackId": playback_id,
                    })
                except Exception as err:
                    _LOGGER.warning(f"Failed to send playback-done: {err}")
        
        elif msg_type == "negotiate/agree":
            # Уже обработано в _negotiate_protocols
            pass
        
        else:
            _LOGGER.debug(f"Unhandled message type: {msg_type}")
    
    async def send_text_command(self, text: str) -> str:
        """Send text command to Irene and get response."""
        try:
            await self.ensure_websocket_connected()
            
            # Add to history
            self._add_to_history("user", text)
            
            # Create future for response
            response_id = str(self._response_counter)
            self._response_counter += 1
            
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()
            self._pending_responses[response_id] = future
            
            # Send message по документации
            message = {
                "type": MSG_IN_TEXT_DIRECT_TEXT,
                "text": text,
            }
            
            _LOGGER.debug(f"Sending: {message}")
            
            try:
                await self.ws_connection.send_json(message)
            except (aiohttp.ClientConnectionResetError, ConnectionResetError) as err:
                _LOGGER.warning(f"Connection lost while sending: {err}")
                self.ws_connected = False
                # Try to reconnect and resend
                await asyncio.sleep(1)
                await self.ensure_websocket_connected()
                await self.ws_connection.send_json(message)
            
            # Wait for response with timeout
            try:
                result = await asyncio.wait_for(future, timeout=30.0)
                
                if result["type"] == "text":
                    return result["text"]
                elif result["type"] == "audio":
                    return result.get("alt_text") or "[Аудио ответ]"
                else:
                    return str(result)
                    
            except asyncio.TimeoutError:
                _LOGGER.warning("Timeout waiting for response")
                return "Превышено время ожидания ответа от Ирины"
            finally:
                self._pending_responses.pop(response_id, None)
                
        except Exception as err:
            _LOGGER.error(f"Error sending command: {err}", exc_info=True)
            self.ws_connected = False
            return f"Ошибка связи с Ириной: {err}"
    
    async def tts_say(self, text: str) -> None:
        """Make Irene say text via notification API."""
        try:
            url = f"{self.base_url}{API_NOTIFY}"
            payload = {"text": text}
            
            async with self.session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    _LOGGER.info(f"TTS message sent: {text}")
                    self._add_to_history("assistant", f"[TTS] {text}")
                else:
                    error_text = await response.text()
                    _LOGGER.error(f"TTS error {response.status}: {error_text}")
                    raise UpdateFailed(f"TTS error: {response.status}")
                    
        except Exception as err:
            _LOGGER.error(f"Error in TTS say: {err}")
            raise
    
    async def get_configs(self) -> list[dict[str, Any]]:
        """Get all configs from Irene."""
        try:
            url = f"{self.base_url}{API_CONFIGS}"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise UpdateFailed(f"API error: {response.status}")
        except Exception as err:
            _LOGGER.error(f"Error getting configs: {err}")
            raise
    
    async def get_plugins(self) -> list[dict[str, Any]]:
        """Get list of plugins."""
        try:
            url = f"{self.base_url}{API_PLUGINS}"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return []
        except Exception as err:
            _LOGGER.error(f"Error getting plugins: {err}")
            return []
    
    async def disconnect_websocket(self) -> None:
        """Disconnect WebSocket."""
        self.ws_connected = False
        
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
            try:
                await self._ws_listener_task
            except asyncio.CancelledError:
                pass
        
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        
        if self.ws_connection and not self.ws_connection.closed:
            try:
                await self.ws_connection.close()
            except Exception:
                pass
        
        # Cancel all pending futures
        for future in self._pending_responses.values():
            if not future.done():
                future.cancel()
        self._pending_responses.clear()
    
    def _add_to_history(self, role: str, content: str) -> None:
        """Add message to chat history."""
        self.chat_history.append({
            "role": role,
            "content": content,
            "timestamp": dt_util.utcnow().isoformat(),
        })
        
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history:]
    
    def get_chat_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get chat history."""
        return self.chat_history[-limit:]
    
    def clear_history(self) -> None:
        """Clear chat history."""
        self.chat_history = []