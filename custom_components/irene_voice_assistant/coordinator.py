# custom_components/irene_voice_assistant/coordinator.py
"""Data coordinator for Irene Voice Assistant with message buffering and HA bridge."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import timedelta
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util
from homeassistant.components.persistent_notification import async_create as async_create_notification

from .const import (
    API_CONFIGS,
    API_NOTIFY,
    API_WEBSOCKET,
    MSG_NEGOTIATE_AGREE,
    MSG_NEGOTIATE_REQUEST,
    MSG_IN_TEXT_DIRECT_TEXT,
    MSG_OUT_TEXT_PLAIN_TEXT,
    MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST,
    MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
    PROTOCOL_IN_TEXT_DIRECT,
    PROTOCOL_IN_TEXT_INDIRECT,
    PROTOCOL_OUT_TEXT_PLAIN,
    PROTOCOL_OUT_AUDIO_LINK,
    PROTOCOL_OUT_TTS_SERVERSIDE,
    DOMAIN,
)
from .ha_bridge import HaBridge

_LOGGER = logging.getLogger(__name__)

MESSAGE_BUFFER_TIMEOUT = 2.5


def clean_host(host: str) -> str:
    host = host.strip()
    if host.startswith("https://"):
        host = host[8:]
    elif host.startswith("http://"):
        host = host[7:]
    host = host.rstrip("/")
    if ":" in host:
        host = host.split(":")[0]
    return host


class BufferedResponse:
    """Буфер для накопления нескольких ответов от Ирины."""
    
    def __init__(self):
        self.messages: list[dict[str, Any]] = []
        self.timer: asyncio.TimerHandle | None = None
        self.future: asyncio.Future | None = None


class IreneCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator with message buffering and HA bridge."""
    
    def __init__(
        self,
        hass: HomeAssistant,
        session: aiohttp.ClientSession,
        base_url: str,
        name: str,
        return_format: str = "text",
        buffer_timeout: float = MESSAGE_BUFFER_TIMEOUT,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"Irene Voice Assistant ({name})",
            update_interval=timedelta(seconds=60),
        )
        
        self.hass = hass
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.name = name
        self.return_format = return_format
        self.buffer_timeout = buffer_timeout
        
        self.ws_base_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        
        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100
        
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.ws_lock = asyncio.Lock()
        self.agreed_protocols: list[str] = []
        
        # ✅ Буфер для накопления ответов
        self._response_buffer: BufferedResponse = BufferedResponse()
        self._pending_request: bool = False
        self._response_counter = 0
        
        self._ws_listener_task: asyncio.Task | None = None
        self._reconnect_delay = 5
        self._max_reconnect_delay = 60
        self._current_reconnect_delay = self._reconnect_delay
        
        self._ssl_context: ssl.SSLContext | bool = False
        if self.base_url.startswith("https://"):
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
        
        # ✅ HA Bridge для выполнения команд
        self.ha_bridge = HaBridge(hass)
    
    async def _async_update_data(self) -> dict[str, Any]:
        try:
            url = f"{self.base_url}{API_CONFIGS}"
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    configs = await response.json()
                    return {
                        "available": True,
                        "last_update": dt_util.utcnow(),
                        "configs_count": len(configs) if isinstance(configs, list) else 0,
                        "ws_status": "connected" if self.ws_connected else "disconnected",
                        "agreed_protocols": self.agreed_protocols,
                        "buffer_timeout": self.buffer_timeout,
                    }
        except (asyncio.TimeoutError, aiohttp.ClientError) as err:
            _LOGGER.warning(f"Update error: {err}")
        raise UpdateFailed("Failed to communicate with Irene")
    
    async def ensure_websocket_connected(self) -> None:
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
                    heartbeat=30.0,
                    autoping=True,
                )
                
                await self._negotiate_protocols()
                
                if self._ws_listener_task is None or self._ws_listener_task.done():
                    self._ws_listener_task = self.hass.async_create_task(self._ws_listener())
                
                self.ws_connected = True
                self._current_reconnect_delay = self._reconnect_delay
                _LOGGER.info(f"WebSocket connected. Protocols: {self.agreed_protocols}")
                
            except Exception as err:
                _LOGGER.error(f"WS connect failed: {err}", exc_info=True)
                self.ws_connected = False
                raise
    
    async def _negotiate_protocols(self) -> None:
        negotiate_msg = {
            "type": MSG_NEGOTIATE_REQUEST,
            "protocols": [
                [PROTOCOL_IN_TEXT_DIRECT, PROTOCOL_IN_TEXT_INDIRECT],
                [PROTOCOL_OUT_TEXT_PLAIN],
                [PROTOCOL_OUT_AUDIO_LINK],
                [PROTOCOL_OUT_TTS_SERVERSIDE],
            ]
        }
        await self.ws_connection.send_json(negotiate_msg)
        
        try:
            msg = await asyncio.wait_for(self.ws_connection.receive_json(), timeout=10.0)
            if msg.get("type") == MSG_NEGOTIATE_AGREE:
                self.agreed_protocols = msg.get("protocols", [])
                _LOGGER.info(f"Protocols agreed: {self.agreed_protocols}")
            else:
                self.agreed_protocols = []
        except asyncio.TimeoutError:
            self.agreed_protocols = []
    
    async def _ws_listener(self) -> None:
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
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await self.ws_connection.pong(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        _LOGGER.error(f"WS error: {self.ws_connection.exception()}")
                        break
                except asyncio.TimeoutError:
                    try:
                        if self.ws_connection and not self.ws_connection.closed:
                            await self.ws_connection.ping()
                    except Exception:
                        break
                    continue
                except Exception as err:
                    _LOGGER.error(f"WS listener error: {err}")
                    break
        finally:
            was_connected = self.ws_connected
            self.ws_connected = False
            if was_connected:
                _LOGGER.info("WS disconnected, scheduling reconnect")
                self.hass.async_create_task(self._reconnect())
    
    async def _reconnect(self) -> None:
        while not self.ws_connected and self.hass.is_running:
            await asyncio.sleep(self._current_reconnect_delay)
            try:
                await self.ensure_websocket_connected()
                return
            except Exception:
                self._current_reconnect_delay = min(
                    self._current_reconnect_delay * 2, self._max_reconnect_delay
                )
    
    def _flush_response_buffer(self) -> None:
        """Сбрасывает буфер и резолвит future со всеми накопленными сообщениями."""
        if self._response_buffer.future and not self._response_buffer.future.done():
            if self._response_buffer.messages:
                if len(self._response_buffer.messages) > 1:
                    texts = [m.get("text", "") for m in self._response_buffer.messages if m.get("text")]
                    combined = "\n\n".join(texts)
                    self._response_buffer.future.set_result({
                        "type": "text",
                        "text": combined,
                        "parts": self._response_buffer.messages,
                    })
                else:
                    msg = self._response_buffer.messages[0]
                    self._response_buffer.future.set_result({
                        "type": "text",
                        "text": msg.get("text", ""),
                        "parts": self._response_buffer.messages,
                    })
            else:
                self._response_buffer.future.set_result({
                    "type": "text",
                    "text": "",
                    "parts": [],
                })
        
        self._response_buffer.messages.clear()
        self._response_buffer.timer = None
        self._pending_request = False
    
    def _start_buffer_timer(self) -> None:
        """Запускает таймер ожидания дополнительных сообщений."""
        if self._response_buffer.timer:
            self._response_buffer.timer.cancel()
        
        loop = asyncio.get_event_loop()
        self._response_buffer.timer = loop.call_later(
            self.buffer_timeout,
            lambda: self.hass.async_create_task(self._async_flush_buffer())
        )
    
    async def _async_flush_buffer(self) -> None:
        """Асинхронный сброс буфера."""
        self._flush_response_buffer()
    
    async def _handle_ws_message(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type", "")
        
        if msg_type == MSG_OUT_TEXT_PLAIN_TEXT:
            text = data.get("text", "")
            _LOGGER.info(f"Received text: {text}")
            
            # ✅ Добавляем в буфер
            self._response_buffer.messages.append({
                "type": "text",
                "text": text,
                "timestamp": dt_util.utcnow().isoformat(),
            })
            
            self._add_to_history("assistant", text)
            
            self.hass.bus.async_fire(f"{DOMAIN}_message", {
                "type": "text",
                "content": text,
                "role": "assistant",
            })
            
            # ✅ Проверяем есть ли HA команды в ответе
            ha_result = await self.ha_bridge.process_message(text)
            if ha_result:
                _LOGGER.info(f"HA bridge result: {ha_result}")
                if ha_result.get("executed"):
                    confirm = f"✅ Выполнено: {', '.join(ha_result['executed'])}"
                    self._response_buffer.messages.append({
                        "type": "text",
                        "text": confirm,
                        "timestamp": dt_util.utcnow().isoformat(),
                    })
            
            # ✅ Если это сообщение вне запроса (таймер, уведомление) — показываем сразу
            if not self._pending_request:
                _LOGGER.info("Unsolicited message from Irene, showing as notification")
                async_create_notification(
                    self.hass,
                    message=text,
                    title=f"💬 {self.name}",
                    notification_id=f"irene_{int(dt_util.utcnow().timestamp())}",
                )
                self.hass.bus.async_fire("irene_notification", {
                    "message": text,
                    "title": self.name,
                    "type": "text",
                })
            else:
                # ✅ Запускаем/сбрасываем таймер буфера
                self._start_buffer_timer()
        
        elif msg_type == MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST:
            url = data.get("url", "")
            playback_id = data.get("playbackId", "")
            alt_text = data.get("altText", "")
            _LOGGER.info(f"Audio request: {url}")
            
            self._add_to_history("assistant", f"[Аудио] {alt_text or url}")
            self.hass.bus.async_fire(f"{DOMAIN}_message", {
                "type": "audio",
                "url": url,
                "alt_text": alt_text,
            })
            
            if playback_id:
                try:
                    await self.ws_connection.send_json({
                        "type": MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
                        "playbackId": playback_id,
                    })
                except Exception:
                    pass
            
            if not self._pending_request:
                async_create_notification(
                    self.hass,
                    message=alt_text or f"[Аудио: {url}]",
                    title=f"🔊 {self.name}",
                    notification_id=f"irene_audio_{int(dt_util.utcnow().timestamp())}",
                )
    
    async def send_text_command(self, text: str) -> str:
        """Send command and wait for ALL responses (with buffering)."""
        try:
            await self.ensure_websocket_connected()
            
            self._add_to_history("user", text)
            
            # ✅ Создаём future и буфер
            self._pending_request = True
            self._response_buffer.messages.clear()
            if self._response_buffer.timer:
                self._response_buffer.timer.cancel()
            
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()
            self._response_buffer.future = future
            
            message = {"type": MSG_IN_TEXT_DIRECT_TEXT, "text": text}
            _LOGGER.debug(f"Sending: {message}")
            
            try:
                await self.ws_connection.send_json(message)
            except (aiohttp.ClientConnectionResetError, ConnectionResetError):
                self.ws_connected = False
                await asyncio.sleep(1)
                await self.ensure_websocket_connected()
                await self.ws_connection.send_json(message)
            
            try:
                result = await asyncio.wait_for(future, timeout=30.0 + self.buffer_timeout)
                
                if result["type"] == "text":
                    return result["text"]
                elif result["type"] == "audio":
                    return result.get("alt_text") or "[Аудио ответ]"
                return str(result)
                    
            except asyncio.TimeoutError:
                if self._response_buffer.messages:
                    texts = [m.get("text", "") for m in self._response_buffer.messages]
                    return "\n\n".join(texts)
                return "Превышено время ожидания ответа от Ирины"
            finally:
                self._response_buffer.future = None
                self._response_buffer.timer = None
                self._pending_request = False
                
        except Exception as err:
            _LOGGER.error(f"Error sending command: {err}", exc_info=True)
            self.ws_connected = False
            return f"Ошибка связи с Ириной: {err}"
    
    async def tts_say(self, text: str) -> None:
        try:
            url = f"{self.base_url}{API_NOTIFY}"
            payload = {"text": text}
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    _LOGGER.info(f"TTS sent: {text}")
                    self._add_to_history("assistant", f"[TTS] {text}")
                else:
                    raise UpdateFailed(f"TTS error: {response.status}")
        except Exception as err:
            _LOGGER.error(f"TTS error: {err}")
            raise
    
    async def get_configs(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}{API_CONFIGS}"
        async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                return await response.json()
            raise UpdateFailed(f"API error: {response.status}")
    
    async def get_plugins(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/api/discover_plugins/plugins"
        async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                return await response.json()
            return []
    
    async def disconnect_websocket(self) -> None:
        self.ws_connected = False
        if self._response_buffer.timer:
            self._response_buffer.timer.cancel()
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
            try:
                await self._ws_listener_task
            except asyncio.CancelledError:
                pass
        if self.ws_connection and not self.ws_connection.closed:
            try:
                await self.ws_connection.close()
            except Exception:
                pass
        if self._response_buffer.future and not self._response_buffer.future.done():
            self._response_buffer.future.cancel()
        self._response_buffer.messages.clear()
    
    def _add_to_history(self, role: str, content: str) -> None:
        self.chat_history.append({
            "role": role,
            "content": content,
            "timestamp": dt_util.utcnow().isoformat(),
        })
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history:]
    
    def get_chat_history(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.chat_history[-limit:]
    
    def clear_history(self) -> None:
        self.chat_history = []