# custom_components/irene_voice_assistant/coordinator.py
"""Data coordinator for Irene Voice Assistant."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import timedelta
from typing import Any, Optional

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util
from homeassistant.components.persistent_notification import async_create as async_create_notification

from .const import (
    API_CONFIGS,
    API_NOTIFY,
    API_WEBSOCKET,
    DOMAIN,
    TTS_MODE_IRENE,
    TTS_MODE_MEDIA_PLAYER,
    TTS_MODE_BOTH,
)
from .ha_bridge import HaBridge

_LOGGER = logging.getLogger(__name__)
MESSAGE_BUFFER_TIMEOUT = 2.5


class BufferedResponse:
    """Буфер для накопления нескольких ответов от Ирины."""
    def __init__(self):
        self.messages: list[dict[str, Any]] = []
        self.timer: asyncio.TimerHandle | None = None
        self.future: asyncio.Future | None = None


class IreneCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator with message buffering, HA bridge and TTS to media_player."""

    def __init__(
        self,
        hass: HomeAssistant,
        session: aiohttp.ClientSession,
        base_url: str,
        name: str,
        return_format: str = "text",
        buffer_timeout: float = MESSAGE_BUFFER_TIMEOUT,
        media_player_entity: str | None = None,
        tts_mode: str = TTS_MODE_BOTH,
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
        self.media_player_entity = media_player_entity
        self.tts_mode = tts_mode
        self._pending_audio_responses: dict[str, asyncio.Future] = {}
        self._tts_request = False
        
        self.ws_base_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.ws_lock = asyncio.Lock()
        self.agreed_protocols: list[str] = []
        
        self._response_buffer: BufferedResponse = BufferedResponse()
        self._pending_request = False
        self._ws_listener_task: asyncio.Task | None = None
        self._reconnect_delay = 5
        self._max_reconnect_delay = 60
        self._current_reconnect_delay = self._reconnect_delay
        
        self._ssl_context: ssl.SSLContext | bool = False
        if self.base_url.startswith("https://"):
            self._ssl_context = ssl.create_default_context()
            self._ssl_context.check_hostname = False
            self._ssl_context.verify_mode = ssl.CERT_NONE
            
        self.ha_bridge = HaBridge(hass)
        _LOGGER.info(f"IreneCoordinator initialized: url={self.base_url}")

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
                    }
        except (asyncio.TimeoutError, aiohttp.ClientError) as err:
            raise UpdateFailed(f"Failed to communicate with Irene: {err}")

    async def ensure_websocket_connected(self) -> None:
        async with self.ws_lock:
            if self.ws_connected and self.ws_connection and not self.ws_connection.closed:
                return
            try:
                ws_url = f"{self.ws_base_url}{API_WEBSOCKET}"
                _LOGGER.info(f"Connecting to WebSocket: {ws_url}")
                self.ws_connection = await self.session.ws_connect(
                    ws_url, timeout=15.0, ssl=self._ssl_context if self._ssl_context else False
                )
                await self._negotiate_protocols()
                if self._ws_listener_task is None or self._ws_listener_task.done():
                    self._ws_listener_task = self.hass.async_create_task(self._ws_listener())
                self.ws_connected = True
                self._current_reconnect_delay = self._reconnect_delay
                _LOGGER.info("WebSocket connected.")
            except Exception as err:
                _LOGGER.error(f"WS connect failed: {err}")
                self.ws_connected = False
                raise

    async def _negotiate_protocols(self) -> None:
        negotiate_msg = {
            "type": "negotiate/request",
            "protocols": [
                ["in.text-direct", "in.text-indirect"],
                ["out.text-plain"],
                ["out.audio.link"],
                ["out.tts.serverside"],
            ],
        }
        await self.ws_connection.send_json(negotiate_msg)
        try:
            msg = await asyncio.wait_for(self.ws_connection.receive_json(), timeout=10.0)
            if msg.get("type") == "negotiate/agree":
                self.agreed_protocols = msg.get("protocols", [])
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
                        except json.JSONDecodeError:
                            pass
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await self.ws_connection.pong(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        break
                except asyncio.TimeoutError:
                    continue
                except Exception as err:
                    _LOGGER.error(f"WS listener error: {err}")
                    break
            finally:
                was_connected = self.ws_connected
                self.ws_connected = False
                if was_connected:
                    self.hass.async_create_task(self._reconnect())

    async def _reconnect(self) -> None:
        while not self.ws_connected and self.hass.is_running:
            await asyncio.sleep(self._current_reconnect_delay)
            try:
                await self.ensure_websocket_connected()
                return
            except Exception:
                self._current_reconnect_delay = min(self._current_reconnect_delay * 2, self._max_reconnect_delay)

    def _flush_response_buffer(self) -> None:
        if self._response_buffer.future and not self._response_buffer.future.done():
            if self._response_buffer.messages:
                texts = [m.get("text", "") for m in self._response_buffer.messages if m.get("text")]
                self._response_buffer.future.set_result({
                    "type": "text", "text": "\n".join(texts), "parts": self._response_buffer.messages
                })
            else:
                self._response_buffer.future.set_result({"type": "text", "text": "", "parts": []})
            self._response_buffer.messages.clear()
            self._response_buffer.timer = None
            self._pending_request = False

    def _start_buffer_timer(self) -> None:
        if self._response_buffer.timer:
            self._response_buffer.timer.cancel()
        loop = asyncio.get_event_loop()
        self._response_buffer.timer = loop.call_later(
            self.buffer_timeout, lambda: self.hass.async_create_task(self._async_flush_buffer())
        )

    async def _async_flush_buffer(self) -> None:
        self._flush_response_buffer()

    async def _handle_ws_message(self, data: dict[str, Any]) -> None:
        msg_type = data.get("type", "")
        if msg_type == "out.text-plain/text":
            text = data.get("text", "")
            if self._tts_request:
                return

            self._response_buffer.messages.append({"type": "text", "text": text, "timestamp": dt_util.utcnow().isoformat()})
            self._add_to_history("assistant", text)
            
            ha_result = await self.ha_bridge.process_message(text)
            if ha_result and ha_result.get("executed"):
                confirm = f"✅ Выполнено: {', '.join(ha_result['executed'])}"
                self._response_buffer.messages.append({"type": "text", "text": confirm, "timestamp": dt_util.utcnow().isoformat()})

            if not self._pending_request:
                async_create_notification(self.hass, message=text, title=f"💬 {self.name}")
            else:
                self._start_buffer_timer()
                
        elif msg_type == "out.audio.link/playback-request":
            url = data.get("url", "")
            playback_id = data.get("playbackId", "")
            if self._pending_audio_responses:
                for future in list(self._pending_audio_responses.values()):
                    if not future.done():
                        future.set_result(url)
                        break
            
            if not self._tts_request and playback_id:
                try:
                    await self.ws_connection.send_json({"type": "out.audio.link/playback-done", "playbackId": playback_id})
                except Exception:
                    pass

    # ✅ ИСПРАВЛЕННЫЙ МЕТОД (правильный порядок try/except/finally)
    async def send_text_command(self, text: str) -> str:
        """Send command and wait for ALL responses."""
        try:
            await self.ensure_websocket_connected()
            self._add_to_history("user", text)

            self._pending_request = True
            self._response_buffer.messages.clear()
            if self._response_buffer.timer:
                self._response_buffer.timer.cancel()
            
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()
            self._response_buffer.future = future
            
            message = {"type": "in.text-direct/text", "text": text}
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
                return str(result)
            except asyncio.TimeoutError:
                if self._response_buffer.messages:
                    texts = [m.get("text", "") for m in self._response_buffer.messages]
                    return "\n".join(texts)
                return "Превышено время ожидания ответа"
                
        except Exception as err:
            _LOGGER.error(f"Error sending command: {err}", exc_info=True)
            self.ws_connected = False
            return f"Ошибка связи с Ириной: {err}"
            
        finally:
            self._response_buffer.future = None
            self._response_buffer.timer = None
            self._pending_request = False

    async def tts_say(self, text: str) -> None:
        try:
            url = f"{self.base_url}{API_NOTIFY}"
            async with self.session.post(url, json={"text": text}, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    raise UpdateFailed(f"TTS error: {response.status}")
        except Exception as err:
            _LOGGER.error(f"TTS error: {err}")

    async def _get_tts_audio_url(self, message: str) -> Optional[str]:
        """Получить URL WAV файла через простой HTTP endpoint /ttsWav."""
        try:
            url = f"{self.base_url}/ttsWav?text={message}"
            _LOGGER.info(f"TTS audio URL generated: {url}")
            return url
        except Exception as err:
            _LOGGER.error(f"Error getting TTS audio URL: {err}")
            return None

    async def tts_to_media_player(self, message: str, media_player_entity: str | None = None) -> bool:
        media_player = media_player_entity or self.media_player_entity
        if not media_player:
            return False

        try:
            audio_url = await self._get_tts_audio_url(message)
            if not audio_url:
                await self.tts_say(message)
                return False

            full_url = audio_url if audio_url.startswith("http") else f"{self.base_url}{audio_url}"
            
            await self.hass.services.async_call(
                "media_player", "play_media",
                {
                    "entity_id": media_player,
                    "media_content_id": full_url,
                    "media_content_type": "audio/wav",
                },
                blocking=False,
            )
            self._add_to_history("assistant", f"[TTS→{media_player}] {message}")
            return True
        except Exception as err:
            _LOGGER.error(f"TTS to media_player error: {err}")
            try:
                await self.tts_say(message)
            except Exception:
                pass
            return False

    async def disconnect_websocket(self) -> None:
        self.ws_connected = False
        if self._response_buffer.timer:
            self._response_buffer.timer.cancel()
        if self._ws_listener_task and not self._ws_listener_task.done():
            self._ws_listener_task.cancel()
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
        if self._response_buffer.future and not self._response_buffer.future.done():
            self._response_buffer.future.cancel()
        self._response_buffer.messages.clear()

    def _add_to_history(self, role: str, content: str) -> None:
        self.chat_history.append({"role": role, "content": content, "timestamp": dt_util.utcnow().isoformat()})
        if len(self.chat_history) > self.max_history:
            self.chat_history = self.chat_history[-self.max_history:]
