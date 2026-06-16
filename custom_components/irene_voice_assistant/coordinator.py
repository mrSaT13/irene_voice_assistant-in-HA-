# custom_components/irene_voice_assistant/coordinator.py
"""Data coordinator for Irene Voice Assistant with message buffering, HA bridge and TTS to media_player."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
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
    API_TTS_WAV,
    API_SEND_TXT_CMD,
    DOMAIN,
    TTS_MODE_IRENE,
    TTS_MODE_MEDIA_PLAYER,
    TTS_MODE_BOTH,
)
from .ha_bridge import HaBridge

_LOGGER = logging.getLogger(__name__)
MESSAGE_BUFFER_TIMEOUT = 2.5

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
        # ✅ НОВЫЕ поля для TTS на колонку
        self.media_player_entity = media_player_entity
        self.tts_mode = tts_mode
        self._pending_audio_responses: dict[str, asyncio.Future] = {}
        # ✅ НОВОЕ: Флаг для TTS запросов (чтобы игнорировать текст в буфере)
        self._tts_request = False
        self.ws_base_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100
        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.ws_lock = asyncio.Lock()
        self.agreed_protocols: list[str] = []
        # Буфер для накопления ответов
        self._response_buffer: BufferedResponse = BufferedResponse()
        self._pending_request = False
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
        # HA Bridge для выполнения команд
        self.ha_bridge = HaBridge(hass)
        
        # ✅ НОВЫЕ поля для STT и TTS через WS
        self.stt_ready_path: str | None = None
        self.stt_result_future: asyncio.Future | None = None
        self.tts_audio_future: asyncio.Future | None = None
        
        _LOGGER.info(
            f"IreneCoordinator initialized: mode={tts_mode}, "
            f"media_player={media_player_entity}, url={base_url}"
        )

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
                        "tts_mode": self.tts_mode,
                        "media_player": self.media_player_entity,
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

                # ✅ ИСПРАВЛЕНИЕ 1: Убираем heartbeat и autoping!
                self.ws_connection = await self.session.ws_connect(
                    ws_url,
                    timeout=15.0,
                    ssl=self._ssl_context if self._ssl_context else False,
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
        # ✅ ИСПРАВЛЕНИЕ 2: Добавляем in.stt.serverside для STT
        negotiate_msg = {
            "type": "negotiate/request",
            "protocols": [
                ["in.text-direct", "in.text-indirect"],
                ["out.text-plain"],
                ["out.audio.link"],
                ["out.tts.serverside"],
                ["in.stt.serverside"],  # ✅ ДОБАВЛЕНО для STT
            ],
        }
        await self.ws_connection.send_json(negotiate_msg)
        try:
            msg = await asyncio.wait_for(self.ws_connection.receive_json(), timeout=10.0)
            if msg.get("type") == "negotiate/agree":
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
                    # ✅ ИСПРАВЛЕНИЕ 3: Убираем отправку PING при таймауте!
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
                    texts = [m.get("text", "") or m.get("alt_text", "") for m in self._response_buffer.messages if m.get("text") or m.get("alt_text")]
                    combined = "\n".join(texts)
                    self._response_buffer.future.set_result({
                        "type": "text",
                        "text": combined,
                        "parts": self._response_buffer.messages,
                    })
                else:
                    msg = self._response_buffer.messages[0]
                    self._response_buffer.future.set_result({
                        "type": "text",
                        "text": msg.get("text", "") or msg.get("alt_text", ""),
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
        _LOGGER.debug(f"WS Received: {msg_type} -> {data}")
        
        # ✅ НОВОЕ: STT обработка
        if msg_type == "in.stt.serverside/ready":
            self.stt_ready_path = data.get("path")
            _LOGGER.info(f"STT ready path received: {self.stt_ready_path}")
            return
        elif msg_type == "in.stt.serverside/recognized":
            text = data.get("text", "")
            _LOGGER.info(f"STT recognized: {text}")
            if self.stt_result_future and not self.stt_result_future.done():
                self.stt_result_future.set_result(text)
            return
        elif msg_type == "in.stt.serverside/processed":
            _LOGGER.info(f"STT processed: {data.get('text')}")
            return

        if msg_type == "out.text-plain/text":
            text = data.get("text", "")
            _LOGGER.info(f"Received text: {text}")

            # ✅ Игнорируем текст для TTS запросов (чтобы не дублировать в буфере)
            if self._tts_request:
                _LOGGER.debug("Ignoring text for TTS request")
                return

            # Добавляем в буфер
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

            # Проверяем есть ли HA команды в ответе
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

            # Если это сообщение вне запроса (таймер, уведомление) — показываем сразу
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
                # Запускаем/сбрасываем таймер буфера
                self._start_buffer_timer()
        elif msg_type == "out.audio.link/playback-request":
            url = data.get("url", "")
            playback_id = data.get("playbackId", "")
            alt_text = data.get("altText", "")
            _LOGGER.info(f"Audio request: {url}")

            # ✅ НОВОЕ: Если есть pending audio responses - резолвим future
            if self._pending_audio_responses:
                for future in list(self._pending_audio_responses.values()):
                    if not future.done():
                        future.set_result(url)
                        _LOGGER.info(f"Resolved audio URL for pending request: {url}")
                        break
            
            # ✅ НОВОЕ: Если это TTS запрос - резолвим tts_audio_future
            if self.tts_audio_future and not self.tts_audio_future.done():
                self.tts_audio_future.set_result({"url": url, "playback_id": playback_id})
                _LOGGER.info(f"TTS audio URL intercepted: {url}")
                return  # TTS сам отправит playback-done

            self._add_to_history("assistant", f"[Аудио] {alt_text or url}")
            self.hass.bus.async_fire(f"{DOMAIN}_message", {
                "type": "audio",
                "url": url,
                "alt_text": alt_text,
            })

            # ✅ ИСПРАВЛЕНИЕ: Не шлем playback-done, если это TTS запрос!
            if not self._tts_request and playback_id:
                try:
                    await self.ws_connection.send_json({
                        "type": "out.audio.link/playback-done",
                        "playbackId": playback_id,
                    })
                except Exception:
                    pass
            
            # ✅ ИСПРАВЛЕНИЕ: Если есть pending_request, добавляем в буфер и запускаем таймер!
            if self._pending_request:
                self._response_buffer.messages.append({
                    "type": "audio",
                    "url": url,
                    "alt_text": alt_text,
                    "timestamp": dt_util.utcnow().isoformat(),
                })
                self._start_buffer_timer()  # ✅ ЗАПУСКАЕМ ТАЙМЕР!
            
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

            # Создаём future и буфер
            self._pending_request = True
            self._response_buffer.messages.clear()
            if self._response_buffer.timer:
                self._response_buffer.timer.cancel()
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()
            self._response_buffer.future = future
            message = {"type": "in.text-direct/text", "text": text}
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
                    texts = [m.get("text", "") or m.get("alt_text", "") for m in self._response_buffer.messages]
                    return "\n".join(texts)
                return "Превышено время ожидания ответа от Ирины"
        except Exception as err:
            _LOGGER.error(f"Error sending command: {err}", exc_info=True)
            self.ws_connected = False
            return f"Ошибка связи с Ириной: {err}"
        finally:
            self._response_buffer.future = None
            self._response_buffer.timer = None
            self._pending_request = False

    async def tts_say(self, text: str) -> None:
        """Озвучить текст через сервер Ирины (через /api/notification_api/notify)."""
        try:
            url = f"{self.base_url}{API_NOTIFY}"
            payload = {"text": text}
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    _LOGGER.info(f"TTS sent to Irene server: {text}")
                    self._add_to_history("assistant", f"[TTS] {text}")
                else:
                    raise UpdateFailed(f"TTS error: {response.status}")
        except Exception as err:
            _LOGGER.error(f"TTS error: {err}")
            raise

    # ✅ ИСПРАВЛЕННЫЙ МЕТОД: Озвучка на колонке через TTS Ирины
    async def tts_to_media_player(
        self,
        message: str,
        media_player_entity: str | None = None,
        timeout: float = 15.0,
    ) -> bool:
        """Озвучить текст на колонке через TTS Ирины.

        Использует WS протокол для получения аудио URL и отправляет WAV на колонку
        через media_player.play_media.

        Returns:
            True если успешно, False если не удалось.
        """
        media_player = media_player_entity or self.media_player_entity

        if not media_player:
            _LOGGER.warning("No media_player configured for TTS")
            return False

        try:
            # 1. Получаем URL WAV файла от Ирины через WS
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

    # ✅ ИСПРАВЛЕННЫЙ МЕТОД: Получение URL WAV файла от Ирины через WS
    async def _get_tts_audio_url(self, message: str, timeout: float = 15.0) -> Optional[str]:
        """Получить URL WAV файла через WS протокол.
        
        Отправляет текст через WS и ждёт out.audio.link/playback-request с URL.
        
        Returns:
            URL WAV файла (например, /api/web-audio-link-output/files/xxx.wav)
            или None если не удалось получить.
        """
        try:
            await self.ensure_websocket_connected()
            
            # Устанавливаем флаг TTS и создаем Future для перехвата URL
            self._tts_request = True
            loop = asyncio.get_event_loop()
            self.tts_audio_future = loop.create_future()
            
            # Отправляем текст в Ирину
            ws_message = {"type": "in.text-direct/text", "text": message}
            await self.ws_connection.send_json(ws_message)
            
            # Ждём, пока coordinator перехватит out.audio.link/playback-request
            result = await asyncio.wait_for(self.tts_audio_future, timeout=timeout)
            audio_url = result["url"]
            playback_id = result["playback_id"]
            
            _LOGGER.info(f"TTS audio URL received: {audio_url}")
            
            # Сообщаем серверу, что мы "воспроизвели" файл
            if playback_id and self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.send_json({
                    "type": "out.audio.link/playback-done",
                    "playbackId": playback_id,
                })
            
            return audio_url
            
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout waiting for TTS audio URL from Irene")
            return None
        except Exception as err:
            _LOGGER.error(f"Error getting TTS audio URL: {err}", exc_info=True)
            return None
        finally:
            self._tts_request = False
            self.tts_audio_future = None

    # ✅ НОВЫЙ МЕТОД: Универсальная озвучка с учётом режима
    async def tts_say_with_mode(
        self,
        message: str,
        mode: str | None = None,
        media_player: str | None = None,
    ) -> None:
        """Озвучить текст с учётом режима и выбранной колонки.

        Args:
            message: Текст для озвучки
            mode: Режим озвучки (irene/media_player/both)
            media_player: Медиаплеер для озвучки (переопределяет настройку)
        """
        mode = mode or self.tts_mode
        media_player = media_player or self.media_player_entity
        _LOGGER.info(f"TTS say: mode={mode}, media_player={media_player}, message='{message[:50]}...'")

        # 1. Озвучка через Ирину (её серверный TTS)
        if mode in (TTS_MODE_IRENE, TTS_MODE_BOTH):
            try:
                await self.tts_say(message)
                _LOGGER.info("TTS sent to Irene server")
            except Exception as err:
                _LOGGER.error(f"Irene TTS error: {err}")

        # 2. Озвучка через колонку (голос Ирины на колонке!)
        if mode in (TTS_MODE_MEDIA_PLAYER, TTS_MODE_BOTH) and media_player:
            try:
                await self.tts_to_media_player(message, media_player)
                _LOGGER.info(f"TTS sent to media_player: {media_player}")
            except Exception as err:
                _LOGGER.error(f"Media player TTS error: {err}")

        # Если режим "только колонка" и не получилось - пробуем fallback на сервер
        if mode == TTS_MODE_MEDIA_PLAYER:
            try:
                await self.tts_say(message)
            except Exception:
                pass

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

        # ✅ Очищаем pending audio responses
        for future in self._pending_audio_responses.values():
            if not future.done():
                future.cancel()
        self._pending_audio_responses.clear()
        
        # ✅ Очищаем STT/TTS futures
        if self.stt_result_future and not self.stt_result_future.done():
            self.stt_result_future.cancel()
        if self.tts_audio_future and not self.tts_audio_future.done():
            self.tts_audio_future.cancel()

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

    # ✅ НОВЫЕ МЕТОДЫ: Fallback на старые HTTP endpoints (браузерная имитация)
    async def send_text_command_http(self, text: str) -> str:
        """Отправить команду через старый HTTP endpoint /sendTxtCmd.
        
        Используется как fallback когда WebSocket не работает.
        Имитирует браузер с помощью user-agent заголовка.
        """
        try:
            from urllib.parse import quote
            url = f"{self.base_url}{API_SEND_TXT_CMD}?cmd={quote(text)}&returnFormat=saytxt"
            _LOGGER.info(f"Sending command via HTTP: {url}")
            
            # ✅ Заголовки для имитации браузера
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
            
            async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    result = await response.text()
                    _LOGGER.info(f"HTTP command result: {result[:200]}")
                    self._add_to_history("user", text)
                    if result and result != "NO_VA_NAME":
                        self._add_to_history("assistant", result)
                        return result
                    return "Команда не распознана"
                else:
                    return f"Ошибка HTTP: {response.status}"
        except Exception as err:
            _LOGGER.error(f"HTTP command error: {err}", exc_info=True)
            return f"Ошибка связи: {err}"

    async def tts_say_http(self, text: str) -> Optional[bytes]:
        """Получить WAV аудио через старый HTTP endpoint /ttsWav.
        
        Используется как fallback когда WebSocket не работает.
        Имитирует браузер с помощью user-agent заголовка.
        Возвращает bytes аудио или None.
        """
        try:
            from urllib.parse import quote
            url = f"{self.base_url}{API_TTS_WAV}?text={quote(text)}"
            _LOGGER.info(f"Getting TTS via HTTP: {url}")
            
            # ✅ Заголовки для имитации браузера
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "audio/wav,audio/wave,audio/x-wav,*/*",
            }
            
            async with self.session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    audio_bytes = await response.read()
                    _LOGGER.info(f"HTTP TTS received: {len(audio_bytes)} bytes")
                    return audio_bytes
                else:
                    _LOGGER.error(f"HTTP TTS error: {response.status}")
                    return None
        except Exception as err:
            _LOGGER.error(f"HTTP TTS error: {err}", exc_info=True)
            return None
