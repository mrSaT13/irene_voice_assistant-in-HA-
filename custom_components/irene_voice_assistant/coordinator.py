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
    MSG_NEGOTIATE_AGREE,
    MSG_NEGOTIATE_REQUEST,
    MSG_IN_TEXT_DIRECT_TEXT,
    MSG_OUT_TEXT_PLAIN_TEXT,
    MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST,
    MSG_OUT_AUDIO_LINK_PLAYBACK_PROGRESS,
    MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
    MSG_IN_STT_SERVERSIDE_READY,
    MSG_IN_MUTE_MUTE,
    MSG_IN_MUTE_UNMUTE,
    PROTOCOL_IN_TEXT_DIRECT,
    PROTOCOL_IN_TEXT_INDIRECT,
    PROTOCOL_IN_STT_SERVERSIDE,
    PROTOCOL_IN_MUTE,
    PROTOCOL_OUT_TEXT_PLAIN,
    PROTOCOL_OUT_AUDIO_LINK,
    PROTOCOL_OUT_TTS_SERVERSIDE,
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

        # ✅ TTS на колонку
        self.media_player_entity = media_player_entity
        self.tts_mode = tts_mode
        self._pending_audio_responses: dict[str, asyncio.Future] = {}

        # ✅ Флаг TTS запросов (чтобы игнорировать текст в буфере)
        self._tts_request = False

        # ✅ НОВОЕ: Текущее воспроизведение аудио
        self._pending_playback: Optional[dict[str, str]] = None
        self._playback_progress_task: Optional[asyncio.Task] = None

        # ✅ НОВОЕ: STT-серверный путь
        self._stt_serverside_path: Optional[str] = None
        self._stt_session_ready = asyncio.Event()

        self.ws_base_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")

        self.chat_history: list[dict[str, Any]] = []
        self.max_history = 100

        self.ws_connection: aiohttp.ClientWebSocketResponse | None = None
        self.ws_connected = False
        self.ws_lock = asyncio.Lock()
        self.agreed_protocols: list[str] = []

        # Буфер для накопления ответов
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

        # HA Bridge для выполнения команд
        self.ha_bridge = HaBridge(hass)

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
        """Согласование протоколов в правильном порядке.
        
        ВАЖНО: out.tts.serverside должен идти ПОСЛЕ out.audio.link.
        """
        negotiate_msg = {
            "type": MSG_NEGOTIATE_REQUEST,
            "protocols": [
                [PROTOCOL_IN_TEXT_DIRECT, PROTOCOL_IN_TEXT_INDIRECT],
                [PROTOCOL_OUT_AUDIO_LINK, PROTOCOL_OUT_TEXT_PLAIN],
                [PROTOCOL_OUT_TTS_SERVERSIDE],
                [PROTOCOL_IN_STT_SERVERSIDE],
                [PROTOCOL_IN_MUTE],
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
                _LOGGER.warning(f"Unexpected negotiate response: {msg}")
        except asyncio.TimeoutError:
            self.agreed_protocols = []
            _LOGGER.warning("Negotiate timeout, no protocols agreed")

    async def send_text_command_http(self, text: str) -> str:
        """Fallback: отправить команду как уведомление Irene (TTS)."""
        try:
            url = f"{self.base_url}{API_NOTIFY}"
            payload = {"text": text}
            async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    _LOGGER.info(f"HTTP notification sent: {text}")
                    return text
                else:
                    _LOGGER.warning(f"HTTP notification returned {response.status}")
                    return f"Ошибка HTTP {response.status}"
        except Exception as err:
            return f"Ошибка связи: {err}"

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

        # === ТЕКСТ ===
        if msg_type == MSG_OUT_TEXT_PLAIN_TEXT:
            text = data.get("text", "")
            _LOGGER.info(f"Received text: {text}")

            if self._tts_request:
                _LOGGER.debug("Ignoring text for TTS request")
                return

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

            ha_result = await self.ha_bridge.process_message(text)
            if ha_result and ha_result.get("executed"):
                _LOGGER.info(f"HA bridge result: {ha_result}")
                confirm = f"✅ Выполнено: {', '.join(ha_result['executed'])}"
                self._response_buffer.messages.append({
                    "type": "text",
                    "text": confirm,
                    "timestamp": dt_util.utcnow().isoformat(),
                })

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
                self._start_buffer_timer()

        # === АУДИО ===
        elif msg_type == MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST:
            url = data.get("url", "")
            playback_id = data.get("playbackId", "")
            alt_text = data.get("altText", "")
            _LOGGER.info(f"Audio request: {url}, playbackId={playback_id}")

            # Резолвим future для TTS (если ждут URL)
            if self._pending_audio_responses:
                for future in list(self._pending_audio_responses.values()):
                    if not future.done():
                        future.set_result(url)
                        _LOGGER.info(f"Resolved audio URL: {url}")
                        break
                # Для TTS запроса - НЕ отправляем playback-done сейчас,
                # отправим после скачивания файла в tts.py
            else:
                # Для обычных команд (не TTS) - отправляем playback-done сразу,
                # чтобы сервер не ждал и освободил ресурсы
                if playback_id:
                    try:
                        await self.ws_connection.send_json({
                            "type": MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
                            "playbackId": playback_id,
                        })
                    except Exception:
                        pass

            self._add_to_history("assistant", f"[Аудио] {alt_text or url}")
            self.hass.bus.async_fire(f"{DOMAIN}_message", {
                "type": "audio",
                "url": url,
                "alt_text": alt_text,
                "playback_id": playback_id,
            })

            if not self._pending_request:
                async_create_notification(
                    self.hass,
                    message=alt_text or f"[Аудио: {url}]",
                    title=f"🔊 {self.name}",
                    notification_id=f"irene_audio_{int(dt_util.utcnow().timestamp())}",
                )

        # === STT ready ===
        elif msg_type == MSG_IN_STT_SERVERSIDE_READY:
            path = data.get("path", "")
            self._stt_serverside_path = path
            _LOGGER.info(f"STT serverside ready, path: {path}")
            self._stt_session_ready.set()

        # === ЗАГЛУШЕНИЕ МИКРОФОНА ===
        elif msg_type == MSG_IN_MUTE_MUTE:
            _LOGGER.info("Muting microphone (Irene is speaking)")
            self.hass.bus.async_fire(f"{DOMAIN}_mute", {"muted": True})

        elif msg_type == MSG_IN_MUTE_UNMUTE:
            _LOGGER.info("Unmuting microphone")
            self.hass.bus.async_fire(f"{DOMAIN}_mute", {"muted": False})

    async def _send_playback_progress(self, playback_id: str) -> None:
        """Отправляет playback-progress каждую секунду."""
        try:
            while (
                self._pending_playback
                and self._pending_playback.get("playback_id") == playback_id
            ):
                await asyncio.sleep(1.0)
                if not self.ws_connection or self.ws_connection.closed:
                    break
                try:
                    await self.ws_connection.send_json({
                        "type": MSG_OUT_AUDIO_LINK_PLAYBACK_PROGRESS,
                        "playbackId": playback_id,
                    })
                except Exception as err:
                    _LOGGER.warning(f"Progress send error: {err}")
                    break
        except asyncio.CancelledError:
            pass  # Нормальная отмена при playback-done

    async def send_playback_done(self, playback_id: str) -> None:
        """Публичный метод: вызывайте когда файл реально скачан/воспроизведён."""
        if not self.ws_connection or self.ws_connection.closed:
            return
        try:
            await self.ws_connection.send_json({
                "type": MSG_OUT_AUDIO_LINK_PLAYBACK_DONE,
                "playbackId": playback_id,
            })
            _LOGGER.info(f"Playback done sent: {playback_id}")
            # Останавливаем таймер прогресса
            if self._playback_progress_task and not self._playback_progress_task.done():
                self._playback_progress_task.cancel()
            if self._pending_playback and self._pending_playback.get("playback_id") == playback_id:
                self._pending_playback = None
        except Exception as err:
            _LOGGER.error(f"Playback done error: {err}")

    def get_pending_playback(self) -> Optional[dict[str, str]]:
        """Получить текущее ожидающее воспроизведение (для tts.py)."""
        return self._pending_playback

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

    async def tts_to_media_player(
        self,
        message: str,
        media_player_entity: str | None = None,
        timeout: float = 15.0,
    ) -> bool:
        """Озвучить текст на колонке через TTS Ирины.

        Использует серверный TTS Ирины (её голос!) и отправляет WAV на колонку
        через media_player.play_media.

        Returns:
            True если успешно, False если не удалось.
        """
        media_player = media_player_entity or self.media_player_entity

        if not media_player:
            _LOGGER.warning("No media_player configured for TTS")
            return False

        try:
            # Сбрасываем предыдущий pending playback
            self._pending_playback = None

            # 1. Получаем URL WAV файла от Ирины
            audio_url = await self._get_tts_audio_url(message, timeout=timeout)

            if not audio_url:
                _LOGGER.warning("Failed to get TTS audio URL, falling back to server TTS")
                await self.tts_say(message)
                return False

            # 2. Формируем полный URL
            full_url = f"{self.base_url}{audio_url}"
            _LOGGER.info(f"TTS audio URL: {full_url}")

            # ✅ Получаем playback_id ДО отправки на колонку
            pending = self.get_pending_playback()
            playback_id = pending.get("playback_id") if pending else None

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

            # ✅ Отправляем done через небольшую задержку (HA ставит в очередь)
            if playback_id:
                # Даём HA 0.5с на постановку в очередь
                await asyncio.sleep(0.5)
                await self.send_playback_done(playback_id)

            _LOGGER.info(f"TTS sent to media_player: {media_player}")
            self._add_to_history("assistant", f"[TTS→{media_player}] {message}")
            return True

        except Exception as err:
            _LOGGER.error(f"TTS to media_player error: {err}", exc_info=True)
            # Fallback: всё равно отправляем done и озвучиваем через сервер
            pending = self.get_pending_playback()
            if pending and pending.get("playback_id"):
                await self.send_playback_done(pending["playback_id"])
            try:
                await self.tts_say(message)
            except Exception:
                pass
            return False

    async def _get_tts_audio_url(self, message: str, timeout: float = 15.0) -> Optional[str]:
        """Отправить текст через WebSocket и получить URL WAV файла.

        Returns:
            URL WAV файла (например, /api/web-audio-link-output/files/xxx.wav)
            или None если не удалось получить.
        """
        try:
            await self.ensure_websocket_connected()

            # ✅ Устанавливаем флаг TTS запроса
            self._tts_request = True

            # Создаём future для ожидания ответа
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()

            # Регистрируем future для получения audio URL
            audio_response_id = f"audio_{int(time.time() * 1000)}"
            self._pending_audio_responses[audio_response_id] = future

            try:
                # Отправляем текст
                message_data = {
                    "type": MSG_IN_TEXT_DIRECT_TEXT,
                    "text": message,
                }
                _LOGGER.info(f"Sending TTS request: {message}")
                await self.ws_connection.send_json(message_data)

                # Ждём ответ с URL аудио
                try:
                    result = await asyncio.wait_for(future, timeout=timeout)
                    _LOGGER.info(f"Got TTS audio URL: {result}")
                    return result
                except asyncio.TimeoutError:
                    _LOGGER.warning("Timeout waiting for TTS audio URL")
                    return None

            finally:
                self._pending_audio_responses.pop(audio_response_id, None)
                # ✅ Сбрасываем флаг
                self._tts_request = False

        except Exception as err:
            _LOGGER.error(f"Error getting TTS audio URL: {err}", exc_info=True)
            self._tts_request = False
            return None

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
        if self._playback_progress_task and not self._playback_progress_task.done():
            self._playback_progress_task.cancel()
        if self.ws_connection and not self.ws_connection.closed:
            try:
                await self.ws_connection.close()
            except Exception:
                pass
        if self._response_buffer.future and not self._response_buffer.future.done():
            self._response_buffer.future.cancel()
        self._response_buffer.messages.clear()

        for future in self._pending_audio_responses.values():
            if not future.done():
                future.cancel()
        self._pending_audio_responses.clear()

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
