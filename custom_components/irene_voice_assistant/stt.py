# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterable

import aiohttp

from homeassistant.components.stt import (
    AudioBitRates,
    AudioChannels,
    AudioCodecs,
    AudioFormats,
    AudioSampleRates,
    SpeechMetadata,
    SpeechResult,
    SpeechResultState,
    SpeechToTextEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DOMAIN,
    API_WEBSOCKET,
    MSG_NEGOTIATE_REQUEST,
    MSG_NEGOTIATE_AGREE,
    MSG_IN_STT_SERVERSIDE_READY,
    MSG_IN_STT_SERVERSIDE_RECOGNIZED,
    PROTOCOL_IN_STT_SERVERSIDE,
)
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up STT platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneSTTEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT."""
    
    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"
    
    @property
    def supported_languages(self) -> list[str]:
        return ["ru", "en"]
    
    @property
    def supported_formats(self) -> list[AudioFormats]:
        return [AudioFormats.WAV]
    
    @property
    def supported_codecs(self) -> list[AudioCodecs]:
        return [AudioCodecs.PCM]
    
    @property
    def supported_bit_rates(self) -> list[AudioBitRates]:
        return [AudioBitRates.BITRATE_16]
    
    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]:
        return [AudioSampleRates.SAMPLERATE_16000]
    
    @property
    def supported_channels(self) -> list[AudioChannels]:
        return [AudioChannels.CHANNEL_MONO]

    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream using the correct 2-stage Irene protocol."""
        try:
            _LOGGER.info("🎤 [STT] Этап 1: Подключение к главному WebSocket для управления")
            ws_url = f"{self.coordinator.ws_base_url}{API_WEBSOCKET}"
            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False
            
            # 1. Подключаемся к ОСНОВНОМУ сокету для управления
            async with session.ws_connect(ws_url, timeout=15.0, ssl=ssl_ctx) as ws_main:
                _LOGGER.info("🎤 [STT] Этап 2: Согласование протокола in.stt.serverside")
                await ws_main.send_json({
                    "type": MSG_NEGOTIATE_REQUEST,
                    "protocols": [[PROTOCOL_IN_STT_SERVERSIDE]],
                })
                
                msg = await asyncio.wait_for(ws_main.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"🎤 [STT] Ошибка: ожидался TEXT, получено {msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                    
                data = msg.json()
                if data.get("type") != MSG_NEGOTIATE_AGREE:
                    _LOGGER.error(f"🎤 [STT] Ошибка согласования: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                # 2. Ждем сообщение READY с путем для аудио
                _LOGGER.info("🎤 [STT] Этап 3: Ожидание сообщения READY от сервера")
                msg = await asyncio.wait_for(ws_main.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"🎤 [STT] Ошибка READY: ожидался TEXT, получено {msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                    
                data = msg.json()
                if data.get("type") != MSG_IN_STT_SERVERSIDE_READY:
                    _LOGGER.error(f"🎤 [STT] Сервер не готов: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                path = data.get("path")
                if not path:
                    _LOGGER.error("🎤 [STT] В сообщении READY отсутствует поле 'path'")
                    return SpeechResult(None, SpeechResultState.ERROR)
                
                # 3. Подключаемся к НОВОМУ сокету для передачи аудио (Этап 4)
                audio_ws_url = f"{self.coordinator.ws_base_url}{path}?sample_rate=16000"
                _LOGGER.info(f"🎤 [STT] Этап 4: Подключение к аудио-сокету: {audio_ws_url}")
                
                ws_audio = await session.ws_connect(audio_ws_url, timeout=15.0, ssl=ssl_ctx)
                try:
                    _LOGGER.info("🎤 [STT] Этап 5: Отправка аудиопотока на сервер...")
                    chunk_count = 0
                    async for chunk in stream:
                        if chunk:
                            await ws_audio.send_bytes(chunk)
                            chunk_count += 1
                    
                    _LOGGER.info(f"🎤 [STT] Отправлено {chunk_count} чанков аудио. Закрытие аудио-сокета...")
                except Exception as e:
                    _LOGGER.error(f"🎤 [STT] Критическая ошибка при отправке аудио: {e}", exc_info=True)
                    return SpeechResult(None, SpeechResultState.ERROR)
                finally:
                    # КРИТИЧЕСКИ ВАЖНО: Закрываем аудио-сокет, чтобы сервер начал распознавание
                    await ws_audio.close()
                    _LOGGER.info("🎤 [STT] Аудио-сокет закрыт. Ожидание результата распознавания...")
                
                # 4. Ждем результат распознавания в ОСНОВНОМ сокете
                try:
                    # Увеличили таймаут до 30 секунд, так как Vosk может думать дольше
                    _LOGGER.info("🎤 [STT] Этап 6: Ожидание результата (таймаут 30 сек)...")
                    while True:
                        msg = await asyncio.wait_for(ws_main.receive(), timeout=30.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            if data.get("type") == MSG_IN_STT_SERVERSIDE_RECOGNIZED:
                                text = data.get("text", "").strip()
                                _LOGGER.info(f"✅ [STT] Успешно распознано: '{text}'")
                                return SpeechResult(text, SpeechResultState.SUCCESS)
                            else:
                                _LOGGER.debug(f"🎤 [STT] Получено другое сообщение: {data}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            _LOGGER.error(f"🎤 [STT] Главный сокет закрыт до получения результата. Код: {msg.type}")
                            return SpeechResult(None, SpeechResultState.ERROR)
                except asyncio.TimeoutError:
                    _LOGGER.error("🎤 [STT] ТАЙМАУТ: Сервер не вернул результат распознавания за 30 секунд.")
                    return SpeechResult(None, SpeechResultState.ERROR)
            
        except Exception as err:
            _LOGGER.error(f"🎤 [STT] Глобальная ошибка: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
