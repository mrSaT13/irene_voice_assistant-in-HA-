# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant using WebSocket audio links."""
from __future__ import annotations
import asyncio
import logging
from typing import Any
import aiohttp
from homeassistant.components.tts import TextToSpeechEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneTTSEntity(hass, coordinator, config_entry)])

class IreneTTSEntity(TextToSpeechEntity):
    _attr_name = None

    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_tts"
        self._attr_name = f"{coordinator.name} TTS"

    @property
    def default_language(self) -> str: return "ru"
    @property
    def supported_languages(self) -> list[str]: return ["ru", "en"]
    @property
    def supported_options(self) -> list[str]: return []

    async def async_get_tts_audio(self, message: str, language: str, options: dict[str, Any]) -> tuple[str | None, bytes | None]:
        try:
            _LOGGER.info(f"TTS request: '{message}'")
            await self.coordinator.ensure_websocket_connected()

            # 1. Устанавливаем флаг TTS и создаем Future для перехвата URL
            self.coordinator._tts_request = True
            loop = asyncio.get_event_loop()
            self.coordinator.tts_audio_future = loop.create_future()

            # 2. Отправляем текст в Ирину (это спровоцирует ответ с аудио)
            ws_message = {"type": "in.text-direct/text", "text": message}
            await self.coordinator.ws_connection.send_json(ws_message)

            # 3. Ждем, пока coordinator перехватит out.audio.link/playback-request
            try:
                result = await asyncio.wait_for(self.coordinator.tts_audio_future, timeout=30.0)
                audio_url = result["url"]
                playback_id = result["playback_id"]
                
                # Формируем полный URL
                full_url = audio_url if audio_url.startswith("http") else f"{self.coordinator.base_url}{audio_url}"
                _LOGGER.info(f"Downloading TTS audio from: {full_url}")

                # 4. Скачиваем WAV файл
                session = async_get_clientsession(self.hass, verify_ssl=False)
                async with session.get(full_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        audio_bytes = await response.read()
                        _LOGGER.info(f"TTS audio downloaded: {len(audio_bytes)} bytes")
                        
                        # 5. Сообщаем серверу, что мы "воспроизвели" файл (чтобы он его удалил/освободил)
                        if playback_id and self.coordinator.ws_connection and not self.coordinator.ws_connection.closed:
                            await self.coordinator.ws_connection.send_json({
                                "type": "out.audio.link/playback-done",
                                "playbackId": playback_id,
                            })
                        
                        return "wav", audio_bytes
                    else:
                        _LOGGER.error(f"Failed to download TTS audio: HTTP {response.status}")
                        return None, None

            except asyncio.TimeoutError:
                _LOGGER.error("Timeout waiting for TTS audio URL from Irene")
                return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None
        finally:
            self.coordinator._tts_request = False
            self.coordinator.tts_audio_future = None
