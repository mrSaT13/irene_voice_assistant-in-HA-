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

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up TTS platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneTTSEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")

class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity using WebSocket audio links."""
    _attr_name = None

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the TTS entity."""
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_tts"
        self._attr_name = f"{coordinator.name} TTS"

    @property
    def default_language(self) -> str:
        """Return the default language."""
        return "ru"

    @property
    def supported_languages(self) -> list[str]:
        """Return the list of supported languages."""
        return ["ru", "en"]

    @property
    def supported_options(self) -> list[str]:
        """Return list of supported options."""
        return []

    async def async_get_tts_audio(
        self,
        message: str,
        language: str,
        options: dict[str, Any]
    ) -> tuple[str | None, bytes | None]:
        """Load TTS from Irene using WebSocket protocol with HTTP fallback."""
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")
            
            # ✅ ПРОБУЕМ СНАЧАЛА WebSocket
            try:
                await self.coordinator.ensure_websocket_connected()

                # 1. Устанавливаем флаг TTS и создаем Future для перехвата URL
                self.coordinator._tts_request = True
                loop = asyncio.get_event_loop()
                self.coordinator.tts_audio_future = loop.create_future()

                # 2. Отправляем текст в Ирину (это спровоцирует ответ с аудио)
                ws_message = {"type": "in.text-direct/text", "text": message}
                await self.coordinator.ws_connection.send_json(ws_message)

                # 3. Ждем, пока coordinator перехватит out.audio.link/playback-request
                result = await asyncio.wait_for(self.coordinator.tts_audio_future, timeout=10.0)
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
                        _LOGGER.info(f"TTS audio downloaded successfully: {len(audio_bytes)} bytes")
                        
                        # 5. Сообщаем серверу, что мы "воспроизвели" файл
                        if playback_id and self.coordinator.ws_connection and not self.coordinator.ws_connection.closed:
                            await self.coordinator.ws_connection.send_json({
                                "type": "out.audio.link/playback-done",
                                "playbackId": playback_id,
                            })
                        
                        return "wav", audio_bytes
                    else:
                        _LOGGER.warning(f"HTTP {response.status}, trying fallback...")

            except Exception as ws_err:
                _LOGGER.warning(f"WebSocket TTS failed: {ws_err}, trying HTTP fallback...")

            # ✅ FALLBACK: Пробуем старый HTTP endpoint /ttsWav
            _LOGGER.info("Trying HTTP fallback for TTS...")
            audio_bytes = await self.coordinator.tts_say_http(message)
            
            if audio_bytes and len(audio_bytes) > 100:
                _LOGGER.info(f"HTTP TTS fallback succeeded: {len(audio_bytes)} bytes")
                return "wav", audio_bytes
            else:
                _LOGGER.error("HTTP TTS fallback also failed")
                return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None
        finally:
            self.coordinator._tts_request = False
            self.coordinator.tts_audio_future = None
