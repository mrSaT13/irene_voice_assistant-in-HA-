# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from homeassistant.components.tts import TextToSpeechEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, API_TTS_WAV
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up TTS platform."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]

    async_add_entities([
        IreneTTSEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity.

    Два подхода:
    1. HTTP GET /ttsWav?text=... — прямой запрос WAV от сервера Ирины.
    2. WebSocket: отправка текста → ожидание out.audio.link/playback-request.
    """

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
        return "ru"

    @property
    def supported_languages(self) -> list[str]:
        return ["ru", "en"]

    @property
    def supported_options(self) -> list[str]:
        return []

    async def async_get_tts_audio(
        self,
        message: str,
        language: str,
        options: dict[str, Any]
    ) -> tuple[str | None, bytes | None]:
        """Load TTS audio from Irene server.

        Стратегия:
        1. Пробуем HTTP /ttsWav — сервер отдаёт WAV напрямую.
        2. Если не вышло — пробуем WebSocket (playback-request).
        3. Если ничего — возвращаем None.
        """
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")

            audio_bytes = await self._try_http_tts(message)
            if audio_bytes:
                return "wav", audio_bytes

            _LOGGER.info("HTTP /ttsWav failed, trying WebSocket approach")
            audio_bytes = await self._try_ws_tts(message)
            if audio_bytes:
                return "wav", audio_bytes

            _LOGGER.warning("All TTS methods failed")
            return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            return None, None

    async def _try_http_tts(self, message: str) -> bytes | None:
        """Пробуем получить WAV через HTTP /ttsWav."""
        try:
            session = async_get_clientsession(self.hass, verify_ssl=False)
            from urllib.parse import quote
            encoded = quote(message, safe="")
            url = f"{self.coordinator.base_url}{API_TTS_WAV}?text={encoded}"
            _LOGGER.info(f"HTTP TTS: {url}")

            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 200:
                    data = await response.read()
                    if len(data) > 100:
                        _LOGGER.info(f"HTTP TTS OK: {len(data)} bytes")
                        return data
                    _LOGGER.warning(f"HTTP TTS response too small: {len(data)} bytes")
                else:
                    _LOGGER.warning(f"HTTP TTS error: {response.status}")
        except Exception as err:
            _LOGGER.warning(f"HTTP TTS failed: {err}")
        return None

    async def _try_ws_tts(self, message: str) -> bytes | None:
        """Пробуем получить WAV через WebSocket (playback-request)."""
        try:
            audio_url = await self.coordinator._get_tts_audio_url(
                message, timeout=20.0
            )
            if not audio_url:
                return None

            full_url = f"{self.coordinator.base_url}{audio_url}"
            _LOGGER.info(f"Downloading TTS audio: {full_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.get(
                full_url,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 200:
                    data = await response.read()
                    _LOGGER.info(f"WS TTS OK: {len(data)} bytes")

                    pending = self.coordinator.get_pending_playback()
                    playback_id = pending.get("playback_id") if pending else None
                    if playback_id:
                        await self.coordinator.send_playback_done(playback_id)

                    return data
                else:
                    _LOGGER.error(f"WS TTS download error: HTTP {response.status}")
        except Exception as err:
            _LOGGER.warning(f"WS TTS failed: {err}")
        return None
