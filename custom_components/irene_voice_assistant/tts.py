# custom_components/irene_voice_assistant/tts.py
"""TTS platform for Irene Voice Assistant.

Архитектура:
  1. POST /api/notification_api/notify {"text": "..."} → Ирина синтезирует
     речь через свой плагин voice.
  2. Ирина шлёт по WS out.audio.link/playback-request с URL готового WAV.
  3. Листенер резолвит future → отдаём URL в HA как результат TTS.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp

from homeassistant.components.tts import TextToSpeechEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, API_NOTIFY
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
    """Irene TTS entity."""

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
        """Load TTS from Irene.

        Использует HTTP /api/notification_api/notify для синтеза,
        ждёт out.audio.link/playback-request по WS, скачивает WAV.
        """
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")

            # 1. Убеждаемся, что основной WS подключён
            await self.coordinator.ensure_websocket_connected()

            # 2. Готовим future ДО отправки HTTP-запроса
            # (Ирина может ответить playback-request очень быстро)
            loop = asyncio.get_event_loop()
            future: asyncio.Future = loop.create_future()
            audio_response_id = f"tts_{int(time.time() * 1000)}"
            self.coordinator._pending_audio_responses[audio_response_id] = future

            # 3. Ставим флаг TTS-запроса чтобы текстовые ответы игнорировались
            self.coordinator._tts_request = True

            try:
                # 4. Отправляем HTTP POST на /api/notification_api/notify.
                # Это документированный способ заставить Ирину проговорить текст.
                url = f"{self.coordinator.base_url}{API_NOTIFY}"
                payload = {"text": message}
                _LOGGER.info(f"POSTing TTS to Irene notify API: {url}")

                async with self.coordinator.session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        _LOGGER.error(
                            f"Notify API returned HTTP {response.status} "
                            f"for TTS message '{message}'"
                        )
                        return None, None
                    _LOGGER.info("TTS notify POST accepted by Irene")

                # 5. Ждём playback-request с URL аудио
                try:
                    audio_url = await asyncio.wait_for(future, timeout=15.0)
                    _LOGGER.info(f"Got TTS audio URL: {audio_url}")
                except asyncio.TimeoutError:
                    _LOGGER.warning(
                        f"Timeout waiting for out.audio.link/playback-request "
                        f"after HTTP notify (15s). Message='{message}'"
                    )
                    return None, None

                # 6. Скачиваем WAV
                full_url = f"{self.coordinator.base_url}{audio_url}"
                _LOGGER.info(f"Downloading TTS audio from: {full_url}")

                session = async_get_clientsession(self.hass, verify_ssl=False)
                async with session.get(
                    full_url, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        _LOGGER.error(
                            f"Failed to download TTS audio: HTTP {response.status}"
                        )
                        return None, None

                    audio_bytes = await response.read()
                    _LOGGER.info(f"TTS audio downloaded: {len(audio_bytes)} bytes")

                # 7. Планируем playback-done после проигрывания (для отслеживания)
                pending = self.coordinator.get_pending_playback()
                if pending and pending.get("playback_id"):
                    self.hass.async_create_task(
                        self.coordinator.schedule_playback_done_after_bytes(
                            pending["playback_id"],
                            audio_bytes,
                        )
                    )

                return "wav", audio_bytes

            finally:
                self.coordinator._pending_audio_responses.pop(audio_response_id, None)
                self.coordinator._tts_request = False

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            self.coordinator._tts_request = False
            return None, None
