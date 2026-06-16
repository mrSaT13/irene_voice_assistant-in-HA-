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

    async_add_entities([
        IreneTTSEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene TTS entity registered: {config_entry.title}")


class IreneTTSEntity(TextToSpeechEntity):
    """Irene TTS entity.

    Использует протокол out.tts.serverside Ирины:
    1. Запрашивает у сервера URL WAV-файла через WebSocket.
    2. Скачивает WAV по этому URL.
    3. Отправляет playback-done СРАЗУ после скачивания (файл доступен клиенту).
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
        """Load TTS from Irene.

        Возвращает (format, audio_bytes) или (None, None) при ошибке.
        """
        try:
            _LOGGER.info(f"TTS request: '{message}' (lang: {language})")

            # Сбрасываем предыдущий pending playback, чтобы не подцепить чужой
            self.coordinator._pending_playback = None

            # 1. Получаем URL WAV файла от Ирины через WebSocket
            audio_url = await self.coordinator._get_tts_audio_url(
                message, timeout=15.0
            )

            if not audio_url:
                _LOGGER.warning("Failed to get TTS audio URL from Irene")
                return None, None

            # 2. Берём текущий pending playback с playbackId
            #    (он был установлен в _handle_ws_message при получении playback-request)
            pending = self.coordinator.get_pending_playback()
            playback_id = pending.get("playback_id") if pending else None

            # 3. Формируем полный URL и скачиваем
            full_url = f"{self.coordinator.base_url}{audio_url}"
            _LOGGER.info(f"Downloading TTS audio from: {full_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            async with session.get(
                full_url,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status == 200:
                    audio_bytes = await response.read()
                    _LOGGER.info(
                        f"TTS audio downloaded: {len(audio_bytes)} bytes"
                    )

                    # ✅ 4. ОТПРАВЛЯЕМ playback-done ПОСЛЕ СКАЧИВАНИЯ!
                    # Это критично: без этого сервер не знает, что файл ушёл клиенту,
                    # и может не очистить временный файл или зависнуть.
                    if playback_id:
                        await self.coordinator.send_playback_done(playback_id)
                    else:
                        _LOGGER.warning(
                            "No playback_id found in pending playback, "
                            "skipping done notification"
                        )

                    return "wav", audio_bytes
                else:
                    _LOGGER.error(
                        f"Failed to download TTS audio: HTTP {response.status}"
                    )
                    # Всё равно отправляем done, чтобы сервер не завис
                    if playback_id:
                        await self.coordinator.send_playback_done(playback_id)
                    return None, None

        except Exception as err:
            _LOGGER.error(f"TTS error: {err}", exc_info=True)
            # В случае ошибки — всё равно закрываем playback
            pending = self.coordinator.get_pending_playback()
            if pending and pending.get("playback_id"):
                try:
                    await self.coordinator.send_playback_done(
                        pending["playback_id"]
                    )
                except Exception:
                    pass
            return None, None
