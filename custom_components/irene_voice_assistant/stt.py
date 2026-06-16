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
    MSG_IN_STT_SERVERSIDE_RECOGNIZED,
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

    async_add_entities([
        IreneSTTEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT.

    Использует основной WebSocket из coordinator (с согласованным STT-протоколом)
    и открывает дополнительное соединение по пути, который прислал сервер
    в сообщении in.stt.serverside/ready.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: IreneCoordinator,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the STT entity."""
        self.hass = hass
        self.coordinator = coordinator
        self._attr_unique_id = f"{config_entry.entry_id}_stt"
        self._attr_name = f"{coordinator.name} STT"

    @property
    def supported_languages(self) -> list[str]:
        """Return supported languages."""
        return ["ru", "en"]

    @property
    def supported_formats(self) -> list[AudioFormats]:
        """Return supported audio formats."""
        return [AudioFormats.WAV]

    @property
    def supported_codecs(self) -> list[AudioCodecs]:
        """Return supported audio codecs."""
        return [AudioCodecs.PCM]

    @property
    def supported_bit_rates(self) -> list[AudioBitRates]:
        """Return supported bit rates."""
        return [AudioBitRates.BITRATE_16]

    @property
    def supported_sample_rates(self) -> list[AudioSampleRates]:
        """Return supported sample rates."""
        return [AudioSampleRates.SAMPLERATE_16000]

    @property
    def supported_channels(self) -> list[AudioChannels]:
        """Return supported channels."""
        return [AudioChannels.CHANNEL_MONO]

    async def async_process_audio_stream(
        self,
        metadata: SpeechMetadata,
        stream: AsyncIterable[bytes],
    ) -> SpeechResult:
        """Process audio stream via server-side STT.

        Алгоритм:
        1. Убеждаемся, что основной WS подключен (там согласован STT-протокол).
        2. Ждём, пока сервер пришлёт in.stt.serverside/ready с path.
        3. Открываем ДОПОЛНИТЕЛЬНЫЙ WebSocket по этому path.
        4. Отправляем аудио-чанки + пустые байты как маркер конца.
        5. Ждём in.stt.serverside/recognized с распознанным текстом.
        """
        try:
            _LOGGER.info("Starting STT processing")

            # 1. Убеждаемся, что основной WS подключен и согласован STT
            await self.coordinator.ensure_websocket_connected()

            # 2. Ждём, пока сервер пришлёт in.stt.serverside/ready
            if not self.coordinator._stt_serverside_path:
                try:
                    await asyncio.wait_for(
                        self.coordinator._stt_session_ready.wait(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.error(
                        "STT server-side protocol not negotiated "
                        "(no in.stt.serverside/ready from server)"
                    )
                    return SpeechResult(None, SpeechResultState.ERROR)

            if not self.coordinator._stt_serverside_path:
                _LOGGER.error("STT path not received from server")
                return SpeechResult(None, SpeechResultState.ERROR)

            # 3. Открываем ДОПОЛНИТЕЛЬНЫЙ WebSocket по указанному пути
            stt_ws_url = (
                f"{self.coordinator.ws_base_url}"
                f"{self.coordinator._stt_serverside_path}"
            )
            _LOGGER.info(f"Connecting to STT WS: {stt_ws_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = (
                self.coordinator._ssl_context
                if self.coordinator._ssl_context
                else False
            )

            async with session.ws_connect(
                stt_ws_url,
                timeout=15.0,
                ssl=ssl_ctx,
            ) as stt_ws:
                _LOGGER.info("STT WS connected, sending audio")

                # 4. Отправляем аудио чанками
                chunk_count = 0
                async for chunk in stream:
                    if chunk:
                        await stt_ws.send_bytes(chunk)
                        chunk_count += 1

                _LOGGER.info(f"Sent {chunk_count} audio chunks")

                # Маркер конца аудио
                await stt_ws.send_bytes(b"")

                # 5. Ждём распознанный текст
                try:
                    msg = await asyncio.wait_for(stt_ws.receive(), timeout=15.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if data.get("type") == MSG_IN_STT_SERVERSIDE_RECOGNIZED:
                            text = data.get("text", "").strip()
                            _LOGGER.info(f"STT recognized: '{text}'")
                            return SpeechResult(text, SpeechResultState.SUCCESS)
                        else:
                            _LOGGER.error(f"STT unexpected message: {data}")
                            return SpeechResult(None, SpeechResultState.ERROR)
                    else:
                        _LOGGER.error(f"STT unexpected message type: {msg.type}")
                        return SpeechResult(None, SpeechResultState.ERROR)
                except asyncio.TimeoutError:
                    _LOGGER.error("STT timeout waiting for recognition result")
                    return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
