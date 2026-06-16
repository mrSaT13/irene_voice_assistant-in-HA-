# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT.

STT открывает СОБСТВЕННЫЙ WebSocket-канал и самостоятельно согласовывает
протокол in.stt.serverside. Это надёжнее, чем пытаться расшарить состояние
между основным WS координатора и STT-платформой (такая схема приводила к
race condition и timeout).
"""

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
    API_WEBSOCKET,
    DOMAIN,
    MSG_NEGOTIATE_AGREE,
    MSG_NEGOTIATE_REQUEST,
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

    async_add_entities([
        IreneSTTEntity(hass, coordinator, config_entry),
    ])

    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT.

    Алгоритм:
    1. Открываем свой собственный WebSocket.
    2. Negotiate ТОЛЬКО in.stt.serverside.
    3. Получаем in.stt.serverside/ready с path.
    4. Открываем ДОПОЛНИТЕЛЬНЫЙ WebSocket по этому path.
    5. Отправляем аудио-чанки + пустые байты (маркер конца).
    6. Ждём in.stt.serverside/recognized.
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
        """Process audio stream via server-side STT."""
        try:
            _LOGGER.info("Starting STT processing")

            # 1. Открываем СВОЙ собственный WebSocket
            ws_url = f"{self.coordinator.ws_base_url}{API_WEBSOCKET}"
            _LOGGER.info(f"STT opening own WS: {ws_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = (
                self.coordinator._ssl_context
                if self.coordinator._ssl_context
                else False
            )

            async with session.ws_connect(
                ws_url,
                timeout=15.0,
                ssl=ssl_ctx,
            ) as main_ws:
                _LOGGER.info("STT WS connected, negotiating STT protocol")

                # 2. Negotiate ТОЛЬКО in.stt.serverside
                negotiate_msg = {
                    "type": MSG_NEGOTIATE_REQUEST,
                    "protocols": [[PROTOCOL_IN_STT_SERVERSIDE]],
                }
                await main_ws.send_json(negotiate_msg)

                # 3. Ждём negotiate/agree
                msg = await asyncio.wait_for(main_ws.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"STT negotiate failed, got type={msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                data = msg.json()
                if data.get("type") != MSG_NEGOTIATE_AGREE:
                    _LOGGER.error(f"STT negotiate rejected: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                _LOGGER.info(f"STT negotiate agreed: {data.get('protocols', [])}")

                # 4. Ждём in.stt.serverside/ready
                msg = await asyncio.wait_for(main_ws.receive(), timeout=10.0)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    _LOGGER.error(f"STT not ready, got type={msg.type}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                data = msg.json()
                if data.get("type") != MSG_IN_STT_SERVERSIDE_READY:
                    _LOGGER.error(f"STT unexpected message: {data}")
                    return SpeechResult(None, SpeechResultState.ERROR)

                stt_path = data.get("path", "")
                if not stt_path:
                    _LOGGER.error("STT ready without path!")
                    return SpeechResult(None, SpeechResultState.ERROR)

                _LOGGER.info(f"STT server ready, audio path: {stt_path}")

                # 5. Открываем ДОПОЛНИТЕЛЬНЫЙ WebSocket по указанному пути
                stt_ws_url = f"{self.coordinator.ws_base_url}{stt_path}"
                _LOGGER.info(f"Connecting to STT audio WS: {stt_ws_url}")

                async with session.ws_connect(
                    stt_ws_url,
                    timeout=15.0,
                    ssl=ssl_ctx,
                ) as stt_ws:
                    _LOGGER.info("STT audio WS connected, sending audio")

                    # 6. Отправляем аудио чанками
                    chunk_count = 0
                    async for chunk in stream:
                        if chunk:
                            await stt_ws.send_bytes(chunk)
                            chunk_count += 1

                    _LOGGER.info(f"Sent {chunk_count} audio chunks")

                    # 7. Маркер конца аудио
                    await stt_ws.send_bytes(b"")

                    # 8. Ждём распознанный текст
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
