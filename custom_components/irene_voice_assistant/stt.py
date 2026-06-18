# custom_components/irene_voice_assistant/stt.py
"""STT platform for Irene Voice Assistant using server-side STT via WebSocket."""

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

from .const import DOMAIN
from .coordinator import IreneCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities([IreneSTTEntity(hass, coordinator, config_entry)])
    _LOGGER.info(f"Irene STT entity registered: {config_entry.title}")


class IreneSTTEntity(SpeechToTextEntity):
    """Irene STT entity using server-side STT via WebSocket."""

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
        try:
            _LOGGER.info(
                f"Starting STT processing "
                f"(lang={metadata.language}, sample_rate={metadata.sample_rate}, "
                f"channels={metadata.channel}, codec={metadata.codec}, bit_rate={metadata.bit_rate})"
            )

            await self.coordinator.ensure_websocket_connected()

            if "in.stt.serverside" not in self.coordinator.agreed_protocols:
                _LOGGER.warning(
                    f"STT protocol not agreed by server. "
                    f"Available: {self.coordinator.agreed_protocols}"
                )
                return SpeechResult(None, SpeechResultState.ERROR)

            # ✅ ИСПРАВЛЕНО: увеличенный таймаут ожидания ready.
            # На холодном старте HA может инициировать STT через несколько секунд
            # после старта — 5 секунд слишком мало, особенно если Ирина стартует.
            if not self.coordinator._stt_serverside_path:
                _LOGGER.info(
                    "Waiting for STT server-side ready event from Irene "
                    f"(currently path={self.coordinator._stt_serverside_path}, "
                    f"event_set={self.coordinator._stt_session_ready.is_set()})..."
                )
                try:
                    await asyncio.wait_for(
                        self.coordinator._stt_session_ready.wait(),
                        timeout=30.0,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.error(
                        f"STT server-side not ready after 30s timeout. "
                        f"path={self.coordinator._stt_serverside_path}, "
                        f"event_set={self.coordinator._stt_session_ready.is_set()}, "
                        f"agreed_protocols={self.coordinator.agreed_protocols}"
                    )
                    return SpeechResult(None, SpeechResultState.ERROR)

            if not self.coordinator._stt_serverside_path:
                _LOGGER.error("STT path not received from Irene")
                return SpeechResult(None, SpeechResultState.ERROR)

            # ✅ Без sample_rate сервер по умолчанию ждёт 44100 Гц, и WAV 16 кГц
            # интерпретируется как «каша» — поэтому всегда передаём явно.
            stt_ws_url = (
                f"{self.coordinator.ws_base_url}"
                f"{self.coordinator._stt_serverside_path}?sample_rate=16000"
            )
            _LOGGER.info(f"Connecting to STT WS: {stt_ws_url}")

            session = async_get_clientsession(self.hass, verify_ssl=False)
            ssl_ctx = self.coordinator._ssl_context if self.coordinator._ssl_context else False

            # Готовим future ДО открытия WS — иначе можем пропустить ответ
            await self.coordinator.prepare_stt_result()

            chunk_count = 0
            try:
                async with session.ws_connect(
                    stt_ws_url, timeout=15.0, ssl=ssl_ctx
                ) as stt_ws:
                    _LOGGER.info("STT WS connected, streaming audio")

                    async for chunk in stream:
                        # Если Ирина сама сейчас говорит (TTS) — она прислала in.mute/mute.
                        # Не шлём микрофон, чтобы не было эха/фантомного распознавания.
                        if self.coordinator.is_muted:
                            _LOGGER.debug("Skipping chunk — microphone muted (TTS playing)")
                            continue

                        if chunk:
                            await stt_ws.send_bytes(chunk)
                            chunk_count += 1

                    _LOGGER.info(
                        f"Sent {chunk_count} audio chunks to STT WS, "
                        "closing stream to signal end-of-speech"
                    )

                    # ✅ ИСПРАВЛЕНО: корректное закрытие WS — это сигнал EOS для Ирины.
                    # Раньше отправлялся send_bytes(b"") что недокументировано
                    # и могло игнорироваться, из-за чего Ирина ждала ещё данных
                    # и не присылала `recognized` — был timeout.
                    try:
                        await stt_ws.close(code=1000, message=b"eof")
                    except Exception as close_err:
                        _LOGGER.debug(f"STT WS close exception (usually fine): {close_err}")

                # WS закрыт — ждём результат распознавания (приходит через основной WS)
                text = await self.coordinator.wait_stt_result(timeout=20.0)

                if text is not None and len(text.strip()) > 0:
                    _LOGGER.info(f"STT recognized: '{text}'")
                    return SpeechResult(text, SpeechResultState.SUCCESS)
                else:
                    _LOGGER.warning(
                        f"STT no text recognized (sent {chunk_count} chunks, "
                        f"mute_during={self.coordinator.is_muted})"
                    )
                    return SpeechResult(None, SpeechResultState.ERROR)

            except aiohttp.ClientError as ws_err:
                _LOGGER.error(f"STT WS connection error: {ws_err}", exc_info=True)
                return SpeechResult(None, SpeechResultState.ERROR)

        except Exception as err:
            _LOGGER.error(f"STT error: {err}", exc_info=True)
            return SpeechResult(None, SpeechResultState.ERROR)
