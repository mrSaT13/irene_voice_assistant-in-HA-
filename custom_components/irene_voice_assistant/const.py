# custom_components/irene_voice_assistant/const.py
"""Constants for Irene Voice Assistant integration."""

from homeassistant.const import Platform

DOMAIN = "irene_voice_assistant"

DEFAULT_PORT = 8186
DEFAULT_NAME = "Irene"
DEFAULT_RETURN_FORMAT = "text"
DEFAULT_REFRESH_INTERVAL = 60

CONF_RETURN_FORMAT = "return_format"
CONF_REFRESH_INTERVAL = "refresh_interval"
CONF_WS_HEARTBEAT = "ws_heartbeat"

# ✅ НОВЫЕ константы для TTS на колонку
CONF_MEDIA_PLAYER = "media_player_entity"
CONF_TTS_MODE = "tts_mode"

# Режимы озвучки
TTS_MODE_IRENE = "irene"  # Только через Ирину (её серверный TTS)
TTS_MODE_MEDIA_PLAYER = "media_player"  # Только через колонку (голос Ирины!)
TTS_MODE_BOTH = "both"  # И там и там

TTS_MODES = {
    TTS_MODE_IRENE: "🎤 Только сервер Ирины",
    TTS_MODE_MEDIA_PLAYER: "🔊 Только колонка (голос Ирины!)",
    TTS_MODE_BOTH: "🎤🔊 Обе (сервер + колонка)",
}

# Платформы
PLATFORMS = [
    Platform.CONVERSATION,
    Platform.NOTIFY,
    Platform.SENSOR,
    Platform.TTS,
    Platform.STT,
]

# WebSocket protocols
PROTOCOL_IN_TEXT_DIRECT = "in.text-direct"
PROTOCOL_IN_TEXT_INDIRECT = "in.text-indirect"
PROTOCOL_IN_STT_CLIENTSIDE = "in.stt.clientside"
PROTOCOL_IN_STT_SERVERSIDE = "in.stt.serverside"
PROTOCOL_OUT_TEXT_PLAIN = "out.text-plain"
PROTOCOL_OUT_AUDIO_LINK = "out.audio.link"
PROTOCOL_OUT_TTS_SERVERSIDE = "out.tts.serverside"
PROTOCOL_IN_MUTE = "in.mute"

# WebSocket message types
MSG_NEGOTIATE_REQUEST = "negotiate/request"
MSG_NEGOTIATE_AGREE = "negotiate/agree"
MSG_IN_TEXT_DIRECT_TEXT = "in.text-direct/text"
MSG_IN_TEXT_INDIRECT_TEXT = "in.text-indirect/text"
MSG_IN_STT_CLIENTSIDE_RECOGNIZED = "in.stt.clientside/recognized"
MSG_IN_STT_CLIENTSIDE_PROCESSED = "in.stt.clientside/processed"
MSG_OUT_TEXT_PLAIN_TEXT = "out.text-plain/text"
MSG_OUT_AUDIO_LINK_PLAYBACK_REQUEST = "out.audio.link/playback-request"
MSG_OUT_AUDIO_LINK_PLAYBACK_PROGRESS = "out.audio.link/playback-progress"
MSG_OUT_AUDIO_LINK_PLAYBACK_DONE = "out.audio.link/playback-done"
MSG_IN_STT_SERVERSIDE_READY = "in.stt.serverside/ready"
MSG_IN_STT_SERVERSIDE_RECOGNIZED = "in.stt.serverside/recognized"
MSG_IN_STT_SERVERSIDE_PROCESSED = "in.stt.serverside/processed"
MSG_IN_MUTE_MUTE = "in.mute/mute"
MSG_IN_MUTE_UNMUTE = "in.mute/unmute"

# API endpoints
API_CONFIGS = "/api/config/configs"
API_NOTIFY = "/api/notification_api/notify"
API_PLUGINS = "/api/discover_plugins/plugins"
API_WEBSOCKET = "/api/face_web/ws"
API_AUDIO_FILE = "/api/web-audio-link-output/files/{file_name}"

# ✅ СТАРЫЕ HTTP endpoints (fallback для совместимости)
API_TTS_WAV = "/ttsWav"
API_SEND_TXT_CMD = "/sendTxtCmd"
API_WSMIC = "/wsmic"

# Panel
PANEL_URL = "/irene_voice_assistant/panel"
PANEL_ICON = "mdi:robot-happy"
PANEL_TITLE = "Ирина"
