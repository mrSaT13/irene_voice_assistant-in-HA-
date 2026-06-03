# custom_components/irene_voice_assistant/const.py
"""Constants for Irene Voice Assistant integration."""

from typing import Final

DOMAIN = "irene_voice_assistant"
NAME = "Irene Voice Assistant"

DEFAULT_PORT = 5003
DEFAULT_RETURN_FORMAT = "saywav"
DEFAULT_NAME = "Irene"

CONF_RETURN_FORMAT = "return_format"
CONF_MAX_HISTORY = "max_history"

RETURN_FORMATS = {
    "none": "TTS on server",
    "saytxt": "Text response (TTS on client)",
    "saywav": "WAV audio response",
}

PLATFORMS = [
    "conversation",
    "assist_pipeline",
    "notify",
    "sensor",
    "switch",
]

# Services
SERVICE_SEND_COMMAND = "send_command"
SERVICE_SEND_RAW_TEXT = "send_raw_text"
SERVICE_TTS_SAY = "tts_say"
SERVICE_GET_CHAT_HISTORY = "get_chat_history"
SERVICE_CLEAR_CHAT_HISTORY = "clear_chat_history"

# Events
EVENT_IRENE_RESPONSE = "irene_response"