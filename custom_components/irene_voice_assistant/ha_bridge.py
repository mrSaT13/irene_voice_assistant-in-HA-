# custom_components/irene_voice_assistant/ha_bridge.py
"""Bridge between Irene and Home Assistant for device control."""

from __future__ import annotations

import logging
import re
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Паттерн для явных команд [HA:domain.service] параметры
HA_COMMAND_PATTERN = re.compile(r'\[HA:([a-z_]+\.[a-z_]+)\]\s*([^\n\r\[\\]]*)', re.IGNORECASE)

# Маппинг русских слов на домены HA
DOMAIN_ALIASES = {
    "свет": "light",
    "лампа": "light",
    "лампочка": "light",
    "лампочки": "light",
    "светильник": "light",
    "люстра": "light",
    "торшер": "light",
    "бра": "light",
    "розетка": "switch",
    "выключатель": "switch",
    "реле": "switch",
    "кондиционер": "climate",
    "обогреватель": "climate",
    "термостат": "climate",
    "температура": "climate",
    "вентилятор": "fan",
    "штора": "cover",
    "жалюзи": "cover",
    "ворота": "cover",
    "дверь": "cover",
    "замок": "lock",
    "пылесос": "vacuum",
    "медиаплеер": "media_player",
    "телевизор": "media_player",
    "колонка": "media_player",
    "музыка": "media_player",
    "сцена": "scene",
    "скрипт": "script",
    "автоматизация": "automation",
    "датчик": "sensor",
    "сенсор": "sensor",
}

# Глаголы действия
ACTION_VERBS = {
    "включи": "turn_on",
    "включить": "turn_on",
    "зажги": "turn_on",
    "зажечь": "turn_on",
    "выключи": "turn_off",
    "выключить": "turn_off",
    "потуши": "turn_off",
    "отключи": "turn_off",
    "отключить": "turn_off",
    "открой": "open_cover",
    "открыть": "open_cover",
    "закрой": "close_cover",
    "закрыть": "close_cover",
    "заблокируй": "lock",
    "разблокируй": "unlock",
}


class HaBridge:
    """Bridge to execute HA commands from Irene messages."""
    
    def __init__(self, hass: HomeAssistant):
        self.hass = hass
        self._enabled = True
        self._entity_cache: dict[str, str] = {}
        self._last_update = None
    
    async def process_message(self, text: str) -> dict[str, Any] | None:
        """Process Irene message and execute HA commands if found."""
        if not self._enabled:
            return None
        
        result = {"executed": [], "errors": []}
        
        # 1. Проверяем явные команды [HA:domain.service] параметры
        matches = HA_COMMAND_PATTERN.findall(text)
        for cmd, params in matches:
            try:
                await self._execute_explicit_command(cmd.strip(), params.strip())
                result["executed"].append(cmd)
            except Exception as err:
                _LOGGER.error(f"HA command error [{cmd}]: {err}")
                result["errors"].append(f"{cmd}: {err}")
        
        # 2. Проверяем неявные команды на русском
        if not result["executed"]:
            implicit = await self._try_implicit_command(text)
            if implicit:
                result["executed"].extend(implicit)
        
        return result if result["executed"] or result["errors"] else None
    
    async def _execute_explicit_command(self, command: str, params: str) -> None:
        """Execute explicit HA command like [HA:light.turn_on] кухня."""
        parts = command.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid command format: {command}")
        
        domain, service = parts
        
        service_data = {}
        if params:
            entity_id = await self._resolve_entity(params)
            if entity_id:
                service_data["entity_id"] = entity_id
            else:
                _LOGGER.warning(f"Entity not found for: {params}")
                return
        
        await self.hass.services.async_call(domain, service, service_data, blocking=True)
        _LOGGER.info(f"HA command executed: {domain}.{service} -> {params}")
    
    async def _try_implicit_command(self, text: str) -> list[str]:
        """Try to parse implicit Russian command."""
        text_lower = text.lower().strip()
        executed = []
        
        # Ищем глагол действия
        verb = None
        service = None
        for v, s in ACTION_VERBS.items():
            if text_lower.startswith(v):
                verb = v
                service = s
                text_lower = text_lower[len(v):].strip()
                break
        
        if not verb:
            return []
        
        # Ищем объект (устройство)
        entity_id = await self._resolve_entity(text_lower)
        if not entity_id:
            return []
        
        # Определяем домен
        domain = entity_id.split(".")[0]
        
        # Проверяем что сервис поддерживается
        if domain in ("light", "switch") and service in ("turn_on", "turn_off"):
            pass
        elif domain == "cover" and service in ("open_cover", "close_cover"):
            pass
        elif domain == "lock" and service in ("lock", "unlock"):
            pass
        else:
            # Для других доменов используем turn_on/turn_off если есть
            if service not in ("turn_on", "turn_off"):
                return []
        
        try:
            await self.hass.services.async_call(
                domain, 
                service, 
                {"entity_id": entity_id},
                blocking=True
            )
            executed.append(f"{domain}.{service}({entity_id})")
            _LOGGER.info(f"Implicit command executed: {verb} {text_lower} -> {entity_id}")
        except Exception as err:
            _LOGGER.error(f"Implicit command error: {err}")
        
        return executed
    
    async def _resolve_entity(self, target: str) -> str | None:
        """Resolve human-readable target to entity_id."""
        if not target:
            return None
        
        target_lower = target.lower().strip()
        
        # Обновляем кэш если нужно
        await self._update_cache()
        
        # 1. Точное совпадение в кэше
        if target_lower in self._entity_cache:
            return self._entity_cache[target_lower]
        
        # 2. Частичное совпадение в friendly_name
        for friendly, entity_id in self._entity_cache.items():
            if target_lower in friendly or friendly in target_lower:
                return entity_id
        
        # 3. Поиск по entity_id
        for state in self.hass.states.async_all():
            if target_lower in state.entity_id:
                return state.entity_id
        
        return None
    
    async def _update_cache(self) -> None:
        """Update entity cache from HA states."""
        from homeassistant.util import dt as dt_util
        from datetime import timedelta
        
        # Обновляем раз в 5 минут
        if self._last_update and (dt_util.utcnow() - self._last_update) < timedelta(minutes=5):
            return
        
        self._entity_cache.clear()
        
        for state in self.hass.states.async_all():
            friendly = state.attributes.get("friendly_name", "")
            if friendly:
                self._entity_cache[friendly.lower()] = state.entity_id
        
        self._last_update = dt_util.utcnow()
        _LOGGER.debug(f"Entity cache updated: {len(self._entity_cache)} entities")