"""Sensor platform for Irene Voice Assistant."""
from __future__ import annotations
from typing import Any
from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util
from .const import DOMAIN
from .coordinator import IreneCoordinator

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    entities = [
        IreneStatusSensor(coordinator, config_entry),
        IreneHistorySensor(coordinator, config_entry),
        IreneLastMessageSensor(hass, coordinator, config_entry),
    ]
    async_add_entities(entities)


class IreneStatusSensor(CoordinatorEntity, SensorEntity):
    def __init__(self, coordinator, config_entry):
        super().__init__(coordinator)
        self._attr_name = f"{coordinator.name} Status"
        self._attr_unique_id = f"{config_entry.entry_id}_status"
        self._attr_icon = "mdi:robot"

    @property
    def native_value(self) -> str | None:
        if self.coordinator.data and self.coordinator.data.get("available"):
            return "Online"
        return "Offline"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        attrs = {}
        if self.coordinator.data:
            attrs["last_update"] = self.coordinator.data.get("last_update")
            attrs["ws_status"] = self.coordinator.data.get("ws_status")
            attrs["agreed_protocols"] = self.coordinator.data.get("agreed_protocols", [])
        return attrs


class IreneHistorySensor(CoordinatorEntity, SensorEntity):
    def __init__(self, coordinator, config_entry):
        super().__init__(coordinator)
        self._attr_name = f"{coordinator.name} Messages"
        self._attr_unique_id = f"{config_entry.entry_id}_messages"
        self._attr_icon = "mdi:message-text"

    @property
    def native_value(self) -> int:
        return len(self.coordinator.chat_history)


class IreneLastMessageSensor(SensorEntity):
    """Sensor showing the last message from Irene (including unsolicited)."""
    def __init__(self, hass: HomeAssistant, coordinator: IreneCoordinator, config_entry: ConfigEntry):
        self.hass = hass
        self.coordinator = coordinator
        self._attr_name = f"{coordinator.name} Last Message"
        self._attr_unique_id = f"{config_entry.entry_id}_last_message"
        self._attr_icon = "mdi:chat"
        self._last_message = ""
        self._last_message_time = None
        
        config_entry.async_on_unload(
            hass.bus.async_listen(f"{DOMAIN}_message", self._handle_message)
        )

    @callback
    def _handle_message(self, event):
        data = event.data
        if data.get("type") == "text":
            self._last_message = data.get("content", "")
            self._last_message_time = dt_util.utcnow()
            self.async_write_ha_state()

    @property
    def native_value(self) -> str:
        msg = self._last_message if self._last_message else "Нет сообщений"
        # 🔥 ИСПРАВЛЕНО: Обрезаем до 255 символов, чтобы HA не заменял на unknown
        return msg[:255]

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        return {
            "last_message_time": self._last_message_time,
            "message_count": len(self.coordinator.chat_history),
            # 🔥 ИСПРАВЛЕНО: Полный текст без ограничений по длине хранится здесь
            "full_message": self._last_message, 
        }
