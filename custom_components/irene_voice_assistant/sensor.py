# custom_components/irene_voice_assistant/sensor.py
"""Sensor platform for Irene Voice Assistant."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import IreneCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Irene sensors."""
    coordinator: IreneCoordinator = hass.data[DOMAIN][config_entry.entry_id]
    
    entities = [
        IreneStatusSensor(coordinator, config_entry),
        IreneHistorySensor(coordinator, config_entry),
    ]
    
    async_add_entities(entities)


class IreneStatusSensor(CoordinatorEntity, SensorEntity):
    """Sensor for Irene connection status."""
    
    def __init__(self, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._attr_name = f"{coordinator.name} Status"
        self._attr_unique_id = f"{config_entry.entry_id}_status"
        self._attr_icon = "mdi:robot"
    
    @property
    def native_value(self) -> str | None:
        """Return the sensor value."""
        if self.coordinator.data and self.coordinator.data.get("available"):
            return "Online"
        return "Offline"
    
    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional attributes."""
        attrs = {}
        if self.coordinator.data:
            attrs["last_update"] = self.coordinator.data.get("last_update")
        return attrs


class IreneHistorySensor(CoordinatorEntity, SensorEntity):
    """Sensor for Irene chat history count."""
    
    def __init__(self, coordinator: IreneCoordinator, config_entry: ConfigEntry) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._attr_name = f"{coordinator.name} Messages"
        self._attr_unique_id = f"{config_entry.entry_id}_messages"
        self._attr_icon = "mdi:message-text"
    
    @property
    def native_value(self) -> int:
        """Return the number of messages."""
        return len(self.coordinator.chat_history)