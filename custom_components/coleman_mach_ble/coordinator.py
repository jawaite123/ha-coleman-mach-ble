"""Data coordinator for Coleman Mach BLE thermostat."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta

from bleak_retry_connector import establish_connection, BleakNotFoundError, BleakClientWithServiceCache

from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    DOMAIN,
    CHAR_ROOM_TEMPERATURE,
    CHAR_ZONE_ID,
    CHAR_MODE_OPERATION,
    CHAR_AVAILABLE_MODE,
    CHAR_SET_POINT,
    CHAR_CELSIUS,
    CHAR_UNIT_ID,
    READ_ORDER,
)

_LOGGER = logging.getLogger(__name__)

BLE_READ_TIMEOUT = 10.0  # seconds


@dataclass
class ColemanMachData:
    room_temperature: float | None = None
    set_point: int | None = None
    mode_operation: str | None = None
    available_modes: int | None = None
    is_celsius: bool = False
    zone_name: str | None = None
    unit_id: str | None = None


def _parse_string(raw: bytes, max_len: int) -> str:
    try:
        return raw[:max_len].rstrip(b"\x00").decode("ascii", errors="replace").strip()
    except Exception:
        return ""


def _parse_data(raw_data: dict[str, bytes]) -> ColemanMachData:
    d = ColemanMachData()

    if (v := raw_data.get(CHAR_CELSIUS)) and len(v) >= 1:
        d.is_celsius = (v[0] == 1)

    if (v := raw_data.get(CHAR_SET_POINT)) and len(v) >= 1:
        d.set_point = v[0]

    if (v := raw_data.get(CHAR_ROOM_TEMPERATURE)) and len(v) >= 1:
        d.room_temperature = float(v[0])

    if (v := raw_data.get(CHAR_MODE_OPERATION)):
        d.mode_operation = _parse_string(v, 14)

    if (v := raw_data.get(CHAR_ZONE_ID)):
        d.zone_name = _parse_string(v, 7)

    if (v := raw_data.get(CHAR_UNIT_ID)):
        d.unit_id = _parse_string(v, 3)

    if (v := raw_data.get(CHAR_AVAILABLE_MODE)) and len(v) >= 1:
        d.available_modes = v[0]

    return d


def _get_ble_device(hass: HomeAssistant, mac_address: str):
    device = bluetooth.async_ble_device_from_address(hass, mac_address, connectable=True)
    if device is None:
        raise UpdateFailed(
            f"Device {mac_address} not found in BLE scan — is the AC powered on and in range?"
        )
    return device


async def _read_chars(client: BleakClientWithServiceCache, mac_address: str) -> ColemanMachData:
    raw_data: dict[str, bytes] = {}
    for char_uuid in READ_ORDER:
        try:
            value = await asyncio.wait_for(
                client.read_gatt_char(char_uuid),
                timeout=BLE_READ_TIMEOUT,
            )
            raw_data[char_uuid] = bytes(value)
            _LOGGER.debug("Read %s: %s", char_uuid, bytes(value).hex())
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout reading characteristic %s", char_uuid)
        except Exception as err:
            _LOGGER.warning("Error reading characteristic %s: %s", char_uuid, err)

    if not raw_data:
        raise UpdateFailed("No data received from device")

    return _parse_data(raw_data)


class ColemanMachCoordinator(DataUpdateCoordinator[ColemanMachData]):
    """Coordinator that polls the Coleman Mach BLE thermostat."""

    def __init__(self, hass: HomeAssistant, mac_address: str, interval: int) -> None:
        self.mac_address = mac_address
        self._ble_lock = asyncio.Lock()
        self._client: BleakClientWithServiceCache | None = None
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{mac_address}",
            update_interval=timedelta(seconds=interval),
        )

    def _on_disconnect(self, client: BleakClientWithServiceCache) -> None:
        _LOGGER.debug("Disconnected from %s", self.mac_address)
        self._client = None

    async def _ensure_connected(self) -> BleakClientWithServiceCache:
        if self._client and self._client.is_connected:
            return self._client
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None
        device = _get_ble_device(self.hass, self.mac_address)
        _LOGGER.debug("Connecting to %s (rssi=%s)", self.mac_address, getattr(device, "rssi", "unknown"))
        try:
            self._client = await establish_connection(
                BleakClientWithServiceCache,
                device,
                self.mac_address,
                disconnected_callback=self._on_disconnect,
            )
        except BleakNotFoundError as err:
            raise UpdateFailed(f"Device {self.mac_address} not found: {err}") from err
        except Exception as err:
            raise UpdateFailed(f"BLE connection failed: {err}") from err
        _LOGGER.info("Connected to %s", self.mac_address)
        return self._client

    async def _async_update_data(self) -> ColemanMachData:
        async with self._ble_lock:
            client = await self._ensure_connected()
            try:
                return await _read_chars(client, self.mac_address)
            except Exception:
                self._client = None
                raise

    async def write_set_point(self, value: int) -> None:
        async with self._ble_lock:
            client = await self._ensure_connected()
            try:
                await client.write_gatt_char(CHAR_SET_POINT, bytes([value]))
                _LOGGER.debug("Wrote set_point=%d to %s", value, self.mac_address)
            except Exception as err:
                self._client = None
                raise UpdateFailed(f"Failed to write set_point: {err}") from err

    async def write_mode(self, mode: str) -> None:
        async with self._ble_lock:
            client = await self._ensure_connected()
            try:
                await client.write_gatt_char(CHAR_MODE_OPERATION, mode.encode("ascii"))
                _LOGGER.debug("Wrote mode=%r to %s", mode, self.mac_address)
            except Exception as err:
                self._client = None
                raise UpdateFailed(f"Failed to write mode: {err}") from err

    async def async_shutdown(self) -> None:
        async with self._ble_lock:
            if self._client and self._client.is_connected:
                await self._client.disconnect()
            self._client = None
