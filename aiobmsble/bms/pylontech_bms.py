"""Module to support Pylontech RT-series BMS (RT12100, RT12200, RT24100, ...).

Project: aiobmsble, https://pypi.org/p/aiobmsble/

See docs/pylontech_bms.md for protocol details and register map.
"""

import re
from typing import Final

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.backends.device import BLEDevice
from bleak.uuids import normalize_uuid_str

from aiobmsble import BMSDp, BMSInfo, BMSSample, MatcherPattern
from aiobmsble.basebms import BaseBMS, b2str, crc_modbus


class BMS(BaseBMS):
    """Pylontech RT series BMS implementation."""

    INFO: BMSInfo = {
        "default_manufacturer": "Pylontech",
        "default_model": "RT series",
    }

    _DEV_ID: Final[int] = 1

    _REG_SN:       Final[int] = 0x2000
    _SN_COUNT:     Final[int] = 8

    # Contiguous block 0x1016-0x1022 = 13 registers
    _BLOCK_START: Final[int] = 0x1016
    _BLOCK_COUNT: Final[int] = 13

    # Byte offsets within the Modbus response payload: (register - 0x1016) * 2
    _FIELDS: Final[tuple[BMSDp, ...]] = (
        BMSDp("voltage", 0, 2, False, lambda x: x * 0.01),
        BMSDp("current", 2, 2, True,  lambda x: x / 10),
        BMSDp("battery_level", 12, 2, False),
        BMSDp("battery_health", 14, 2, False),
        BMSDp("power", 16, 2, False, float),
        BMSDp("design_capacity", 24, 2, False, lambda x: x // 10),
    )

    # Lookup: nominal voltage -> LFP cells in series
    _VOLTAGE_TO_CELLS: Final[dict[int, int]] = {12: 4, 24: 8, 36: 12, 48: 16}

    # Nominal LFP cell voltage used to convert lifetime energy (kWh) -> total charge (Ah)
    _LFP_CELL_VOLTAGE: Final[float] = 3.2

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_model(name: str) -> tuple[int, int]:
        """Extract (capacity_ah, cell_count) from BLE device name.

        Name format: "RT{vv}{ccc}[-suffix]", e.g. "RT12100-710003".
        Falls back to (100, 4) for unknown names (RT12100 default).
        """
        m = re.search(r"RT(\d{2})(\d+)", name or "")
        if m:
            voltage_v   = int(m.group(1))
            capacity_ah = int(m.group(2))
            return capacity_ah, BMS._VOLTAGE_TO_CELLS.get(voltage_v, 4)
        return 100, 4

    # ------------------------------------------------------------------
    # BaseBMS static interface
    # ------------------------------------------------------------------

    @staticmethod
    def matcher_dict_list() -> list[MatcherPattern]:
        """Return Bluetooth advertisement matchers.

        The BLE module (Telink TLSR8266) advertises under two possible local names:
          - "RT12100-XXXXXX" (or RT24100 etc.) - set by Pylontech firmware,
            advertises Battery Service UUID (0x180F)
          - "GModule" / "GMod"                 - Telink default fallback name
            (may be truncated in BLE advertisement packets),
            advertises the vendor-specific service UUID
        """
        return [
            {"local_name": "RT[0-9]*", "service_uuid": normalize_uuid_str("180f"), "connectable": True},
            {"local_name": "GModule",  "service_uuid": BMS.uuid_services()[0],     "connectable": True},
            {"local_name": "GMod",     "service_uuid": BMS.uuid_services()[0],     "connectable": True},
        ]

    @staticmethod
    def uuid_services() -> tuple[str, ...]:
        """Return required BLE service UUIDs."""
        return ("00010203-0405-0607-0809-0a0b0c0d1910",)

    @staticmethod
    def uuid_rx() -> str:
        """Return UUID of the notify characteristic (Module -> Phone)."""
        return "00010203-0405-0607-0809-0a0b0c0d2b10"

    @staticmethod
    def uuid_tx() -> str:
        """Return UUID of the write characteristic (Phone -> Module)."""
        return "00010203-0405-0607-0809-0a0b0c0d2b11"

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def __init__(
        self,
        ble_device: BLEDevice,
        keep_alive: bool = True,
        secret: str = "",
        logger_name: str = "",
    ) -> None:
        """Initialize BMS members."""
        super().__init__(ble_device, keep_alive, secret, logger_name)
        self._msg: bytes = b""
        self._exp_len: int = 0
        self._capacity_ah, self._cell_count = BMS._parse_model(self.name)
        self._nominal_voltage: float = self._cell_count * BMS._LFP_CELL_VOLTAGE
        self._log.debug(
            "model parsed from '%s': capacity=%d Ah, cells=%d, nominal=%.1f V",
            self.name, self._capacity_ah, self._cell_count, self._nominal_voltage,
        )

    # ------------------------------------------------------------------
    # Notification handler
    # ------------------------------------------------------------------

    def _notification_handler(
        self,
        _sender: BleakGATTCharacteristic,
        data: bytearray,
    ) -> None:
        """Accumulate BLE fragments; signal when a complete and valid Modbus frame arrives."""
        self._log.debug("RX BLE (%dB): %s", len(data), data.hex(" "))
        self._frame.extend(data)

        if not self._frame or self._frame[0] != BMS._DEV_ID:
            self._log.debug("unexpected SOF - discarding")
            self._frame.clear()
            return

        if len(self._frame) >= 3:
            self._exp_len = (
                5 if self._frame[1] & 0x80 else 3 + self._frame[2] + 2
            )

        if not self._exp_len or len(self._frame) < self._exp_len:
            return

        exp_len = self._exp_len
        frame = bytes(self._frame[: exp_len])
        self._frame.clear()
        self._exp_len = 0

        # Validate: function code, byte count, CRC
        if frame[1] & 0x80:
            self._log.debug("Modbus exception response: 0x%02X", frame[2])
            return
        data_bytes = frame[2]
        if frame[1] != 0x03 or data_bytes != exp_len - 5 or crc_modbus(frame[:-2]) != int.from_bytes(frame[-2:], "little"):
            self._log.debug("invalid frame (bad FC, bytecount or CRC) - discarding")
            return

        self._msg = frame
        self._msg_event.set()

    # ------------------------------------------------------------------
    # Device info
    # ------------------------------------------------------------------

    async def _fetch_device_info(self) -> BMSInfo:
        """Read standard BT device info plus serial number from Modbus registers."""
        info = await super()._fetch_device_info()

        try:
            await self._await_msg(
                BMS._cmd_modbus(
                    dev_id=BMS._DEV_ID, addr=BMS._REG_SN, count=BMS._SN_COUNT
                )
            )
            if sn := b2str(self._msg[3 : 3 + BMS._SN_COUNT * 2]):
                info["serial_number"] = sn
        except TimeoutError:
            pass

        return info

    # ------------------------------------------------------------------
    # Main data update
    # ------------------------------------------------------------------

    async def _async_update(self) -> BMSSample:
        """Read current BMS state and return a BMSSample."""
        await self._await_msg(
            BMS._cmd_modbus(dev_id=BMS._DEV_ID, addr=BMS._BLOCK_START, count=BMS._BLOCK_COUNT)
        )

        result: BMSSample = BMS._decode_data(BMS._FIELDS, self._msg, start=3)
        # Modbus exposes only min/max cell aggregates (0x1018, 0x1019), not per-cell values.
        result["cell_voltages"] = BMS._cell_voltages(self._msg, cells=2, start=7)
        result["temp_values"] = BMS._temp_values(self._msg, values=2, start=11, divider=10)

        # design_capacity from _FIELDS (0x1022 x0.1 Ah); fall back to name-parsed value.
        if not result.get("design_capacity"):
            result["design_capacity"] = self._capacity_ah

        # total_charge [Ah] from lifetime energy register (0x1020 x0.1 kWh)
        result["total_charge"] = int(int.from_bytes(self._msg[23:25], "big") * 100 / self._nominal_voltage)

        return result
