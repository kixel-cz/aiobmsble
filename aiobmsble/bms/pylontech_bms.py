"""Module to support Pylontech RT-series BMS (RT12100, RT12200, RT24100, ...).

Project: aiobmsble, https://pypi.org/p/aiobmsble/

See docs/pylontech_bms.md for protocol details and register map.
"""

from functools import cache
import re
from typing import Final

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.backends.device import BLEDevice

from aiobmsble import BMSDp, BMSInfo, BMSSample, MatcherPattern
from aiobmsble.basebms import BaseBMS, crc_modbus


class BMS(BaseBMS):
    """Pylontech RT series BMS implementation."""

    INFO: BMSInfo = {
        "default_manufacturer": "Pylontech",
        "default_model": "RT series",
    }

    _MB_ADDR: Final[int] = 1

    # Register addresses kept only where referenced outside _FIELDS
    _REG_CELL_MAX: Final[int] = 0x1018
    _REG_CELL_MIN: Final[int] = 0x1019
    _REG_TEMP_MAX: Final[int] = 0x101A
    _REG_TEMP_MIN: Final[int] = 0x101B
    _REG_SN:       Final[int] = 0x2000

    # Contiguous block 0x1016-0x1022 = 13 registers
    _BLOCK_START: Final[int] = 0x1016
    _BLOCK_COUNT: Final[int] = 13

    # Byte offsets within the Modbus response payload: (register - 0x1016) * 2
    _FIELDS: Final[tuple[BMSDp, ...]] = (
        BMSDp("voltage",          0, 2, False, lambda x: x * 0.01),
        BMSDp("current",          2, 2, True,  lambda x: x * 0.1),
        BMSDp("battery_level",   12, 2, False),
        BMSDp("battery_health",  14, 2, False),
        BMSDp("power",           16, 2, False, float),
        BMSDp("design_capacity", 24, 2, False, lambda x: round(x * 0.1)),
    )

    # Lookup: nominal voltage -> LFP cells in series
    _VOLTAGE_TO_CELLS: Final[dict[int, int]] = {12: 4, 24: 8, 36: 12, 48: 16}

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

    @staticmethod
    @cache
    def _cmd(register: int, count: int) -> bytes:
        """Build a Modbus RTU FC=0x03 request."""
        frame = bytes([BMS._MB_ADDR, 0x03]) + register.to_bytes(2, "big") + count.to_bytes(2, "big")
        return frame + crc_modbus(frame).to_bytes(2, "little")

    @staticmethod
    def _parse_regs(data: bytes, count: int) -> list[int] | None:
        """Parse a Modbus RTU response; return list of uint16 or None on error."""
        if len(data) < 3 + count * 2 + 2:
            return None
        if data[1] & 0x80 or data[1] != 0x03 or data[2] != count * 2:
            return None
        if crc_modbus(data[:-2]) != int.from_bytes(data[-2:], "little"):
            return None
        return [int.from_bytes(data[3 + i * 2: 5 + i * 2], "big") for i in range(count)]

    # ------------------------------------------------------------------
    # BaseBMS static interface
    # ------------------------------------------------------------------

    @staticmethod
    def matcher_dict_list() -> list[MatcherPattern]:
        """Return Bluetooth advertisement matchers.

        The BLE module (Telink TLSR8266) advertises under two possible local names:
          - "RT12100-XXXXXX" (or RT24100 etc.) - set by Pylontech firmware
          - "GModule"                           - Telink default fallback name

        The vendor-specific service UUID is included in advertisement packets and
        is combined with the local name for more reliable discovery — in particular
        when the host Bluetooth stack has cached a stale device name.
        """
        _SVC = BMS.uuid_services()[0]
        return [
            {"local_name": "RT[0-9]*", "service_uuid": _SVC, "connectable": True},
            {"local_name": "GModule",  "service_uuid": _SVC, "connectable": True},
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
        self._log.debug(
            "model parsed from '%s': capacity=%d Ah, cells=%d",
            self.name, self._capacity_ah, self._cell_count,
        )

    # ------------------------------------------------------------------
    # Notification handler
    # ------------------------------------------------------------------

    def _notification_handler(
        self,
        _sender: BleakGATTCharacteristic,
        data: bytearray,
    ) -> None:
        """Accumulate BLE fragments; signal when a complete Modbus frame arrives."""
        self._log.debug("RX BLE (%dB): %s", len(data), data.hex(" "))
        self._frame.extend(data)

        if not self._frame or self._frame[0] != BMS._MB_ADDR:
            self._log.debug("unexpected SOF - discarding")
            self._frame.clear()
            return

        if len(self._frame) >= 3:
            self._exp_len = (
                5 if self._frame[1] & 0x80 else 3 + self._frame[2] + 2
            )

        if self._exp_len and len(self._frame) >= self._exp_len:
            self._msg = bytes(self._frame[: self._exp_len])
            self._frame.clear()
            self._exp_len = 0
            self._msg_event.set()

    # ------------------------------------------------------------------
    # Device info
    # ------------------------------------------------------------------

    async def _fetch_device_info(self) -> BMSInfo:
        """Read standard BT device info plus serial number from Modbus registers."""
        info = await super()._fetch_device_info()

        try:
            await self._await_msg(BMS._cmd(BMS._REG_SN, 8))
            regs = self._parse_regs(self._msg, 8)
        except TimeoutError:
            regs = None
        if regs:
            sn = "".join(
                chr(b)
                for r in regs
                for b in [(r >> 8) & 0xFF, r & 0xFF]
                if 32 <= b < 127
            ).strip()
            if sn:
                info["serial_number"] = sn

        return info

    # ------------------------------------------------------------------
    # Main data update
    # ------------------------------------------------------------------

    async def _async_update(self) -> BMSSample:
        """Read current BMS state and return a BMSSample."""
        await self._await_msg(BMS._cmd(BMS._BLOCK_START, BMS._BLOCK_COUNT))
        raw = self._parse_regs(self._msg, BMS._BLOCK_COUNT)
        if not raw:
            raise TimeoutError("failed to read BMS data block")

        data = self._msg[3: 3 + BMS._BLOCK_COUNT * 2]
        result: BMSSample = BMS._decode_data(BMS._FIELDS, data)

        # ---- cell voltages: [max, min] ----
        # Modbus exposes only min/max aggregates (0x1018, 0x1019), not per-cell values.
        def reg(r: int) -> int:
            return raw[r - BMS._BLOCK_START]

        result["cell_voltages"] = [
            round(reg(BMS._REG_CELL_MAX) * 0.001, 3),
            round(reg(BMS._REG_CELL_MIN) * 0.001, 3),
        ]

        # ---- temperatures: [max, min] ----
        def _signed(v: int) -> int:
            return v if v < 0x8000 else v - 0x10000

        result["temp_values"] = [
            round(_signed(reg(BMS._REG_TEMP_MAX)) * 0.1, 1),
            round(_signed(reg(BMS._REG_TEMP_MIN)) * 0.1, 1),
        ]

        result.setdefault("design_capacity", self._capacity_ah)

        # ---- informational logs ----
        # 0x1020: lifetime accumulated energy x0.1 kWh
        self._log.debug("lifetime energy: %.1f kWh", raw[0x1020 - BMS._BLOCK_START] * 0.1)
        # 0x1021: BMS dynamic allowed current x0.1 A
        self._log.debug("allowed current: %.1f A",  raw[0x1021 - BMS._BLOCK_START] * 0.1)

        return result
