"""Module to support Pylontech RT-series BMS (RT12100, RT12200, RT24100, ...).

Project: aiobmsble, https://pypi.org/p/aiobmsble/

Protocol: Modbus RTU over BLE SPP (Telink TLSR8266 chip)
BLE characteristics:
  RX (notify):  0x..2b10  "Gmodule SPP: Module->Phone"
  TX (write):   0x..2b11  "Gmodule SPP: Phone->Module"

Register map (Modbus FC=0x03, device address=1):
  0x1016: voltage       uint16  x0.01   V
  0x1017: current       int16   x0.1    A   (negative = discharging)
  0x1018: max cell V    uint16  x0.001  V
  0x1019: min cell V    uint16  x0.001  V
  0x101A: max temp      int16   x0.1    degC
  0x101B: min temp      int16   x0.1    degC
  0x101C: SoC           uint16  x1      %
  0x101D: SOH           uint16  x1      %
  0x101E: cycle count   uint16  x1
  0x1020: total energy  uint16  x0.1    kWh (lifetime accumulated)
  0x2000: serial number ASCII, 16 chars across 8 registers

Capacity and cell count are derived from the model number encoded in the BLE
device name (e.g. "RT12100" -> 12V / 4 cells / 100Ah) or serial number prefix.
Validated on: Pylontech RT12100G31 (S/N K220924000710003)
"""

from functools import cache
import re
from typing import Final

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.backends.device import BLEDevice

from aiobmsble import BMSInfo, BMSSample, MatcherPattern
from aiobmsble.basebms import BaseBMS, crc_modbus


class BMS(BaseBMS):
    """Pylontech RT12100 BMS implementation."""

    INFO: BMSInfo = {
        "default_manufacturer": "Pylontech",
        "default_model": "RT series",
    }

    # Modbus device address
    _MB_ADDR: Final[int] = 1

    # Register addresses
    _REG_VOLTAGE:  Final[int] = 0x1016
    _REG_CURRENT:  Final[int] = 0x1017
    _REG_CELL_MAX: Final[int] = 0x1018
    _REG_CELL_MIN: Final[int] = 0x1019
    _REG_TEMP_MAX: Final[int] = 0x101A
    _REG_TEMP_MIN: Final[int] = 0x101B
    _REG_SOC:      Final[int] = 0x101C
    _REG_SOH:      Final[int] = 0x101D
    _REG_CYCLES:   Final[int] = 0x101E
    _REG_ENERGY:   Final[int] = 0x1020
    _REG_SN:       Final[int] = 0x2000

    # Contiguous block 0x1016-0x101E = 9 registers
    _BLOCK_START: Final[int] = _REG_VOLTAGE
    _BLOCK_COUNT: Final[int] = _REG_CYCLES - _REG_VOLTAGE + 1  # 9

    # Fallback capacity when model cannot be determined from name/serial
    _DEFAULT_CAPACITY_AH: Final[float] = 100.0

    # Lookup table: voltage (V) -> number of LFP cells in series
    # All Pylontech RT batteries use 3.2V nominal LFP cells
    _VOLTAGE_TO_CELLS: Final[dict[int, int]] = {
        12: 4,   # 12V  = 4S
        24: 8,   # 24V  = 8S
        36: 12,  # 36V  = 12S
        48: 16,  # 48V  = 16S
    }

    @staticmethod
    def _parse_model(name: str) -> tuple[int, int, int]:
        """Extract (voltage, capacity_ah, cell_count) from device name or serial number.

        Device name format:  "RT{voltage}{capacity}-{serial}"
          e.g. "RT12100-710003" -> voltage=12, capacity=100, cells=4
               "RT24100-000001" -> voltage=24, capacity=100, cells=8

        Serial number prefix: first character encodes generation/family but
        not the model — use device name as primary source.

        Returns a tuple of (voltage_v, capacity_ah, cell_count).
        Falls back to (12, 100, 4) if parsing fails.
        """
        # Match "RT{vv}{ccc}" at the start, e.g. RT12100, RT24100, RT12200
        m = re.search(r"RT(\d{2})(\d+)", name or "")
        if m:
            voltage_v    = int(m.group(1))
            capacity_ah  = int(m.group(2))
            cell_count   = BMS._VOLTAGE_TO_CELLS.get(voltage_v, 4)
            return voltage_v, capacity_ah, cell_count
        return 12, 100, 4  # safe fallback for RT12100

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
        self._exp_len: int = 0  # expected response length in bytes

        # Derive capacity and cell count from the BLE device name when available.
        # The name "RT12100-XXXXXX" encodes voltage and capacity directly.
        # Falls back to defaults when name is "GModule" (Telink default).
        _dev_name: str = ble_device.name or ""
        _, self._capacity_ah, self._cell_count = BMS._parse_model(_dev_name)
        self._log.debug(
            "model parsed from name '%s': capacity=%dAh cells=%d",
            _dev_name, self._capacity_ah, self._cell_count,
        )

    # ------------------------------------------------------------------
    # Static interface required by BaseBMS
    # ------------------------------------------------------------------

    @staticmethod
    def matcher_dict_list() -> list[MatcherPattern]:
        """Return Bluetooth advertisement matchers.

        The BLE module (Telink TLSR8266) advertises under two possible local names:
          - "RT12100-XXXXXX" when Pylontech firmware sets the device name
          - "GModule"        when the module falls back to its Telink default name

        The advertised service UUIDs differ depending on pairing state:
          - Unpaired: standard BT profiles (HID 0x1812, Battery 0x180F)
          - Paired/connected: proprietary Telink SPP service (0x..1910)
        Therefore service_uuid is not used as a matcher criterion.
        """
        return [
            {
                "local_name": "RT[0-9]*",
                "connectable": True,
            },
            {
                "local_name": "GModule",
                "connectable": True,
            },
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
    # Modbus helpers
    # ------------------------------------------------------------------

    @staticmethod
    @cache
    def _cmd(register: int, count: int) -> bytes:
        """Build a Modbus RTU read holding registers (FC=0x03) request."""
        frame = bytes([BMS._MB_ADDR, 0x03]) + register.to_bytes(2, "big") + count.to_bytes(2, "big")
        return frame + crc_modbus(frame).to_bytes(2, "little")

    @staticmethod
    def _parse_regs(data: bytes, count: int) -> list[int] | None:
        """Parse a Modbus RTU response, return list of uint16 or None on error.

        Validates: function code, byte count, and CRC.
        """
        expected = 3 + count * 2 + 2
        if len(data) < expected:
            return None
        if data[1] & 0x80:  # exception response
            return None
        if data[1] != 0x03 or data[2] != count * 2:
            return None
        if crc_modbus(data[:-2]) != int.from_bytes(data[-2:], "little"):
            return None
        return [
            int.from_bytes(data[3 + i * 2 : 5 + i * 2], "big")
            for i in range(count)
        ]

    # ------------------------------------------------------------------
    # Notification handler (called by BaseBMS on RX characteristic)
    # ------------------------------------------------------------------

    def _notification_handler(
        self,
        _sender: BleakGATTCharacteristic,
        data: bytearray,
    ) -> None:
        """Accumulate BLE fragments and signal when a complete frame arrives."""
        self._log.debug("RX BLE data (%dB): %s", len(data), data.hex(" "))
        self._frame.extend(data)

        # A valid Modbus response starts with our device address
        if not self._frame or self._frame[0] != BMS._MB_ADDR:
            self._log.debug("unexpected SOF, discarding frame")
            self._frame.clear()
            return

        # Determine expected length once we have at least 3 bytes
        if len(self._frame) >= 3:
            if self._frame[1] & 0x80:
                self._exp_len = 5  # exception response: addr + FC + code + CRC(2)
            else:
                self._exp_len = 3 + self._frame[2] + 2  # normal response

        if self._exp_len and len(self._frame) >= self._exp_len:
            self._msg = bytes(self._frame[: self._exp_len])
            self._frame.clear()
            self._exp_len = 0
            self._msg_event.set()

    # ------------------------------------------------------------------
    # Device info (override to read serial number from BMS registers)
    # ------------------------------------------------------------------

    async def _fetch_device_info(self) -> BMSInfo:
        """Read device info: standard BT service + serial number from BMS registers."""
        info = await super()._fetch_device_info()

        # Read serial number from Modbus registers 0x2000-0x2007 (8 regs, 16 ASCII chars)
        await self._await_msg(BMS._cmd(BMS._REG_SN, 8))
        regs = self._parse_regs(self._msg, 8)
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
        # Read the contiguous main block (9 registers: 0x1016-0x101E)
        await self._await_msg(BMS._cmd(BMS._BLOCK_START, BMS._BLOCK_COUNT))
        regs = self._parse_regs(self._msg, BMS._BLOCK_COUNT)
        if not regs:
            raise TimeoutError("Failed to read BMS data block")

        def g(reg: int) -> int:
            return regs[reg - BMS._BLOCK_START]

        voltage    = g(BMS._REG_VOLTAGE) * 0.01
        current    = (g(BMS._REG_CURRENT) if g(BMS._REG_CURRENT) < 0x8000 else g(BMS._REG_CURRENT) - 0x10000) * 0.1
        cell_v_max = round(g(BMS._REG_CELL_MAX) * 0.001, 3)
        cell_v_min = round(g(BMS._REG_CELL_MIN) * 0.001, 3)
        temp_max   = (g(BMS._REG_TEMP_MAX) if g(BMS._REG_TEMP_MAX) < 0x8000 else g(BMS._REG_TEMP_MAX) - 0x10000) * 0.1
        temp_min   = (g(BMS._REG_TEMP_MIN) if g(BMS._REG_TEMP_MIN) < 0x8000 else g(BMS._REG_TEMP_MIN) - 0x10000) * 0.1
        soc        = g(BMS._REG_SOC)
        soh        = g(BMS._REG_SOH)
        cycles     = g(BMS._REG_CYCLES)

        result: BMSSample = {
            "voltage":        round(voltage, 2),
            "current":        round(current, 2),
            "battery_level":  soc,
            "battery_health":  soh,
            "temp_values":    [round(temp_max, 1), round(temp_min, 1)],
            "cell_voltages":  [cell_v_max, cell_v_min],
            "cycles":         cycles,
            # cycle_charge [Ah] lets BaseBMS calculate cycle_capacity [Wh] automatically
            "cycle_charge":   round(self._capacity_ah * soc / 100.0, 1),
        }

        # Lifetime accumulated energy (separate register, outside main block)
        # Scale: x0.1 kWh — informational only, logged but not in BMSSample
        # (BMSSample.total_charge expects Ah, not kWh)
        try:
            await self._await_msg(BMS._cmd(BMS._REG_ENERGY, 1))
            e_regs = self._parse_regs(self._msg, 1)
            if e_regs:
                self._log.debug(
                    "lifetime energy: %.1f kWh", e_regs[0] * 0.1
                )
        except TimeoutError:
            self._log.debug("Energy register not available, skipping")

        return result
