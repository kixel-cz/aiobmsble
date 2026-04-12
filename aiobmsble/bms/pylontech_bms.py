"""Module to support Pylontech RT-series BMS (RT12100, RT12200, RT24100, ...).

Project: aiobmsble, https://pypi.org/p/aiobmsble/

Protocol: Modbus RTU over BLE SPP (Telink TLSR8266 chip)
BLE characteristics:
  RX (notify):  0x..2b10  "Gmodule SPP: Module->Phone"
  TX (write):   0x..2b11  "Gmodule SPP: Phone->Module"

Register map (Modbus FC=0x03, device address=1):
  Contiguous main block 0x1016–0x101E (9 registers):
  0x1016: voltage       uint16  ×0.01   V
  0x1017: current       int16   ×0.1    A   (negative = discharging)
  0x1018: max cell V    uint16  ×0.001  V
  0x1019: min cell V    uint16  ×0.001  V
  0x101A: max temp      int16   ×0.1    °C
  0x101B: min temp      int16   ×0.1    °C
  0x101C: SoC           uint16  ×1      %
  0x101D: SoH           uint16  ×1      %
  0x101E: power         uint16  ×1      W   (absolute, no sign; equals V × |I|)
  0x101F: unknown        – always 0 in all measurements, skipped
  0x1020: lifetime kWh   uint16  ×0.1    kWh (accumulated, e.g. 2976=297.6 kWh)
  0x1021: allowed current uint16  ×0.1    A   (BMS dynamic limit; 300=30A when
                                             charging, 100=10A when discharging)
  0x1022: design cap     uint16  ×0.1    Ah  (1000 = 100.0 Ah for RT12100)
  0x1023: chg V limit    uint16  ×0.01   V   (1410 = 14.10 V)
  0x1024: dischg cutoff  uint16  ×0.01   V   (1080 = 10.80 V)
  0x2000: serial number  ASCII, 16 chars across 8 registers (0x2000–0x2007)
  0x2008: GModule config – Telink TLSR8266 internal params, not BMS data

Capacity and cell count are derived from the model number encoded in the BLE
device name (e.g. "RT12100" → 12 V / 4 cells / 100 Ah) or serial number prefix.

Note on cell_voltages / temp_values:
  Modbus registers 0x1018–0x101B expose only min/max aggregates, not per-cell
  or per-sensor raw values.  We expose them as a two-element list
  [max_value, min_value] so HA sensors and delta_voltage calculation remain
  consistent with other BMS plugins.

Validated on: Pylontech RT12100G31 (S/N K220924000710003)
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
    _REG_POWER:    Final[int] = 0x101E  # absolute power W (sign-less)
    # 0x101F: unknown – always 0 in measurements, skipped
    _REG_ENERGY:   Final[int] = 0x1020  # lifetime accumulated energy ×0.1 kWh
    # 0x1021: unknown (possibly max charge current or MPPT-related)
    _REG_DESIGN_CAP: Final[int] = 0x1022  # design capacity ×0.1 Ah (1000 = 100.0 Ah)
    _REG_CHG_VLIM:   Final[int] = 0x1023  # charge voltage limit ×0.01 V (1410 = 14.10 V)
    _REG_SN:         Final[int] = 0x2000  # serial number (8 regs, 16 ASCII chars)

    # Contiguous main block: 0x1016–0x101E = 9 registers
    _BLOCK_START: Final[int] = _REG_VOLTAGE
    _BLOCK_COUNT: Final[int] = _REG_POWER - _REG_VOLTAGE + 1  # 9

    # Fields decoded from the contiguous block via _decode_data().
    # pos is relative to the first data byte of the Modbus response payload.
    # Offset within block: (register - _REG_VOLTAGE) × 2 bytes.
    _FIELDS: Final[tuple[BMSDp, ...]] = (
        BMSDp("voltage",        (0x1016 - _REG_VOLTAGE) * 2, 2, False, lambda x: x * 0.01),
        BMSDp("current",        (0x1017 - _REG_VOLTAGE) * 2, 2, True,  lambda x: x * 0.1),
        BMSDp("battery_level",  (0x101C - _REG_VOLTAGE) * 2, 2, False),
        BMSDp("battery_health", (0x101D - _REG_VOLTAGE) * 2, 2, False),
        BMSDp("power",          (0x101E - _REG_VOLTAGE) * 2, 2, False, float),
    )

    # Lookup: nominal voltage → number of LFP cells in series
    _VOLTAGE_TO_CELLS: Final[dict[int, int]] = {
        12: 4,
        24: 8,
        36: 12,
        48: 16,
    }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_model(name: str) -> tuple[int, int, int]:
        """Extract (voltage_v, capacity_ah, cell_count) from BLE device name.

        Name format: "RT{vv}{ccc}[-suffix]", e.g. "RT12100-710003".
        Falls back to (12, 100, 4) for unknown names.
        """
        m = re.search(r"RT(\d{2})(\d+)", name or "")
        if m:
            voltage_v   = int(m.group(1))
            capacity_ah = int(m.group(2))
            cell_count  = BMS._VOLTAGE_TO_CELLS.get(voltage_v, 4)
            return voltage_v, capacity_ah, cell_count
        return 12, 100, 4

    @staticmethod
    @cache
    def _cmd(register: int, count: int) -> bytes:
        """Build a Modbus RTU read holding registers (FC=0x03) request."""
        frame = bytes([BMS._MB_ADDR, 0x03]) + register.to_bytes(2, "big") + count.to_bytes(2, "big")
        return frame + crc_modbus(frame).to_bytes(2, "little")

    @staticmethod
    def _parse_regs(data: bytes, count: int) -> list[int] | None:
        """Parse a Modbus RTU response; return list of uint16 or None on error."""
        expected = 3 + count * 2 + 2
        if len(data) < expected:
            return None
        if data[1] & 0x80:
            return None
        if data[1] != 0x03 or data[2] != count * 2:
            return None
        if crc_modbus(data[:-2]) != int.from_bytes(data[-2:], "little"):
            return None
        return [
            int.from_bytes(data[3 + i * 2: 5 + i * 2], "big")
            for i in range(count)
        ]

    # ------------------------------------------------------------------
    # BaseBMS static interface
    # ------------------------------------------------------------------

    @staticmethod
    def matcher_dict_list() -> list[MatcherPattern]:
        """Return Bluetooth advertisement matchers.

        The BLE module (Telink TLSR8266) advertises under two possible local names:
          - "RT12100-XXXXXX" (or RT24100 etc.) – set by Pylontech firmware
          - "GModule"                           – Telink default fallback name

        The device always advertises the vendor-specific service UUID
        00010203-0405-0607-0809-0a0b0c0d1910 regardless of its local name, so we
        include service_uuid in every matcher.  This makes discovery robust against
        stale name caches in the HA Bluetooth stack (e.g. after a bluetoothctl remove
        the host may still know the device under its old "GModule" name until restart).
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
        """Return UUID of the notify characteristic (Module → Phone)."""
        return "00010203-0405-0607-0809-0a0b0c0d2b10"

    @staticmethod
    def uuid_tx() -> str:
        """Return UUID of the write characteristic (Phone → Module)."""
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

        _dev_name: str = ble_device.name or ""
        _, self._capacity_ah, self._cell_count = BMS._parse_model(_dev_name)
        self._log.debug(
            "model parsed from '%s': capacity=%d Ah, cells=%d",
            _dev_name, self._capacity_ah, self._cell_count,
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
            self._log.debug("unexpected SOF – discarding")
            self._frame.clear()
            return

        if len(self._frame) >= 3:
            if self._frame[1] & 0x80:
                self._exp_len = 5
            else:
                self._exp_len = 3 + self._frame[2] + 2

        if self._exp_len and len(self._frame) >= self._exp_len:
            self._msg = bytes(self._frame[: self._exp_len])
            self._frame.clear()
            self._exp_len = 0
            self._msg_event.set()

    # ------------------------------------------------------------------
    # Device info (optional serial number readout)
    # ------------------------------------------------------------------

    async def _fetch_device_info(self) -> BMSInfo:
        """Read standard BT device info plus serial number from Modbus registers."""
        info = await super()._fetch_device_info()

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
        await self._await_msg(BMS._cmd(BMS._BLOCK_START, BMS._BLOCK_COUNT))
        raw = self._parse_regs(self._msg, BMS._BLOCK_COUNT)
        if not raw:
            raise TimeoutError("failed to read BMS data block")

        # Build the raw byte payload that _decode_data expects (strip addr+FC+bytecount)
        data = self._msg[3: 3 + BMS._BLOCK_COUNT * 2]
        result: BMSSample = BMS._decode_data(BMS._FIELDS, data)

        # Helper: get raw uint16 value for a given register address
        def reg(r: int) -> int:
            return raw[r - BMS._REG_VOLTAGE]

        # ---- cell voltages: [max, min] ----
        # Only min/max aggregates are available via Modbus (0x1018, 0x1019).
        # Exposed as a two-element list so BaseBMS can compute delta_voltage.
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

        # ---- cycle_charge [Ah]: BaseBMS uses this to compute cycle_capacity [Wh] ----
        result["cycle_charge"] = round(
            self._capacity_ah * result.get("battery_level", 0) / 100.0, 1
        )

        # ---- design_capacity and lifetime energy: separate reads ----
        # 0x1022: design capacity ×0.1 Ah (e.g. 1000 → 100.0 Ah for RT12100)
        await self._await_msg(BMS._cmd(BMS._REG_DESIGN_CAP, 1))
        dcap = self._parse_regs(self._msg, 1)
        if dcap:
            result["design_capacity"] = round(dcap[0] * 0.1)
            # Update internal capacity so cycle_charge reflects BMS-reported value
            self._capacity_ah = dcap[0] * 0.1
            result["cycle_charge"] = round(
                self._capacity_ah * result.get("battery_level", 0) / 100.0, 1
            )

        # 0x1020: lifetime accumulated energy ×0.1 kWh – informational log only
        await self._await_msg(BMS._cmd(BMS._REG_ENERGY, 1))
        energy = self._parse_regs(self._msg, 1)
        if energy:
            self._log.debug("lifetime energy: %.1f kWh", energy[0] * 0.1)

        # 0x1021: BMS dynamic allowed current ×0.1 A – informational log only
        # Observed: 300 (30.0 A) when charging, 100 (10.0 A) when discharging
        await self._await_msg(BMS._cmd(0x1021, 1))
        alim = self._parse_regs(self._msg, 1)
        if alim:
            self._log.debug("allowed current: %.1f A", alim[0] * 0.1)

        return result
