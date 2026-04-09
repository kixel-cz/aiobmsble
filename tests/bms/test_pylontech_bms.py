"""Test the Pylontech RT12100 BMS implementation."""

from collections.abc import Buffer
from typing import Final
from uuid import UUID

import pytest
from bleak.backends.characteristic import BleakGATTCharacteristic

from aiobmsble import BMSSample
from aiobmsble.bms.pylontech_bms import BMS
from aiobmsble.basebms import crc_modbus
from tests.bluetooth import generate_ble_device
from tests.conftest import MockBleakClient
from tests.test_basebms import BMSBasicTests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mb_response(register: int, values: list[int]) -> bytearray:
    """Build a valid Modbus RTU read response for the given register values."""
    data = bytearray()
    data.append(0x01)           # device address
    data.append(0x03)           # function code
    data.append(len(values) * 2)  # byte count
    for v in values:
        data.extend(v.to_bytes(2, "big"))
    crc = crc_modbus(bytes(data))
    data.extend(crc.to_bytes(2, "little"))
    return data


def _mb_exception(code: int) -> bytearray:
    """Build a Modbus RTU exception response."""
    data = bytearray([0x01, 0x83, code])
    crc = crc_modbus(bytes(data))
    data.extend(crc.to_bytes(2, "little"))
    return data


# ---------------------------------------------------------------------------
# Reference values captured from a real Pylontech RT12100G31 device
#
# Raw register values (from actual device log):
#   0x1016: 0x052C = 1324  → 13.24 V
#   0x1017: 0xFFD0 = -48   → -4.8 A
#   0x1018: 0x0CF0 = 3312  → 3.312 V (max cell)
#   0x1019: 0x0CEF = 3311  → 3.311 V (min cell)
#   0x101A: 0x0096 = 150   → 15.0 °C (max temp)
#   0x101B: 0x0096 = 150   → 15.0 °C (min temp)
#   0x101C: 0x005B = 91    → 91 % SoC
#   0x101D: 0x0063 = 99    → 99 % SoH (not exposed in BMSSample)
#   0x101E: 0x003F = 63    → 63 cycles
#   0x1020: 0x0B90 = 2960  → 296.0 kWh total energy
#   0x2000-0x2007: "K220924000710003" (ASCII S/N)
# ---------------------------------------------------------------------------

# Raw bytes for the 9-register main block (0x1016-0x101E)
_MAIN_BLOCK_RAW: Final[list[int]] = [
    0x052C,  # voltage:    13.24 V
    0xFFD0,  # current:    -4.8 A (signed)
    0x0CF0,  # cell max:   3.312 V
    0x0CEF,  # cell min:   3.311 V
    0x0096,  # temp max:   15.0 °C
    0x0096,  # temp min:   15.0 °C
    0x005B,  # SoC:        91 %
    0x0063,  # SoH:        99 % (read but not in BMSSample)
    0x003F,  # cycles:     63
]

# Raw bytes for energy register (0x1020)
_ENERGY_RAW: Final[list[int]] = [0x0B90]  # 2960 → 296.0 kWh

# Serial number as register values (ASCII "K220924000710003")
_SN_RAW: Final[list[int]] = [
    0x4B32,  # 'K', '2'
    0x3230,  # '2', '0'
    0x3932,  # '9', '2'
    0x3430,  # '4', '0'
    0x3030,  # '0', '0'
    0x3731,  # '7', '1'
    0x3030,  # '0', '0'
    0x3033,  # '0', '3'
]

# Modbus request bytes for each query (used as keys in mock response dict)
def _make_request(register: int, count: int) -> bytes:
    frame = bytes([0x01, 0x03]) + register.to_bytes(2, "big") + count.to_bytes(2, "big")
    return frame + crc_modbus(frame).to_bytes(2, "little")


_REQ_MAIN   = _make_request(0x1016, 9)
_REQ_ENERGY = _make_request(0x1020, 1)
_REQ_SN     = _make_request(0x2000, 8)

TX_UUID: Final[str] = BMS.uuid_tx()


def ref_value() -> BMSSample:
    """Return the expected BMSSample for the reference register values above."""
    return {
        "voltage":          13.24,
        "current":          -4.8,
        "power":            round(13.24 * -4.8, 3),
        "battery_level":    91,
        "temp_values":      [15.0, 15.0],
        "temperature":      15.0,
        "cell_voltages":    [3.312, 3.311],
        "delta_voltage":    0.001,
        "cell_count":       2,
        "cycles":           63,
        "total_charge":     296.0,
        "battery_charging": False,
        "problem":          False,
    }


# ---------------------------------------------------------------------------
# BMSBasicTests — covers test_bms_id and test_matcher_dict automatically
# ---------------------------------------------------------------------------

class TestBasicBMS(BMSBasicTests):
    """Run the standard BMS interface tests for Pylontech RT12100."""

    bms_class = BMS


# ---------------------------------------------------------------------------
# Mock BleakClient
# ---------------------------------------------------------------------------

class MockPylontechBleakClient(MockBleakClient):
    """Emulate a Pylontech RT12100 BleakClient."""

    RESP: dict[bytes, bytearray] = {
        _REQ_MAIN:   _mb_response(0x1016, _MAIN_BLOCK_RAW),
        _REQ_ENERGY: _mb_response(0x1020, _ENERGY_RAW),
        _REQ_SN:     _mb_response(0x2000, _SN_RAW),
    }

    def _response(self, char_specifier: str | int, cmd: bytes) -> bytearray:
        """Return the mock response for the given command."""
        if isinstance(char_specifier, str) and char_specifier != TX_UUID:
            return bytearray()
        return self.RESP.get(cmd, bytearray())

    async def write_gatt_char(
        self,
        char_specifier: BleakGATTCharacteristic | int | str | UUID,
        data: Buffer,
        response: bool | None = None,
    ) -> None:
        """Issue write command to GATT and trigger notification with response."""
        await super().write_gatt_char(char_specifier, data, response)
        assert self._notify_callback is not None
        resp = self._response(
            char_specifier if isinstance(char_specifier, str) else str(char_specifier),
            bytes(data),
        )
        if resp:
            self._notify_callback("MockPylontechBleakClient", resp)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_update(patch_bleak_client, keep_alive_fixture: bool) -> None:
    """Test BMS data update returns correct values."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device(), keep_alive_fixture)

    result = await bms.async_update()
    assert result == ref_value()

    # Second call verifies already-connected behaviour
    await bms.async_update()
    assert bms.is_connected is keep_alive_fixture
    await bms.disconnect()


async def test_device_info(patch_bleak_client) -> None:
    """Test that the BMS returns serial number from BMS registers."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    assert info.get("serial_number") == "K220924000710003"
    await bms.disconnect()


# ---------------------------------------------------------------------------
# Invalid response tests
# ---------------------------------------------------------------------------

@pytest.fixture(
    name="wrong_response",
    params=[
        # CRC is wrong
        (
            bytearray(_mb_response(0x1016, _MAIN_BLOCK_RAW)[:-2]) + bytearray([0x00, 0x00]),
            "wrong_CRC",
        ),
        # Modbus exception response (illegal data address)
        (
            _mb_exception(0x02),
            "modbus_exception",
        ),
        # Too short to be a valid response
        (
            bytearray([0x01, 0x03, 0x02]),
            "too_short",
        ),
        # Wrong function code
        (
            bytearray([0x01, 0x04]) + bytearray(20),
            "wrong_FC",
        ),
        # Empty response
        (
            bytearray(),
            "empty",
        ),
    ],
    ids=lambda p: p[1],
)
def fix_wrong_response(request: pytest.FixtureRequest) -> bytearray:
    """Return a faulty response frame."""
    return request.param[0]


async def test_invalid_response(
    monkeypatch: pytest.MonkeyPatch,
    patch_bleak_client,
    patch_bms_timeout,
    wrong_response: bytearray,
) -> None:
    """Test that invalid BMS responses raise TimeoutError."""
    patch_bms_timeout()
    monkeypatch.setattr(
        MockPylontechBleakClient,
        "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: wrong_response},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    with pytest.raises(TimeoutError):
        await bms.async_update()
    await bms.disconnect()


async def test_energy_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    patch_bleak_client,
) -> None:
    """Test that missing energy register does not crash the update."""
    # Remove energy response — BMS should still return data, just without total_charge
    resp_without_energy = {
        k: v for k, v in MockPylontechBleakClient.RESP.items() if k != _REQ_ENERGY
    }
    monkeypatch.setattr(MockPylontechBleakClient, "RESP", resp_without_energy)
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    result = await bms.async_update()
    # Should succeed but total_charge will be absent
    assert "voltage" in result
    assert "total_charge" not in result
    await bms.disconnect()
