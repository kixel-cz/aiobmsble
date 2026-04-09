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
    # cycle_charge = 100 Ah * 91% SoC = 91.0 Ah
    # cycle_capacity = 13.24 V * 91.0 Ah = 1204.84 Wh (calculated by BaseBMS)
    return {
        "voltage":          13.24,
        "current":          -4.8,
        "power":            round(13.24 * -4.8, 3),
        "battery_level":    91,
        "cycle_charge":     91.0,
        "cycle_capacity":   round(13.24 * 91.0, 3),
        "temp_values":      [15.0, 15.0],
        "temperature":      15.0,
        "cell_voltages":    [3.312, 3.311],
        "delta_voltage":    0.001,
        "cell_count":       2,
        "cycles":           63,
        "total_charge":     296.0,
        "battery_charging": False,
        "problem":          False,
        "runtime":          int(91.0 / 4.8 * 3600),
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
    patch_bms_timeout,
) -> None:
    """Test that a timeout on the energy register does not crash the update."""
    # Remove energy response — _await_msg will timeout but update must still succeed
    resp_without_energy = {
        k: v for k, v in MockPylontechBleakClient.RESP.items() if k != _REQ_ENERGY
    }
    monkeypatch.setattr(MockPylontechBleakClient, "RESP", resp_without_energy)
    patch_bms_timeout()  # speed up retries
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    result = await bms.async_update()
    # Main data must be present, total_charge absent (energy register timed out)
    assert "voltage" in result
    assert "total_charge" not in result
    await bms.disconnect()


# ---------------------------------------------------------------------------
# Model name parsing tests
# ---------------------------------------------------------------------------

import pytest

@pytest.mark.parametrize(
    ("name", "expected_voltage", "expected_capacity", "expected_cells"),
    [
        ("RT12100-710003", 12, 100, 4),  # validated device
        ("RT12200-000001", 12, 200, 4),  # 12V 200Ah
        ("RT24100-000001", 24, 100, 8),  # 24V 100Ah
        ("RT48100-000001", 48, 100, 16), # 48V 100Ah
        ("RT36050-000001", 36, 50,  12), # 36V  50Ah
        ("GModule",        12, 100, 4),  # Telink default name -> fallback
        ("",               12, 100, 4),  # empty -> fallback
    ],
    ids=["RT12100", "RT12200", "RT24100", "RT48100", "RT36050", "GModule", "empty"],
)
def test_parse_model(
    name: str,
    expected_voltage: int,
    expected_capacity: int,
    expected_cells: int,
) -> None:
    """Test that model name is correctly parsed to voltage, capacity and cell count."""
    voltage, capacity, cells = BMS._parse_model(name)
    assert voltage   == expected_voltage
    assert capacity  == expected_capacity
    assert cells     == expected_cells


async def test_capacity_from_device_name(patch_bleak_client) -> None:
    """Test that capacity is correctly set from the BLE device name."""
    patch_bleak_client(MockPylontechBleakClient)

    # RT12100 -> 100 Ah
    bms_100 = BMS(generate_ble_device(name="RT12100-710003"))
    assert bms_100._capacity_ah == 100
    assert bms_100._cell_count  == 4

    # RT12200 -> 200 Ah
    bms_200 = BMS(generate_ble_device(name="RT12200-000001"))
    assert bms_200._capacity_ah == 200
    assert bms_200._cell_count  == 4

    # GModule -> fallback to 100 Ah
    bms_gmod = BMS(generate_ble_device(name="GModule"))
    assert bms_gmod._capacity_ah == 100
    assert bms_gmod._cell_count  == 4


# ---------------------------------------------------------------------------
# Matcher pattern tests
# ---------------------------------------------------------------------------

import re
from fnmatch import translate


@pytest.mark.parametrize(
    ("local_name", "should_match"),
    [
        ("RT12100-710003", True),   # standard RT12100
        ("RT12200-000001", True),   # RT12200 variant
        ("RT24100-000001", True),   # 24V variant
        ("RT48100-000001", True),   # 48V variant
        ("RT36050-000001", True),   # 36V variant
        ("GModule",        True),   # Telink default name (second matcher)
        ("RT-fake",        False),  # RT without digits
        ("SomeOther",      False),  # unrelated device
        ("",               False),  # empty name
    ],
    ids=[
        "RT12100", "RT12200", "RT24100", "RT48100", "RT36050",
        "GModule", "RT-no-digits", "unrelated", "empty",
    ],
)
def test_matcher_covers_rt_variants(local_name: str, should_match: bool) -> None:
    """Test that matcher_dict_list covers all RT voltage/capacity variants."""
    from aiobmsble.utils import _advertisement_matches
    from aiobmsble.test_data import adv_dict_to_advdata

    adv = adv_dict_to_advdata({"local_name": local_name} if local_name else {})
    matched = any(
        _advertisement_matches(m, adv, "")
        for m in BMS.matcher_dict_list()
    )
    assert matched is should_match


# ---------------------------------------------------------------------------
# _parse_regs unit tests (covers remaining branches)
# ---------------------------------------------------------------------------

def test_parse_regs_exception_response() -> None:
    """Test that Modbus exception response (FC|0x80) returns None."""
    exc = _mb_exception(0x02)
    assert BMS._parse_regs(bytes(exc), 1) is None


def test_parse_regs_wrong_fc() -> None:
    """Test that wrong function code returns None."""
    data = bytearray([0x01, 0x04, 0x02, 0x00, 0x01])  # FC=0x04, not 0x03
    crc = crc_modbus(bytes(data))
    data.extend(crc.to_bytes(2, "little"))
    assert BMS._parse_regs(bytes(data), 1) is None


async def test_notification_handler_bad_sof(patch_bleak_client) -> None:
    """Test that _notification_handler discards frames with wrong device address."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    await bms._connect()
    # Inject a frame starting with wrong device address (0x02 instead of 0x01)
    bad_frame = bytearray([0x02, 0x03, 0x02, 0x00, 0x5B, 0x00, 0x00])
    bms._notification_handler(None, bad_frame)  # type: ignore[arg-type]
    assert len(bms._frame) == 0  # frame must be cleared
    await bms.disconnect()
