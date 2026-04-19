"""Test the Pylontech RT12100 BMS implementation."""

from collections.abc import Buffer
from typing import Final
from uuid import UUID

import pytest
from bleak.backends.characteristic import BleakGATTCharacteristic

from aiobmsble import BMSSample
from bleak.uuids import normalize_uuid_str
from aiobmsble.basebms import crc_modbus
from aiobmsble.bms.pylontech_bms import BMS
from aiobmsble.test_data import adv_dict_to_advdata
from aiobmsble.utils import _advertisement_matches
from tests.bluetooth import generate_ble_device
from tests.conftest import MockBleakClient
from tests.test_basebms import BMSBasicTests

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mb_response(values: list[int]) -> bytearray:
    """Build a valid Modbus RTU read response for the given register values."""
    data = bytearray([0x01, 0x03, len(values) * 2])
    for v in values:
        data.extend(v.to_bytes(2, "big"))
    data.extend(crc_modbus(bytes(data)).to_bytes(2, "little"))
    return data


def _mb_exception(code: int) -> bytearray:
    """Build a Modbus RTU exception response."""
    data = bytearray([0x01, 0x83, code])
    data.extend(crc_modbus(bytes(data)).to_bytes(2, "little"))
    return data


# ---------------------------------------------------------------------------
# Reference values captured from a real Pylontech RT12100G31 device
#
# Block 0x1016-0x1022 (13 registers):
#   0x1016: 0x052C = 1324  ->  13.24 V
#   0x1017: 0xFFD0 = -48   ->  -4.8 A  (discharging)
#   0x1018: 0x0CF0 = 3312  ->   3.312 V (max cell)
#   0x1019: 0x0CEF = 3311  ->   3.311 V (min cell)
#   0x101A: 0x0096 =  150  ->  15.0 °C (max temp)
#   0x101B: 0x0096 =  150  ->  15.0 °C (min temp)
#   0x101C: 0x005B =   91  ->  91 % SoC
#   0x101D: 0x0063 =   99  ->  99 % SoH
#   0x101E: 0x003F =   63  ->  63 W  (power)
#   0x101F: 0x0000 =    0  ->  unknown, always 0
#   0x1020: 0x0B90 = 2960  -> 296.0 kWh (lifetime energy, log only)
#   0x1021: 0x03E8 = 1000  -> 100.0 A  (allowed current, log only)
#   0x1022: 0x03E8 = 1000  -> 100.0 Ah (design capacity)
# ---------------------------------------------------------------------------

_MAIN_BLOCK: Final[list[int]] = [
    0x052C,  # 0x1016 voltage:         13.24 V
    0xFFD0,  # 0x1017 current:         -4.8 A
    0x0CF0,  # 0x1018 cell max:        3.312 V
    0x0CEF,  # 0x1019 cell min:        3.311 V
    0x0096,  # 0x101A temp max:        15.0 °C
    0x0096,  # 0x101B temp min:        15.0 °C
    0x005B,  # 0x101C SoC:             91 %
    0x0063,  # 0x101D SoH:             99 %
    0x003F,  # 0x101E power:           63 W
    0x0000,  # 0x101F unknown:         0
    0x0B90,  # 0x1020 lifetime energy: 296.0 kWh (log only)
    0x03E8,  # 0x1021 allowed current: 100.0 A   (log only)
    0x03E8,  # 0x1022 design capacity: 100.0 Ah
]

_SN_RAW: Final[list[int]] = [
    0x4132, 0x3331, 0x3031, 0x3530,  # 'A231015'
    0x3030, 0x3030, 0x3030, 0x3031,  # '00000001'
]


def _make_request(register: int, count: int) -> bytes:
    frame = bytes([0x01, 0x03]) + register.to_bytes(2, "big") + count.to_bytes(2, "big")
    return frame + crc_modbus(frame).to_bytes(2, "little")


_REQ_MAIN = _make_request(0x1016, 13)
_REQ_SN   = _make_request(0x2000, 8)
TX_UUID: Final[str] = BMS.uuid_tx()


def ref_value() -> BMSSample:
    """Expected BMSSample for the reference register values above."""
    return {
        "voltage":          13.24,
        "current":          -4.8,
        "power":            63.0,
        "battery_level":    91,
        "battery_health":   99,
        "design_capacity":  100,
        "cycle_charge":     91.0,
        "cycle_capacity":   round(13.24 * 91.0, 3),
        "temp_values":      [15.0, 15.0],
        "temperature":      15.0,
        "cell_voltages":    [3.312, 3.311],
        "delta_voltage":    0.001,
        "cell_count":       2,
        "battery_charging": False,
        "problem":          False,
        "runtime":          int(91.0 / 4.8 * 3600),
    }


# ---------------------------------------------------------------------------
# BMSBasicTests
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
        _REQ_MAIN: _mb_response(_MAIN_BLOCK),
        _REQ_SN:   _mb_response(_SN_RAW),
    }

    def _response(self, char_specifier: str | int, cmd: bytes) -> bytearray:
        if isinstance(char_specifier, str) and char_specifier != TX_UUID:
            return bytearray()
        return self.RESP.get(cmd, bytearray())

    async def write_gatt_char(
        self,
        char_specifier: BleakGATTCharacteristic | int | str | UUID,
        data: Buffer,
        response: bool | None = None,
    ) -> None:
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

    await bms.async_update()
    assert bms.is_connected is keep_alive_fixture
    await bms.disconnect()


async def test_device_info_sn_from_registers(patch_bleak_client) -> None:
    """Test that serial_number is read from Modbus registers and overrides BT value."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    # Modbus SN registers contain "A231015000000001" which should be present
    assert info.get("serial_number") == "A231015000000001"
    await bms.disconnect()


async def test_device_info_empty_sn_no_exception(monkeypatch, patch_bleak_client) -> None:
    """Test that all-zero SN registers do not raise an exception."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {**MockPylontechBleakClient.RESP, _REQ_SN: _mb_response([0x0000] * 8)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    # BT-level serial is still present (set by base class), no crash
    assert info is not None
    await bms.disconnect()


async def test_negative_temperature(monkeypatch, patch_bleak_client) -> None:
    """Test that negative temperatures (int16) decode correctly."""
    cold_block = list(_MAIN_BLOCK)
    cold_block[4] = 0xFFEC  # 0x101A: int16 -20 -> -2.0 °C
    cold_block[5] = 0xFFEC  # 0x101B: int16 -20 -> -2.0 °C
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: _mb_response(cold_block)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    result = await bms.async_update()
    assert result["temp_values"] == [-2.0, -2.0]
    await bms.disconnect()


async def test_design_capacity_fallback(monkeypatch, patch_bleak_client) -> None:
    """Test that design_capacity falls back to name-parsed value when 0x1022=0."""
    no_dcap_block = list(_MAIN_BLOCK)
    no_dcap_block[12] = 0x0000  # design_capacity = 0 -> falsy, skip update
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: _mb_response(no_dcap_block)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device(name="RT12200-000001"))  # 200 Ah from name
    result = await bms.async_update()
    # capacity_ah should stay at 200 (from name), cycle_charge = 200 * 91% = 182.0
    assert result["cycle_charge"] == 182.0
    await bms.disconnect()


# ---------------------------------------------------------------------------
# Invalid response tests
# ---------------------------------------------------------------------------

@pytest.fixture(
    name="wrong_response",
    params=[
        (bytearray(_mb_response(_MAIN_BLOCK)[:-2]) + bytearray([0x00, 0x00]), "wrong_CRC"),
        (_mb_exception(0x02), "modbus_exception"),
        (bytearray([0x01, 0x03, 0x02]), "too_short"),
        (bytearray([0x01, 0x04] + [0x00] * 20), "wrong_FC"),
        (bytearray(), "empty"),
    ],
    ids=lambda p: p[1],
)
def fix_wrong_response(request: pytest.FixtureRequest) -> bytearray:
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
        MockPylontechBleakClient, "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: wrong_response},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    with pytest.raises(TimeoutError):
        await bms.async_update()
    await bms.disconnect()


# ---------------------------------------------------------------------------
# _parse_regs unit tests
# ---------------------------------------------------------------------------

def test_exception_response_discarded(patch_bleak_client, monkeypatch) -> None:
    """Test that Modbus exception responses are discarded by the notification handler."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: _mb_exception(0x02)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    bms._notification_handler(None, bytearray(_mb_exception(0x02)))  # type: ignore[arg-type]
    assert not bms._msg_event.is_set()


def test_wrong_fc_discarded(patch_bleak_client) -> None:
    """Test that frames with wrong function code are discarded."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    data = bytearray([0x01, 0x04, 0x02, 0x00, 0x01])
    data.extend(crc_modbus(bytes(data)).to_bytes(2, "little"))
    bms._notification_handler(None, data)  # type: ignore[arg-type]
    assert not bms._msg_event.is_set()


async def test_notification_handler_bad_sof(patch_bleak_client) -> None:
    """Test that frames with wrong device address are discarded."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    await bms._connect()
    bms._notification_handler(None, bytearray([0x02, 0x03, 0x02, 0x00, 0x5B, 0x00, 0x00]))  # type: ignore[arg-type]
    assert len(bms._frame) == 0
    await bms.disconnect()


# ---------------------------------------------------------------------------
# Model name parsing tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("name", "expected_capacity", "expected_cells"),
    [
        ("RT12100-710003", 100, 4),
        ("RT12200-000001", 200, 4),
        ("RT24100-000001", 100, 8),
        ("RT48100-000001", 100, 16),
        ("RT36050-000001",  50, 12),
        ("GModule",        100, 4),
        ("",               100, 4),
    ],
    ids=["RT12100", "RT12200", "RT24100", "RT48100", "RT36050", "GModule", "empty"],
)
def test_parse_model(name: str, expected_capacity: int, expected_cells: int) -> None:
    capacity, cells = BMS._parse_model(name)
    assert capacity == expected_capacity
    assert cells    == expected_cells


async def test_capacity_from_device_name(patch_bleak_client) -> None:
    """Test that capacity is correctly set from the BLE device name."""
    patch_bleak_client(MockPylontechBleakClient)

    bms_100 = BMS(generate_ble_device(name="RT12100-710003"))
    assert bms_100._capacity_ah == 100
    assert bms_100._cell_count  == 4

    bms_200 = BMS(generate_ble_device(name="RT12200-000001"))
    assert bms_200._capacity_ah == 200
    assert bms_200._cell_count  == 4

    bms_gmod = BMS(generate_ble_device(name="GModule"))
    assert bms_gmod._capacity_ah == 100
    assert bms_gmod._cell_count  == 4


# ---------------------------------------------------------------------------
# Matcher pattern tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("local_name", "has_service_uuid", "should_match"),
    [
        ("RT12100-710003", True,  True),   # standard RT12100 with service UUID
        ("RT12200-000001", True,  True),   # RT12200 variant
        ("RT24100-000001", True,  True),   # 24V variant
        ("RT48100-000001", True,  True),   # 48V variant
        ("RT36050-000001", True,  True),   # 36V variant
        ("GModule",        True,  True),   # Telink default name
        ("RT12100-710003", False, False),  # correct name but missing service UUID
        ("RT-fake",        True,  False),  # RT without digits
        ("SomeOther",      True,  False),  # unrelated device
        ("",               True,  False),  # no name
    ],
    ids=["RT12100", "RT12200", "RT24100", "RT48100", "RT36050",
         "GModule", "RT12100-no-svc", "RT-no-digits", "unrelated", "empty"],
)
def test_matcher_covers_rt_variants(
    local_name: str, has_service_uuid: bool, should_match: bool
) -> None:
    # Real devices advertise both local_name and the vendor service UUID
    # RT devices advertise Battery Service (0x180F); GModule advertises vendor UUID
    adv_dict: dict = {}
    if local_name:
        adv_dict["local_name"] = local_name
    if has_service_uuid:
        svc = normalize_uuid_str("180f") if local_name and local_name.startswith("RT") else BMS.uuid_services()[0]
        adv_dict["service_uuids"] = [svc]
    adv = adv_dict_to_advdata(adv_dict)
    matched = any(_advertisement_matches(m, adv, "") for m in BMS.matcher_dict_list())
    assert matched is should_match

async def test_notification_handler_incomplete_frame(patch_bleak_client) -> None:
    """Test that _notification_handler waits for more data when frame is incomplete."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    await bms._connect()
    # Send only 2 bytes – not enough to determine expected length
    bms._notification_handler(None, bytearray([0x01, 0x03]))  # type: ignore[arg-type]
    assert len(bms._frame) == 2  # frame kept, event not set
    assert not bms._msg_event.is_set()
    await bms.disconnect()


async def test_device_info_sn_register_timeout(monkeypatch, patch_bleak_client, patch_bms_timeout) -> None:
    """Test that a timeout reading the SN register does not crash device_info."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {k: v for k, v in MockPylontechBleakClient.RESP.items() if k != _REQ_SN},
    )
    patch_bms_timeout()
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    assert info is not None  # must not raise
    await bms.disconnect()
