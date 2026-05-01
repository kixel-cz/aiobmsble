"""Test the Pylontech RT series BMS implementation."""

from collections.abc import Buffer
from typing import Final
from uuid import UUID

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.uuids import normalize_uuid_str
import pytest

from aiobmsble import BMSSample
from aiobmsble.bms.pylontech_bms import BMS
from aiobmsble.test_data import adv_dict_to_advdata
from aiobmsble.utils import _advertisement_matches
from tests.bluetooth import generate_ble_device
from tests.conftest import MockBleakClient
from tests.test_basebms import BMSBasicTests

# ---------------------------------------------------------------------------
# Reference values
#
# Captured from a Pylontech RT12100G31 during discharge at ~4.8 A:
#
#   Main block response (0x1016–0x1022, 13 registers):
#   01 03 1a                            addr, FC, byte-count
#   05 2c                               0x1016  voltage:    1324  -> 13.24 V
#   ff d0                               0x1017  current:   -48   -> -4.8 A
#   0c f0                               0x1018  cell max:  3312  ->  3.312 V
#   0c ef                               0x1019  cell min:  3311  ->  3.311 V
#   00 96                               0x101A  temp max:   150  -> 15.0 °C
#   00 96                               0x101B  temp min:   150  -> 15.0 °C
#   00 5b                               0x101C  SoC:         91  -> 91 %
#   00 63                               0x101D  SoH:         99  -> 99 %
#   00 3f                               0x101E  power:       63  -> 63 W
#   00 00                               0x101F  unknown:      0
#   0b 90                               0x1020  lifetime: 2960  -> 296.0 kWh
#   03 e8                               0x1021  allowed:  1000  -> 100.0 A
#   03 e8                               0x1022  dcap:     1000  -> 100.0 Ah
#   45 d8                               CRC
#
#   SN block response (0x2000–0x2007, 8 registers = "A231015000000001"):
#   01 03 10 41 32 33 31 30 31 35 30 30 30 30 30 30 30 31 85 04
# ---------------------------------------------------------------------------

# Modbus request bytes (used as keys in mock response dict)
_REQ_MAIN: Final[bytes] = b"\x01\x03\x10\x16\x00\x0d\x61\x0b"
_REQ_SN: Final[bytes] = b"\x01\x03\x20\x00\x00\x08\x4f\xcc"

# Recorded Modbus responses
_RESP_MAIN: Final[bytearray] = bytearray(
    b"\x01\x03\x1a\x05\x2c\xff\xd0\x0c\xf0\x0c\xef\x00\x96\x00\x96"
    b"\x00\x5b\x00\x63\x00\x3f\x00\x00\x0b\x90\x03\xe8\x03\xe8\x45\xd8"
)
_RESP_SN: Final[bytearray] = bytearray(
    b"\x01\x03\x10\x41\x32\x33\x31\x30\x31\x35\x30\x30\x30\x30\x30\x30\x30\x30\x31\x85\x04"
)

# Pre-computed modified responses for specific test cases
_RESP_MAIN_COLD: Final[bytearray] = bytearray(
    # temp max/min replaced with 0xFFEC (int16 -20 -> -2.0 °C), CRC updated
    b"\x01\x03\x1a\x05\x2c\xff\xd0\x0c\xf0\x0c\xef\xff\xec\xff\xec"
    b"\x00\x5b\x00\x63\x00\x3f\x00\x00\x0b\x90\x03\xe8\x03\xe8\xbd\x16"
)
_RESP_MAIN_NO_DCAP: Final[bytearray] = bytearray(
    # design_capacity register (0x1022) set to 0x0000, CRC updated
    b"\x01\x03\x1a\x05\x2c\xff\xd0\x0c\xf0\x0c\xef\x00\x96\x00\x96"
    b"\x00\x5b\x00\x63\x00\x3f\x00\x00\x0b\x90\x03\xe8\x00\x00\x45\x66"
)
_RESP_SN_ZERO: Final[bytearray] = bytearray(
    b"\x01\x03\x10" + b"\x00" * 16 + b"\xe4\x59"
)

_RESP_MAIN_24V: Final[bytearray] = bytearray(
    b"\x01\x03\x1a\x0a\x58\xff\xd0\x0c\xf0\x0c\xef\x00\x96\x00\x96"
    b"\x00\x5b\x00\x63\x00\x3f\x00\x00\x0b\x90\x03\xe8\x03\xe8\x9d\xd7"
)

TX_UUID: Final[str] = BMS.uuid_tx()


def ref_value() -> BMSSample:
    """Return the expected BMSSample for the reference recording above."""
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
        "total_charge":     23125,   # int(296.0 kWh * 1000 / 12.8 V)
        "cycles":           231,     # 23125 Ah // 100 Ah
    }


class TestBasicBMS(BMSBasicTests):
    """Run the standard BMS interface tests for Pylontech RT series."""

    bms_class = BMS


class MockPylontechBleakClient(MockBleakClient):
    """Emulate a Pylontech RT series BleakClient."""

    RESP: dict[bytes, bytearray] = {
        _REQ_MAIN: bytearray(_RESP_MAIN),
        _REQ_SN:   bytearray(_RESP_SN),
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
        """Issue write command to GATT and trigger notification with response."""
        await super().write_gatt_char(char_specifier, data, response)
        assert self._notify_callback is not None
        resp = self._response(
            char_specifier if isinstance(char_specifier, str) else str(char_specifier),
            bytes(data),
        )
        if resp:
            self._notify_callback("MockPylontechBleakClient", resp)


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
    """Test that serial number is read from Modbus registers."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    assert info.get("serial_number") == "A231015000000001"
    await bms.disconnect()


async def test_device_info_empty_sn_no_exception(monkeypatch, patch_bleak_client) -> None:
    """Test that all-zero SN registers do not raise an exception."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {**MockPylontechBleakClient.RESP, _REQ_SN: bytearray(_RESP_SN_ZERO)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    assert info is not None
    await bms.disconnect()


async def test_negative_temperature(monkeypatch, patch_bleak_client) -> None:
    """Test that negative temperatures (int16) decode correctly."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {**MockPylontechBleakClient.RESP, _REQ_MAIN: bytearray(_RESP_MAIN_COLD)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    result = await bms.async_update()
    assert result["temp_values"] == [-2.0, -2.0]
    await bms.disconnect()


async def test_design_capacity_fallback(monkeypatch, patch_bleak_client) -> None:
    """Test that design_capacity falls back to name-parsed value when register is 0."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {**MockPylontechBleakClient.RESP, _REQ_MAIN: bytearray(_RESP_MAIN_NO_DCAP)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device(name="RT12200-000001"))  # 200 Ah from name
    result = await bms.async_update()
    assert result["design_capacity"] == 200
    assert result["cycle_charge"] == 182.0  # 200 * 91%
    await bms.disconnect()


async def test_gmod_calibration_from_voltage(patch_bleak_client) -> None:
    """Test that cell_count and capacity are calibrated from measured data for generic-name devices.

    When the BLE advertisement uses the Telink default name ("GMod" / "GModule"),
    _parse_model() falls back to (100 Ah, 4 cells). After the first successful
    update the values are corrected from the measured voltage and design_capacity
    register so that 24 V / 48 V packs are handled correctly.
    """
    patch_bleak_client(MockPylontechBleakClient)

    for generic_name in ("GMod", "GModule"):
        bms = BMS(generate_ble_device(name=generic_name))
        # Before first update: fallback values
        assert bms._cell_count == 4
        assert bms._capacity_ah == 100

        result = await bms.async_update()
        # voltage=13.24 V → round(13.24 / 3.2) = round(4.14) = 4 cells
        assert bms._cell_count == 4
        assert bms._capacity_ah == 100  # design_capacity from register = 100 Ah
        assert result["design_capacity"] == 100
        await bms.disconnect()


async def test_gmod_calibration_24v(monkeypatch, patch_bleak_client) -> None:
    """Test that cell_count is calibrated correctly for a 24V pack with generic name."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {**MockPylontechBleakClient.RESP, _REQ_MAIN: bytearray(_RESP_MAIN_24V)},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device(name="GMod"))
    assert bms._cell_count == 4        # fallback před update
    assert bms._capacity_ah == 100

    result = await bms.async_update()
    # voltage=26.48V → round(26.48 / 3.2) = round(8.275) = 8 cells
    assert bms._cell_count == 8
    assert bms._nominal_voltage == pytest.approx(25.6)
    assert bms._capacity_ah == 100
    # total_charge přepočítán s nominal=25.6V místo 12.8V
    assert result["total_charge"] == int(296.0 * 1000 / 25.6)
    await bms.disconnect()


@pytest.fixture(
    name="wrong_response",
    params=[
        (bytearray(_RESP_MAIN[:-2]) + bytearray([0x00, 0x00]), "wrong_CRC"),
        (bytearray(b"\x01\x83\x02\xc0\xf1"),                  "modbus_exception"),
        (bytearray(b"\x01\x03\x02"),                           "too_short"),
        (bytearray(b"\x01\x04\x1a") + bytearray(26) + bytearray(b"\x00\x00"), "wrong_FC"),
        (bytearray(),                                           "empty"),
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
        MockPylontechBleakClient, "RESP",
        MockPylontechBleakClient.RESP | {_REQ_MAIN: wrong_response},
    )
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    with pytest.raises(TimeoutError):
        await bms.async_update()
    await bms.disconnect()


async def test_device_info_sn_register_timeout(
    monkeypatch: pytest.MonkeyPatch,
    patch_bleak_client,
    patch_bms_timeout,
) -> None:
    """Test that a timeout reading the SN register does not crash device_info."""
    monkeypatch.setattr(
        MockPylontechBleakClient, "RESP",
        {k: v for k, v in MockPylontechBleakClient.RESP.items() if k != _REQ_SN},
    )
    patch_bms_timeout()
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    info = await bms.device_info()
    assert info is not None
    await bms.disconnect()


async def test_notification_handler_incomplete_frame(patch_bleak_client) -> None:
    """Test that _notification_handler waits for more data when frame is incomplete."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    await bms._connect()
    bms._notification_handler(None, bytearray([0x01, 0x03, 0x1a, 0x05, 0x2c]))  # type: ignore[arg-type]
    assert len(bms._frame) == 5
    assert not bms._msg_event.is_set()
    await bms.disconnect()


async def test_exception_response_discarded(patch_bleak_client) -> None:
    """Test that Modbus exception responses are discarded by the notification handler."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    bms._notification_handler(None, bytearray(b"\x01\x83\x02\xf1\x31"))  # type: ignore[arg-type]
    assert not bms._msg_event.is_set()


async def test_wrong_fc_discarded(patch_bleak_client) -> None:
    """Test that frames with wrong function code are discarded."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    bms._notification_handler(None, bytearray(b"\x01\x04\x02\x00\x01\x78\xf0"))  # type: ignore[arg-type]
    assert not bms._msg_event.is_set()


async def test_notification_handler_bad_sof(patch_bleak_client) -> None:
    """Test that frames with wrong device address are discarded."""
    patch_bleak_client(MockPylontechBleakClient)
    bms = BMS(generate_ble_device())
    await bms._connect()
    bms._notification_handler(None, bytearray([0x02, 0x03, 0x02, 0x00, 0x5B, 0x00, 0x00]))  # type: ignore[arg-type]
    assert len(bms._frame) == 0
    await bms.disconnect()


@pytest.mark.parametrize(
    ("name", "expected_capacity", "expected_cells"),
    [
        ("RT12100-710003", 100, 4),
        ("RT12200-000001", 200, 4),
        ("RT24100-000001", 100, 8),
        ("RT48100-000001", 100, 16),
        ("RT36050-000001",  50, 12),
        ("GModule",        100, 4),
        ("GMod",           100, 4),
        ("",               100, 4),
    ],
    ids=["RT12100", "RT12200", "RT24100", "RT48100", "RT36050", "GModule", "GMod", "empty"],
)
def test_parse_model(name: str, expected_capacity: int, expected_cells: int) -> None:
    """Test that model name is correctly parsed to capacity and cell count."""
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

    bms_gmod = BMS(generate_ble_device(name="GMod"))
    assert bms_gmod._capacity_ah == 100
    assert bms_gmod._cell_count  == 4


@pytest.mark.parametrize(
    ("local_name", "has_service_uuid", "should_match"),
    [
        ("RT12100-710003", True,  True),   # standard RT12100 with Battery Service UUID
        ("RT12200-000001", True,  True),   # RT12200 variant
        ("RT24100-000001", True,  True),   # 24V variant
        ("RT48100-000001", True,  True),   # 48V variant
        ("RT36050-000001", True,  True),   # 36V variant
        ("GModule",        True,  True),   # Telink default full name
        ("GMod",           True,  True),   # Telink name truncated in BLE advertisement
        ("RT12100-710003", False, False),  # correct name but missing service UUID
        ("RT-fake",        True,  False),  # RT without digits
        ("SomeOther",      True,  False),  # unrelated device
        ("",               True,  False),  # no name
    ],
    ids=["RT12100", "RT12200", "RT24100", "RT48100", "RT36050",
         "GModule", "GMod", "RT12100-no-svc", "RT-no-digits", "unrelated", "empty"],
)
def test_matcher_covers_rt_variants(
    local_name: str, has_service_uuid: bool, should_match: bool
) -> None:
    """Test that matcher_dict_list covers all RT voltage/capacity variants."""
    # All variants (RT*, GModule, GMod) advertise 0x180F in BLE advertisement packets.
    adv_dict: dict = {}
    if local_name:
        adv_dict["local_name"] = local_name
    if has_service_uuid:
        adv_dict["service_uuids"] = [normalize_uuid_str("180f")]
    adv = adv_dict_to_advdata(adv_dict)
    matched = any(_advertisement_matches(m, adv, "") for m in BMS.matcher_dict_list())
    assert matched is should_match
