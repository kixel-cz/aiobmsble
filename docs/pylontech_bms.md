# Pylontech RT series BMS

## Supported devices

Pylontech RT series batteries with built-in BLE module (Telink TLSR8266 "GModule"):
RT12100, RT12200, RT24100, RT24200, and likely other RT variants.

## BLE interface

| | UUID |
|---|---|
| Service | `00010203-0405-0607-0809-0a0b0c0d1910` |
| RX (notify) | `00010203-0405-0607-0809-0a0b0c0d2b10` |
| TX (write) | `00010203-0405-0607-0809-0a0b0c0d2b11` |

The Telink module advertises under two possible local names:
- `RT12100-XXXXXX` (or RT24100 etc.) – set by Pylontech firmware
- `GModule` – Telink default fallback, appears after factory reset or unpairing

The vendor-specific service UUID is included in advertisement packets and is
combined with the local name for more reliable discovery — in particular when
the host Bluetooth stack has cached a stale device name.

## Protocol

Modbus RTU over BLE SPP. Device address `0x01`, function code `0x03`
(read holding registers).

## Register map

> **Note:** This mapping was determined by empirical measurement against a
> single unit (RT12100G31). There is no publicly available Modbus register
> documentation for the RT BLE series. Core registers (voltage, current,
> temperatures, SoC, SoH) are confirmed by cross-validation. Others
> (power, design_capacity) are high-confidence but not verified against an
> official specification.

### Main block (0x1016–0x1022, 13 registers, read in a single request)

| Register | Type | Scale | Unit | Description |
|----------|------|-------|------|-------------|
| 0x1016 | uint16 | ×0.01 | V | Pack voltage |
| 0x1017 | int16 | ×0.1 | A | Current (negative = discharging) |
| 0x1018 | uint16 | ×0.001 | V | Max cell voltage |
| 0x1019 | uint16 | ×0.001 | V | Min cell voltage |
| 0x101A | int16 | ×0.1 | °C | Max temperature |
| 0x101B | int16 | ×0.1 | °C | Min temperature |
| 0x101C | uint16 | ×1 | % | State of Charge |
| 0x101D | uint16 | ×1 | % | State of Health |
| 0x101E | uint16 | ×1 | W | Instantaneous power (absolute, no sign) |
| 0x101F | uint16 | – | – | Unknown – observed always 0, not used |
| 0x1020 | uint16 | ×0.1 | kWh | Lifetime accumulated energy |
| 0x1021 | uint16 | ×0.1 | A | BMS dynamic allowed current (varies with SoC/temperature) |
| 0x1022 | uint16 | ×0.1 | Ah | Design capacity (e.g. 1000 = 100.0 Ah) |

### Additional registers (read separately)

| Register | Type | Scale | Unit | Description |
|----------|------|-------|------|-------------|
| 0x1023 | uint16 | ×0.01 | V | Charge voltage limit (e.g. 1410 = 14.10 V) |
| 0x1024 | uint16 | ×0.01 | V | Discharge cutoff voltage (e.g. 1080 = 10.80 V) |
| 0x2000–0x2007 | ASCII | – | – | Serial number (16 chars across 8 registers) |
| 0x2008–0x200D | – | – | – | Telink GModule internal config, not BMS data |

### Notes on cell_voltages and temp_values

The RT series Modbus interface exposes only min/max aggregates for cell
voltages (0x1018, 0x1019) and temperatures (0x101A, 0x101B) — individual
cell or sensor values are not available. Both `cell_voltages` and
`temp_values` are therefore two-element lists `[max, min]`.
