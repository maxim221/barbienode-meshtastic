#!/usr/bin/env python3
"""Record passive BarbieNode availability and power-related health metrics."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


REPORT_URL = os.environ.get("NODE_REPORT_URL", "http://192.168.1.31/json/report")
MODE_STATUS_URL = os.environ.get("NODE_MODE_STATUS_URL", "http://192.168.1.31/status")
STATE_PATH = Path(os.environ.get("NODE_HEALTH_PATH", "/var/lib/barbienode-node-health/health.jsonl"))
POLL_SECONDS = max(15, int(os.environ.get("NODE_HEALTH_POLL_SECONDS", "60")))
SAMPLE_SECONDS = max(POLL_SECONDS, int(os.environ.get("NODE_HEALTH_SAMPLE_SECONDS", "300")))
RETENTION_SECONDS = max(86400, int(os.environ.get("NODE_HEALTH_RETENTION_SECONDS", str(30 * 86400))))
HTTP_TIMEOUT_SECONDS = max(1, int(os.environ.get("NODE_HEALTH_HTTP_TIMEOUT_SECONDS", "5")))


def number(value: object) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def fetch_report() -> dict[str, object]:
    with urlopen(REPORT_URL, timeout=HTTP_TIMEOUT_SECONDS) as response:
        value = json.load(response)
    if not isinstance(value, dict) or value.get("status") != "ok":
        raise ValueError("invalid node report")
    data = value.get("data", value)
    if not isinstance(data, dict):
        raise ValueError("invalid node report data")
    return data


def health_row(data: dict[str, object]) -> dict[str, object]:
    airtime = data.get("airtime", {})
    device = data.get("device", {})
    power = data.get("power", {})
    wifi = data.get("wifi", {})
    if not isinstance(airtime, dict):
        airtime = {}
    if not isinstance(device, dict):
        device = {}
    if not isinstance(power, dict):
        power = {}
    if not isinstance(wifi, dict):
        wifi = {}
    return {
        "ts": int(time.time()),
        "state": "up",
        "mode": "meshtastic",
        "boot_seconds": number(airtime.get("seconds_since_boot")),
        "reboot_counter": number(device.get("reboot_counter")),
        "wifi_rssi": number(wifi.get("rssi")),
        "channel_utilization": number(airtime.get("channel_utilization")),
        "tx_utilization": number(airtime.get("utilization_tx")),
        "battery_percent": number(power.get("battery_percent")),
        "battery_voltage_mv": number(power.get("battery_voltage_mv")),
        "has_battery": power.get("has_battery"),
        "has_usb": power.get("has_usb"),
        "is_charging": power.get("is_charging"),
    }


def fetch_health() -> dict[str, object]:
    try:
        return health_row(fetch_report())
    except HTTPError as error:
        if error.code != 404:
            raise
    with urlopen(MODE_STATUS_URL, timeout=HTTP_TIMEOUT_SECONDS) as response:
        value = json.load(response)
    if not isinstance(value, dict) or value.get("mode") not in {"meshcore", "rnode"}:
        raise ValueError("unknown node mode status")
    return {
        "ts": int(time.time()),
        "state": "up",
        "mode": value["mode"],
    }


def append_row(row: dict[str, object]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def compact() -> None:
    cutoff = int(time.time()) - RETENTION_SECONDS
    try:
        rows = []
        for line in STATE_PATH.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
                if isinstance(row, dict) and int(row.get("ts", 0)) >= cutoff:
                    rows.append(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        temporary = STATE_PATH.with_suffix(".tmp")
        temporary.write_text("\n".join(rows) + ("\n" if rows else ""), encoding="utf-8")
        os.replace(temporary, STATE_PATH)
    except FileNotFoundError:
        return


def main() -> int:
    os.umask(0o077)
    previous_state = ""
    previous_boot: float | int | None = None
    previous_reboots: float | int | None = None
    next_sample = 0.0
    next_compaction = time.monotonic() + 86400
    while True:
        now = time.monotonic()
        try:
            row = fetch_health()
            boot = number(row.get("boot_seconds"))
            reboots = number(row.get("reboot_counter"))
            reboot_detected = (
                (boot is not None and previous_boot is not None and boot < previous_boot)
                or (reboots is not None and previous_reboots is not None and reboots > previous_reboots)
            )
            if previous_state != "up" or reboot_detected or now >= next_sample:
                if reboot_detected:
                    row["event"] = "reboot_detected"
                append_row(row)
                next_sample = now + SAMPLE_SECONDS
            previous_state = "up"
            if boot is not None:
                previous_boot = boot
            if reboots is not None:
                previous_reboots = reboots
        except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as error:
            if previous_state != "down":
                append_row({
                    "ts": int(time.time()),
                    "state": "down",
                    "error": f"{type(error).__name__}: {error}",
                })
            previous_state = "down"
        if now >= next_compaction:
            compact()
            next_compaction = now + 86400
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
