#!/usr/bin/env python3
"""Create a BlueZ bond with one explicitly selected MeshCore companion."""

import argparse
import asyncio
import os
import re

from dbus_fast import BusType, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.service import ServiceInterface, method


ADDRESS_PATTERN = re.compile(r"^(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
AGENT_PATH = "/barbienode/meshcore_pairing_agent"


class PinAgent(ServiceInterface):
    def __init__(self, pin: str) -> None:
        super().__init__("org.bluez.Agent1")
        self.pin = pin

    @method()
    def Release(self) -> None:
        return None

    @method()
    def RequestPinCode(self, _device: "o") -> "s":
        print("BlueZ requested the MeshCore PIN", flush=True)
        return self.pin

    @method()
    def RequestPasskey(self, _device: "o") -> "u":
        print("BlueZ requested the MeshCore passkey", flush=True)
        return int(self.pin)

    @method()
    def DisplayPasskey(self, _device: "o", _passkey: "u", _entered: "q") -> None:
        return None

    @method()
    def DisplayPinCode(self, _device: "o", _pincode: "s") -> None:
        return None

    @method()
    def RequestConfirmation(self, _device: "o", _passkey: "u") -> None:
        print("BlueZ requested passkey confirmation", flush=True)
        return None

    @method()
    def RequestAuthorization(self, _device: "o") -> None:
        return None

    @method()
    def AuthorizeService(self, _device: "o", _uuid: "s") -> None:
        return None

    @method()
    def Cancel(self) -> None:
        return None


async def pair(address: str, pin: str, adapter: str) -> None:
    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    agent = PinAgent(pin)
    bus.export(AGENT_PATH, agent)
    manager_intro = await bus.introspect("org.bluez", "/org/bluez")
    manager_object = bus.get_proxy_object("org.bluez", "/org/bluez", manager_intro)
    manager = manager_object.get_interface("org.bluez.AgentManager1")
    await manager.call_register_agent(AGENT_PATH, "KeyboardOnly")
    await manager.call_request_default_agent(AGENT_PATH)
    try:
        device_path = f"/org/bluez/{adapter}/dev_{address.upper().replace(':', '_')}"
        device_intro = await bus.introspect("org.bluez", device_path)
        device_object = bus.get_proxy_object("org.bluez", device_path, device_intro)
        device = device_object.get_interface("org.bluez.Device1")
        properties = device_object.get_interface("org.freedesktop.DBus.Properties")
        state = await properties.call_get_all("org.bluez.Device1")
        if not bool(state["Paired"].value):
            try:
                await device.call_cancel_pairing()
            except Exception:
                pass
            await asyncio.sleep(1)
            await asyncio.wait_for(device.call_pair(), timeout=45)
        await properties.call_set("org.bluez.Device1", "Trusted", Variant("b", True))
        state = await properties.call_get_all("org.bluez.Device1")
        if not bool(state["Paired"].value) or not bool(state["Bonded"].value):
            raise RuntimeError("BlueZ did not retain the MeshCore bond")
        print(f"MeshCore BLE bond ready for {address.upper()}")
    finally:
        try:
            await manager.call_unregister_agent(AGENT_PATH)
        finally:
            bus.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--adapter", default="hci0")
    parser.add_argument("--pin", default=os.environ.get("MESHCORE_BLE_PIN", "123456"))
    args = parser.parse_args()
    if not ADDRESS_PATTERN.fullmatch(args.address):
        raise SystemExit("invalid BLE address")
    if not re.fullmatch(r"\d{6}", args.pin):
        raise SystemExit("BLE PIN must contain exactly six digits")
    asyncio.run(pair(args.address, args.pin, args.adapter))


if __name__ == "__main__":
    main()
