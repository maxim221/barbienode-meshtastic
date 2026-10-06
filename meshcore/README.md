# MeshCore companion firmware

This target adds a third, BLE companion mode for the installed ESP32-S3 N16R8
and E22-900M22S. It explicitly removes MeshCore's upstream advert-on-boot flag
and does not enable repeater, room-server, automatic beacon, or Wi-Fi OTA
behavior.

For this target hardware CAD is always enabled before transmission. If the
channel is occupied, the outbound packet remains queued and CAD is retried with
MeshCore's randomized delay; unlike the upstream companion default, the packet
is not forced onto an occupied channel after four seconds.

MeshCore mounts only the dedicated `meshcorefs` partition. Meshtastic's
LittleFS remains at its original address and is never mounted or formatted by
MeshCore. A MeshCore factory reset formats `meshcorefs` but is patched not to
erase the shared NVS containing Meshtastic configuration and channel keys.

The target uses the verified BarbieNode wiring, TCXO 1.8 V, separate RXEN/TXEN
control, and an E22-rated ceiling of 22 dBm. MeshCore's companion protocol
persists the selected TX power and exposes the range `-9..22 dBm` to the client
app. Changing the setting does not itself send a LoRa packet. Its explicit
Moscow MeshCore default is 868.731 MHz, bandwidth 62.5 kHz, SF7, CR 7, with a
2-byte path hash selected in the companion app. These values come from the
current community profile at `https://meshcoretel.ru/ru`; re-check it before a
future rebuild. This is a separate protocol profile and does not change
Meshtastic's protected 869.075 MHz `MEDIUM_FAST` profile.

Build from the reviewed upstream revision documented in `docs/firmware.md`:

```bash
python3 /path/to/barbienode-meshtastic/meshcore/apply-meshcore.py /path/to/MeshCore
pio run -d /path/to/MeshCore -e BarbieNode_E22_S3_companion_radio_ble
```

Use only the non-merged application image. Meshtastic validates its SHA-256,
ESP application header, size, and `MESHCORE-E22-TRIPLEBOOT-V1` marker before it
can be selected as `app2`.

MeshCore reconnects to the saved home Wi-Fi, serves a local control page, and
exposes the Companion protocol on TCP port `5000` only to the fixed Orange Pi
address `192.168.1.19`. Orange Pi therefore does not occupy BLE; BLE remains
available to the iPhone application and for trips. The page provides
`POST /restart` for a same-mode reset and `POST /boot/meshtastic` for a
validated return. If the home network is
unavailable for 60 seconds, it starts the protected `BarbieNode-MeshCore` AP
using a previously saved Wi-Fi/AP password. The BLE CLI command `meshtastic`
provides an independent phone-side return path. Both paths validate the
Meshtastic image marker before selecting `app0`; the physical BOOT recovery is
only a final emergency fallback. Keep the antenna attached whenever the radio
is powered.
