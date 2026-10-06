#!/usr/bin/env python3
"""Install the BarbieNode ESP32-S3 + E22 target into a MeshCore checkout."""

from pathlib import Path
from shutil import copy2
import sys


def replace_once(text: str, before: str, after: str) -> str:
    if after in text:
        return text
    if text.count(before) != 1:
        raise RuntimeError(f"Expected exactly one anchor, found {text.count(before)}: {before!r}")
    return text.replace(before, after, 1)


if len(sys.argv) != 2:
    raise SystemExit("usage: apply-meshcore.py /path/to/MeshCore")

root = Path(sys.argv[1]).resolve()
if not (root / "examples/companion_radio/MyMesh.cpp").is_file():
    raise SystemExit(f"MeshCore companion source not found under {root}")

bundle = Path(__file__).resolve().parent / "variant"
target = root / "variants/barbienode_e22_s3"
target.mkdir(parents=True, exist_ok=True)
for name in ("platformio.ini", "target.cpp", "target.h", "tripleboot_16MB.csv"):
    copy2(bundle / name, target / name)

root_platformio = root / "platformio.ini"
text = root_platformio.read_text()
include = "variants/barbienode_e22_s3/platformio.ini"
if include not in text:
    anchor = "extra_configs ="
    if anchor not in text:
        raise RuntimeError("MeshCore platformio.ini has no extra_configs anchor")
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip().startswith(anchor):
            lines.insert(index + 1, f"  {include}")
            break
    root_platformio.write_text("\n".join(lines) + "\n")

# MeshCore normally mounts the generic `spiffs` partition. BarbieNode keeps
# Meshtastic's LittleFS there, so mount the isolated data partition instead.
companion_main = root / "examples/companion_radio/main.cpp"
text = companion_main.read_text()
text = replace_once(
    text,
    "#ifdef WIFI_SSID\n  #ifndef TCP_PORT",
    "#if defined(WIFI_SSID) || defined(BARBIENODE_E22_S3)\n  #ifndef TCP_PORT",
)
text = replace_once(
    text,
    "  #endif\n#endif\n\n// include usb interface",
    "  #endif\n#endif\n\n"
    "#if defined(BARBIENODE_E22_S3)\n"
    "void startBarbieNodeWiFiCompanion() {\n"
    "  static bool started = false;\n"
    "  if (started) return;\n"
    "  // Called by target.cpp only after BLE setup has had ten seconds to\n"
    "  // finish. Starting Wi-Fi in setup() races the ESP32-S3 controller.\n"
    "  wifi_interface.begin(TCP_PORT);\n"
    "  interface_manager.addInterface(InterfaceType::WiFi, &wifi_interface);\n"
    "  wifi_interface.enable();\n"
    "  started = true;\n"
    "}\n"
    "#endif\n\n"
    "// include usb interface",
)
text = replace_once(
    text,
    "  SPIFFS.begin(true);",
    "#if defined(BARBIENODE_E22_S3)\n"
    '  SPIFFS.begin(true, "/spiffs", 10, "meshcorefs");\n'
    "#else\n"
    "  SPIFFS.begin(true);\n"
    "#endif",
)
companion_main.write_text(text)

# The MeshCore TCP companion protocol has no authentication of its own. For
# BarbieNode accept it only from the fixed Orange Pi address on the home LAN.
wifi_interface = root / "src/helpers/esp32/SerialWifiInterface.cpp"
text = wifi_interface.read_text()
text = replace_once(
    text,
    "  if (newClient) {\n\n    // disconnect existing client",
    "  if (newClient) {\n"
    "#if defined(BARBIENODE_TCP_ALLOWED_IP)\n"
    "    if (newClient.remoteIP().toString() != BARBIENODE_TCP_ALLOWED_IP) {\n"
    "      newClient.stop();\n"
    "      return 0;\n"
    "    }\n"
    "#endif\n\n"
    "    // disconnect existing client",
)
wifi_interface.write_text(text)

# The Moscow community profile currently uses two-byte path hashes. The node's
# message scope is separately set to MSK; neither value is an IATA city code.
# Keep upstream's default everywhere else.
node_prefs = root / "examples/companion_radio/NodePrefs.h"
text = node_prefs.read_text()
text = replace_once(
    text,
    "  uint8_t path_hash_mode = 0;    // which path mode to use when sending",
    "#if defined(BARBIENODE_E22_S3)\n"
    "  uint8_t path_hash_mode = 1;    // Moscow profile / MSK scope: 2-byte path hashes\n"
    "#else\n"
    "  uint8_t path_hash_mode = 0;    // which path mode to use when sending\n"
    "#endif",
)
node_prefs.write_text(text)

# This fixed installation must listen before every transmission. Upstream's
# companion example currently hard-codes CAD off and, even when enabled, will
# force a queued packet out after four seconds of continuous activity. For
# BarbieNode enable hardware CAD and retain the packet until the radio reports
# the channel free. Other targets keep the upstream behaviour.
my_mesh_h = root / "examples/companion_radio/MyMesh.h"
text = my_mesh_h.read_text()
text = replace_once(
    text,
    "  bool getCADEnabled() const override;\n"
    "  int calcRxDelay(float score, uint32_t air_time) const override;",
    "  bool getCADEnabled() const override;\n"
    "#if defined(BARBIENODE_E22_S3)\n"
    "  uint32_t getCADFailMaxDuration() const override;\n"
    "#endif\n"
    "  int calcRxDelay(float score, uint32_t air_time) const override;",
)
my_mesh_h.write_text(text)

my_mesh_cpp = root / "examples/companion_radio/MyMesh.cpp"
text = my_mesh_cpp.read_text()
text = replace_once(
    text,
    "bool MyMesh::getCADEnabled() const {\n"
    "  return false; // hardware CAD before TX (disabled by default, until configurable)\n"
    "}",
    "bool MyMesh::getCADEnabled() const {\n"
    "#if defined(BARBIENODE_E22_S3)\n"
    "  return true; // persistent listen-before-talk for this fixed installation\n"
    "#else\n"
    "  return false; // upstream companion default\n"
    "#endif\n"
    "}\n"
    "#if defined(BARBIENODE_E22_S3)\n"
    "uint32_t MyMesh::getCADFailMaxDuration() const {\n"
    "  return UINT32_MAX; // keep queued while occupied; never force TX after the upstream 4 s timeout\n"
    "}\n"
    "#endif",
)
my_mesh_cpp.write_text(text)

# A MeshCore factory reset may format its own filesystem, but it must never
# erase the shared NVS that contains Meshtastic configuration and keys.
data_store = root / "examples/companion_radio/DataStore.cpp"
text = data_store.read_text()
text = replace_once(
    text,
    "  esp_err_t nvs_err = nvs_flash_erase(); // no need to reinit, will be done by reboot",
    "#if defined(BARBIENODE_E22_S3)\n"
    "  esp_err_t nvs_err = ESP_OK; // preserve Meshtastic/RNode shared NVS\n"
    "#else\n"
    "  esp_err_t nvs_err = nvs_flash_erase(); // no need to reinit, will be done by reboot\n"
    "#endif",
)
data_store.write_text(text)

# Expose a local BLE CLI command that selects the validated Meshtastic app0.
# The response is queued before a delayed reboot, so the phone can show that
# the command was accepted. Other MeshCore targets are unchanged.
common_cli = root / "src/helpers/CommonCLI.cpp"
text = common_cli.read_text()
text = replace_once(
    text,
    "#include <RTClib.h>",
    "#include <RTClib.h>\n"
    "#if defined(BARBIENODE_E22_S3)\n"
    "#include <target.h>\n"
    "#endif",
)
text = replace_once(
    text,
    "    _prefs->tx_power_dbm = constrain(_prefs->tx_power_dbm, -9, 30);",
    "#if defined(BARBIENODE_E22_S3)\n"
    "    _prefs->tx_power_dbm = constrain(_prefs->tx_power_dbm, -9, MAX_LORA_TX_POWER);\n"
    "#else\n"
    "    _prefs->tx_power_dbm = constrain(_prefs->tx_power_dbm, -9, 30);\n"
    "#endif",
)
text = replace_once(
    text,
    '    if (memcmp(command, "poweroff", 8) == 0 || memcmp(command, "shutdown", 8) == 0) {',
    '#if defined(BARBIENODE_E22_S3)\n'
    '    if (strcmp(command, "meshtastic") == 0) {\n'
    '      if (requestMeshtasticBoot())\n'
    '        strcpy(reply, "OK - booting Meshtastic app0");\n'
    '      else\n'
    '        strcpy(reply, "ERR - validated Meshtastic app0 not found");\n'
    '    } else\n'
    '#endif\n'
    '    if (memcmp(command, "poweroff", 8) == 0 || memcmp(command, "shutdown", 8) == 0) {',
)
text = replace_once(
    text,
    "    _prefs->tx_power_dbm = atoi(&config[3]);",
    "#if defined(BARBIENODE_E22_S3)\n"
    "    _prefs->tx_power_dbm = constrain(atoi(&config[3]), -9, MAX_LORA_TX_POWER);\n"
    "#else\n"
    "    _prefs->tx_power_dbm = atoi(&config[3]);\n"
    "#endif",
)
common_cli.write_text(text)

print(f"BarbieNode MeshCore target installed in {target}")
