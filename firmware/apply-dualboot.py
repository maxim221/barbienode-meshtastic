#!/usr/bin/env python3
"""Install the E22 triple-boot and autonomous bot additions into Meshtastic."""

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
    raise SystemExit("usage: apply-dualboot.py /path/to/meshtastic-firmware")

root = Path(sys.argv[1]).resolve()
source = root / "src"
handler = source / "mesh/http/ContentHandler.cpp"
wifi_client = source / "mesh/wifi/WiFiAPClient.cpp"
radio_interface = source / "mesh/RadioLibInterface.cpp"
radio_config = source / "mesh/RadioInterface.cpp"
mqtt = source / "mqtt/MQTT.cpp"
if not handler.is_file():
    raise SystemExit(f"Meshtastic ContentHandler.cpp not found under {root}")
if not wifi_client.is_file():
    raise SystemExit(f"Meshtastic WiFiAPClient.cpp not found under {root}")
if not radio_interface.is_file():
    raise SystemExit(f"Meshtastic RadioLibInterface.cpp not found under {root}")
if not radio_config.is_file():
    raise SystemExit(f"Meshtastic RadioInterface.cpp not found under {root}")
if not mqtt.is_file():
    raise SystemExit(f"Meshtastic MQTT.cpp not found under {root}")

bundle = Path(__file__).resolve().parent
overlay = bundle / "overlay"
variant = root / "variants/esp32s3/diy/e22-s3-n16r8"
variant.mkdir(parents=True, exist_ok=True)
for variant_file in ("platformio.ini", "variant.h", "pins_arduino.h", "tripleboot_16MB.csv"):
    copy2(bundle / variant_file, variant / variant_file)
copy2(overlay / "DualBootHandler.h", source / "DualBootHandler.h")
copy2(overlay / "DualBootHandler.cpp", source / "DualBootHandler.cpp")
copy2(overlay / "ReplyBotModule.h", source / "modules/ReplyBotModule.h")
copy2(overlay / "ReplyBotModule.cpp", source / "modules/ReplyBotModule.cpp")

text = handler.read_text()
# The RGB-heartbeat revision already registered the notification endpoints.
# Normalize that older block before adding the firmware updater, otherwise the
# notification declarations are duplicated during an in-place upgrade.
if 'nodeDualBootUpdateRNode' not in text:
    text = text.replace(
        '    ResourceNode *nodeDualBootHome = new ResourceNode("/dualboot/home", "POST", &handleDualBootHomeWiFi);\n'
        '    ResourceNode *nodeNotificationStatus = new ResourceNode("/notifications/status", "GET", &handleNotificationStatus);\n'
        '    ResourceNode *nodeNotificationRead = new ResourceNode("/notifications/read", "POST", &handleNotificationRead);',
        '    ResourceNode *nodeDualBootHome = new ResourceNode("/dualboot/home", "POST", &handleDualBootHomeWiFi);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeDualBootHome);\n'
            f'    {server}->registerNode(nodeNotificationStatus);\n'
            f'    {server}->registerNode(nodeNotificationRead);',
            f'    {server}->registerNode(nodeDualBootHome);',
        )
# Add Ping-bot routes to a checkout that already has the previous updater
# routes. Doing this before the fresh-install anchors keeps reruns idempotent.
if 'nodeDualBootUpdateRNode' in text and 'nodePingBotStatus' not in text:
    text = text.replace(
        '    ResourceNode *nodeNotificationRead = new ResourceNode("/notifications/read", "POST", &handleNotificationRead);\n'
        '    ResourceNode *nodeDualBootUpdateRNode = new ResourceNode("/dualboot/update/rnode", "POST", &handleDualBootUpdateRNode);',
        '    ResourceNode *nodeNotificationRead = new ResourceNode("/notifications/read", "POST", &handleNotificationRead);\n'
        '    ResourceNode *nodePingBotStatus = new ResourceNode("/pingbot/status", "GET", &handlePingBotStatus);\n'
        '    ResourceNode *nodePingBotToggle = new ResourceNode("/pingbot/toggle", "POST", &handlePingBotToggle);\n'
        '    ResourceNode *nodeDualBootUpdateRNode = new ResourceNode("/dualboot/update/rnode", "POST", &handleDualBootUpdateRNode);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeNotificationRead);\n'
            f'    {server}->registerNode(nodeDualBootUpdateRNode);',
            f'    {server}->registerNode(nodeNotificationRead);\n'
            f'    {server}->registerNode(nodePingBotStatus);\n'
            f'    {server}->registerNode(nodePingBotToggle);\n'
            f'    {server}->registerNode(nodeDualBootUpdateRNode);',
        )
# Add local clock synchronization routes. They do not transmit over LoRa.
if 'nodeClockSync' not in text:
    text = text.replace(
        '    ResourceNode *nodePingBotToggle = new ResourceNode("/pingbot/toggle", "POST", &handlePingBotToggle);',
        '    ResourceNode *nodePingBotToggle = new ResourceNode("/pingbot/toggle", "POST", &handlePingBotToggle);\n'
        '    ResourceNode *nodeClockStatus = new ResourceNode("/clock/status", "GET", &handleClockStatus);\n'
        '    ResourceNode *nodeClockSync = new ResourceNode("/clock/sync", "POST", &handleClockSync);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodePingBotToggle);',
            f'    {server}->registerNode(nodePingBotToggle);\n'
            f'    {server}->registerNode(nodeClockStatus);\n'
            f'    {server}->registerNode(nodeClockSync);',
        )
# Add a board-local TX-power route. It changes and persists only tx_power and
# does not send a LoRa packet by itself.
if 'nodeTxPowerSet' not in text:
    text = text.replace(
        '    ResourceNode *nodeClockSync = new ResourceNode("/clock/sync", "POST", &handleClockSync);',
        '    ResourceNode *nodeClockSync = new ResourceNode("/clock/sync", "POST", &handleClockSync);\n'
        '    ResourceNode *nodeTxPowerStatus = new ResourceNode("/radio/tx-power", "GET", &handleTxPowerStatus);\n'
        '    ResourceNode *nodeTxPowerSet = new ResourceNode("/radio/tx-power", "POST", &handleTxPowerSet);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeClockSync);',
            f'    {server}->registerNode(nodeClockSync);\n'
            f'    {server}->registerNode(nodeTxPowerStatus);\n'
            f'    {server}->registerNode(nodeTxPowerSet);',
        )
# Add the persistent LBT/CAD route to an already-patched checkout. Fresh
# installs receive the same nodes from the complete blocks below.
if 'nodeDualBootUpdateRNode' in text and 'nodeListenBeforeTalkSet' not in text:
    text = text.replace(
        '    ResourceNode *nodeTxPowerSet = new ResourceNode("/radio/tx-power", "POST", &handleTxPowerSet);',
        '    ResourceNode *nodeTxPowerSet = new ResourceNode("/radio/tx-power", "POST", &handleTxPowerSet);\n'
        '    ResourceNode *nodeListenBeforeTalkStatus = new ResourceNode("/radio/lbt", "GET", &handleListenBeforeTalkStatus);\n'
        '    ResourceNode *nodeListenBeforeTalkSet = new ResourceNode("/radio/lbt", "POST", &handleListenBeforeTalkSet);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeTxPowerSet);',
            f'    {server}->registerNode(nodeTxPowerSet);\n'
            f'    {server}->registerNode(nodeListenBeforeTalkStatus);\n'
            f'    {server}->registerNode(nodeListenBeforeTalkSet);',
        )
# Add MeshCore app2 routes to a checkout carrying the earlier two-application
# overlay. The complete fresh-install blocks below already contain them.
if 'nodeDualBootUpdateRNode' in text and 'nodeDualBootMeshCore' not in text:
    text = text.replace(
        '    ResourceNode *nodeDualBootRNode = new ResourceNode("/dualboot/rnode", "POST", &handleDualBootRNode);',
        '    ResourceNode *nodeDualBootRNode = new ResourceNode("/dualboot/rnode", "POST", &handleDualBootRNode);\n'
        '    ResourceNode *nodeDualBootMeshCore = new ResourceNode("/dualboot/meshcore", "POST", &handleDualBootMeshCore);',
    )
    text = text.replace(
        '    ResourceNode *nodeDualBootUpdateRNode = new ResourceNode("/dualboot/update/rnode", "POST", &handleDualBootUpdateRNode);',
        '    ResourceNode *nodeDualBootUpdateRNode = new ResourceNode("/dualboot/update/rnode", "POST", &handleDualBootUpdateRNode);\n'
        '    ResourceNode *nodeDualBootUpdateMeshCore = new ResourceNode("/dualboot/update/meshcore", "POST", &handleDualBootUpdateMeshCore);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeDualBootRNode);',
            f'    {server}->registerNode(nodeDualBootRNode);\n'
            f'    {server}->registerNode(nodeDualBootMeshCore);',
        )
        text = text.replace(
            f'    {server}->registerNode(nodeDualBootUpdateRNode);',
            f'    {server}->registerNode(nodeDualBootUpdateRNode);\n'
            f'    {server}->registerNode(nodeDualBootUpdateMeshCore);',
        )
# Add the one-time, exact-hash Wi-Fi partition migration and a read-only
# partition-table backup route. These are deliberately separate from generic
# OTA and accept only the reviewed BarbieNode dual-to-triple layout.
if 'nodeDualBootUpdateMeshCore' in text and 'nodeTripleBootMigration' not in text:
    text = text.replace(
        '    ResourceNode *nodeDualBootUpdateMeshCore = new ResourceNode("/dualboot/update/meshcore", "POST", &handleDualBootUpdateMeshCore);',
        '    ResourceNode *nodeDualBootUpdateMeshCore = new ResourceNode("/dualboot/update/meshcore", "POST", &handleDualBootUpdateMeshCore);\n'
        '    ResourceNode *nodePartitionTableBackup = new ResourceNode("/dualboot/partition-table", "GET", &handlePartitionTableBackup);\n'
        '    ResourceNode *nodeTripleBootMigration = new ResourceNode("/dualboot/migrate/tripleboot", "POST", &handleTripleBootMigration);',
    )
    for server in ('secureServer', 'insecureServer'):
        text = text.replace(
            f'    {server}->registerNode(nodeDualBootUpdateMeshCore);',
            f'    {server}->registerNode(nodeDualBootUpdateMeshCore);\n'
            f'    {server}->registerNode(nodePartitionTableBackup);\n'
            f'    {server}->registerNode(nodeTripleBootMigration);',
        )
# Upgrade a checkout that already has the earlier two-mode overlay without
# duplicating its nodes. Fresh and already-upgraded checkouts skip this block.
if 'ResourceNode *nodeDualBootHome' not in text:
    text = text.replace(
        '    ResourceNode *nodeRestart = new ResourceNode("/restart", "POST", &handleRestart);\n'
        '    ResourceNode *nodeDualBootStatus = new ResourceNode("/dualboot/status", "GET", &handleDualBootStatus);\n'
        '    ResourceNode *nodeDualBootRNode = new ResourceNode("/dualboot/rnode", "POST", &handleDualBootRNode);',
        '    ResourceNode *nodeRestart = new ResourceNode("/restart", "POST", &handleRestart);',
    )
    text = text.replace(
        '    secureServer->registerNode(nodeRestart);\n'
        '    secureServer->registerNode(nodeDualBootStatus);\n'
        '    secureServer->registerNode(nodeDualBootRNode);',
        '    secureServer->registerNode(nodeRestart);',
    )
    text = text.replace(
        '    insecureServer->registerNode(nodeRestart);\n'
        '    insecureServer->registerNode(nodeDualBootStatus);\n'
        '    insecureServer->registerNode(nodeDualBootRNode);',
        '    insecureServer->registerNode(nodeRestart);',
    )
text = replace_once(
    text,
    '#include "mesh/http/WebServer.h"',
    '#include "mesh/http/WebServer.h"\n#include "DualBootHandler.h"',
)
text = replace_once(
    text,
    '    ResourceNode *nodeRestart = new ResourceNode("/restart", "POST", &handleRestart);',
    '    ResourceNode *nodeRestart = new ResourceNode("/restart", "POST", &handleRestart);\n'
    '    ResourceNode *nodeDualBootStatus = new ResourceNode("/dualboot/status", "GET", &handleDualBootStatus);\n'
    '    ResourceNode *nodeDualBootRNode = new ResourceNode("/dualboot/rnode", "POST", &handleDualBootRNode);\n'
    '    ResourceNode *nodeDualBootMeshCore = new ResourceNode("/dualboot/meshcore", "POST", &handleDualBootMeshCore);\n'
    '    ResourceNode *nodeDualBootAP = new ResourceNode("/dualboot/ap", "POST", &handleDualBootPortableAP);\n'
    '    ResourceNode *nodeDualBootHome = new ResourceNode("/dualboot/home", "POST", &handleDualBootHomeWiFi);',
)
text = replace_once(
    text,
    '    ResourceNode *nodeDualBootHome = new ResourceNode("/dualboot/home", "POST", &handleDualBootHomeWiFi);',
    '    ResourceNode *nodeDualBootHome = new ResourceNode("/dualboot/home", "POST", &handleDualBootHomeWiFi);\n'
    '    ResourceNode *nodeNotificationStatus = new ResourceNode("/notifications/status", "GET", &handleNotificationStatus);\n'
    '    ResourceNode *nodeNotificationRead = new ResourceNode("/notifications/read", "POST", &handleNotificationRead);\n'
    '    ResourceNode *nodePingBotStatus = new ResourceNode("/pingbot/status", "GET", &handlePingBotStatus);\n'
    '    ResourceNode *nodePingBotToggle = new ResourceNode("/pingbot/toggle", "POST", &handlePingBotToggle);\n'
    '    ResourceNode *nodeClockStatus = new ResourceNode("/clock/status", "GET", &handleClockStatus);\n'
    '    ResourceNode *nodeClockSync = new ResourceNode("/clock/sync", "POST", &handleClockSync);\n'
    '    ResourceNode *nodeTxPowerStatus = new ResourceNode("/radio/tx-power", "GET", &handleTxPowerStatus);\n'
    '    ResourceNode *nodeTxPowerSet = new ResourceNode("/radio/tx-power", "POST", &handleTxPowerSet);\n'
    '    ResourceNode *nodeListenBeforeTalkStatus = new ResourceNode("/radio/lbt", "GET", &handleListenBeforeTalkStatus);\n'
    '    ResourceNode *nodeListenBeforeTalkSet = new ResourceNode("/radio/lbt", "POST", &handleListenBeforeTalkSet);\n'
    '    ResourceNode *nodeDualBootUpdateRNode = new ResourceNode("/dualboot/update/rnode", "POST", &handleDualBootUpdateRNode);\n'
    '    ResourceNode *nodeDualBootUpdateMeshCore = new ResourceNode("/dualboot/update/meshcore", "POST", &handleDualBootUpdateMeshCore);\n'
    '    ResourceNode *nodePartitionTableBackup = new ResourceNode("/dualboot/partition-table", "GET", &handlePartitionTableBackup);\n'
    '    ResourceNode *nodeTripleBootMigration = new ResourceNode("/dualboot/migrate/tripleboot", "POST", &handleTripleBootMigration);',
)
text = replace_once(
    text,
    '    secureServer->registerNode(nodeRestart);',
    '    secureServer->registerNode(nodeRestart);\n'
    '    secureServer->registerNode(nodeDualBootStatus);\n'
    '    secureServer->registerNode(nodeDualBootRNode);\n'
    '    secureServer->registerNode(nodeDualBootMeshCore);\n'
    '    secureServer->registerNode(nodeDualBootAP);\n'
    '    secureServer->registerNode(nodeDualBootHome);',
)
text = replace_once(
    text,
    '    secureServer->registerNode(nodeDualBootHome);',
    '    secureServer->registerNode(nodeDualBootHome);\n'
    '    secureServer->registerNode(nodeNotificationStatus);\n'
    '    secureServer->registerNode(nodeNotificationRead);\n'
    '    secureServer->registerNode(nodePingBotStatus);\n'
    '    secureServer->registerNode(nodePingBotToggle);\n'
    '    secureServer->registerNode(nodeClockStatus);\n'
    '    secureServer->registerNode(nodeClockSync);\n'
    '    secureServer->registerNode(nodeTxPowerStatus);\n'
    '    secureServer->registerNode(nodeTxPowerSet);\n'
    '    secureServer->registerNode(nodeListenBeforeTalkStatus);\n'
    '    secureServer->registerNode(nodeListenBeforeTalkSet);\n'
    '    secureServer->registerNode(nodeDualBootUpdateRNode);\n'
    '    secureServer->registerNode(nodeDualBootUpdateMeshCore);\n'
    '    secureServer->registerNode(nodePartitionTableBackup);\n'
    '    secureServer->registerNode(nodeTripleBootMigration);',
)
text = replace_once(
    text,
    '    insecureServer->registerNode(nodeRestart);',
    '    insecureServer->registerNode(nodeRestart);\n'
    '    insecureServer->registerNode(nodeDualBootStatus);\n'
    '    insecureServer->registerNode(nodeDualBootRNode);\n'
    '    insecureServer->registerNode(nodeDualBootMeshCore);\n'
    '    insecureServer->registerNode(nodeDualBootAP);\n'
    '    insecureServer->registerNode(nodeDualBootHome);',
)
text = replace_once(
    text,
    '    insecureServer->registerNode(nodeDualBootHome);',
    '    insecureServer->registerNode(nodeDualBootHome);\n'
    '    insecureServer->registerNode(nodeNotificationStatus);\n'
    '    insecureServer->registerNode(nodeNotificationRead);\n'
    '    insecureServer->registerNode(nodePingBotStatus);\n'
    '    insecureServer->registerNode(nodePingBotToggle);\n'
    '    insecureServer->registerNode(nodeClockStatus);\n'
    '    insecureServer->registerNode(nodeClockSync);\n'
    '    insecureServer->registerNode(nodeTxPowerStatus);\n'
    '    insecureServer->registerNode(nodeTxPowerSet);\n'
    '    insecureServer->registerNode(nodeListenBeforeTalkStatus);\n'
    '    insecureServer->registerNode(nodeListenBeforeTalkSet);\n'
    '    insecureServer->registerNode(nodeDualBootUpdateRNode);\n'
    '    insecureServer->registerNode(nodeDualBootUpdateMeshCore);\n'
    '    insecureServer->registerNode(nodePartitionTableBackup);\n'
    '    insecureServer->registerNode(nodeTripleBootMigration);',
)
handler.write_text(text)

wifi_text = wifi_client.read_text()
wifi_text = replace_once(
    wifi_text,
    '#include "mesh/wifi/WiFiAPClient.h"',
    '#include "mesh/wifi/WiFiAPClient.h"\n#include "DualBootHandler.h"',
)
wifi_text = replace_once(
    wifi_text,
    'static bool wifiReconnectPending = false;',
    'static bool wifiReconnectPending = false;\n'
    '#if defined(ARCH_ESP32) && defined(E22_S3_N16R8)\n'
    'static constexpr unsigned long HOME_WIFI_FALLBACK_MS = 60000;\n'
    'static unsigned long homeWifiWaitStartMillis = 0;\n'
    'static bool homeWifiWaitStarted = false;\n'
    '#endif',
)
wifi_text = replace_once(
    wifi_text,
    'static int32_t reconnectWiFi()\n{',
    'static int32_t reconnectWiFi()\n{\n'
    '#if defined(ARCH_ESP32) && defined(E22_S3_N16R8)\n'
    '    if (isDualBootPortableAPActive())\n'
    '        return 300000;\n'
    '    if (config.network.wifi_enabled && !WiFi.isConnected()) {\n'
    '        if (!homeWifiWaitStarted) {\n'
    '            homeWifiWaitStartMillis = millis();\n'
    '            homeWifiWaitStarted = true;\n'
    '        } else if (millis() - homeWifiWaitStartMillis >= HOME_WIFI_FALLBACK_MS) {\n'
    '            isReconnecting = true;\n'
    '            if (startDualBootPortableAP(true)) {\n'
    '                needReconnect = false;\n'
    '                wifiReconnectPending = false;\n'
    '                onNetworkConnected();\n'
    '                LOG_WARN("Home WiFi unavailable for 60 seconds; portable AP ready at 192.168.4.1");\n'
    '                return 300000;\n'
    '            }\n'
    '            isReconnecting = false;\n'
    '            homeWifiWaitStartMillis = millis();\n'
    '            LOG_WARN("Portable AP fallback needs a saved AP password; continuing home WiFi attempts");\n'
    '        }\n'
    '    }\n'
    '#endif',
)
wifi_text = replace_once(
    wifi_text,
    '    case ARDUINO_EVENT_WIFI_STA_GOT_IP:\n'
    '        LOG_INFO("Obtained IP address: %s", WiFi.localIP().toString().c_str());',
    '    case ARDUINO_EVENT_WIFI_STA_GOT_IP:\n'
    '#if defined(E22_S3_N16R8)\n'
    '        homeWifiWaitStarted = false;\n'
    '#endif\n'
    '        LOG_INFO("Obtained IP address: %s", WiFi.localIP().toString().c_str());',
)
disconnect_guard = '        if (!isReconnecting) {'
guarded_disconnect = (
    '#if defined(E22_S3_N16R8)\n'
    '        if (!isReconnecting && !isDualBootPortableAPActive()) {\n'
    '#else\n'
    '        if (!isReconnecting) {\n'
    '#endif'
)
if guarded_disconnect not in wifi_text:
    if wifi_text.count(disconnect_guard) != 2:
        raise RuntimeError(
            f"Expected exactly two WiFi reconnect guards, found {wifi_text.count(disconnect_guard)}"
        )
    wifi_text = wifi_text.replace(disconnect_guard, guarded_disconnect)
wifi_text = replace_once(
    wifi_text,
    'bool initWifi()\n{',
    'bool initWifi()\n{\n'
    '#if defined(ARCH_ESP32) && defined(E22_S3_N16R8)\n'
    '    if (startDualBootPortableAP()) {\n'
    '#if !MESHTASTIC_EXCLUDE_WEBSERVER\n'
    '        createSSLCert();\n'
    '#endif\n'
    '        onNetworkConnected();\n'
    '        LOG_INFO("Portable WiFi AP ready: BarbieNode-Portable at 192.168.4.1");\n'
    '        return true;\n'
    '    }\n'
    '#endif',
)
wifi_client.write_text(wifi_text)

radio_text = radio_interface.read_text()
radio_text = replace_once(
    radio_text,
    '#include "RadioTxHook.h"',
    '#include "RadioTxHook.h"\n'
    '#if defined(E22_S3_N16R8)\n'
    '#include "DualBootHandler.h"\n'
    '#endif\n'
    '#if defined(E22_S3_N16R8) && !MESHTASTIC_EXCLUDE_REPLYBOT\n'
    '#include "modules/ReplyBotModule.h"\n'
    '#endif',
)
radio_config_text = radio_config.read_text()
# This particular installed E22-900M22S has an explicitly authorized 22 dBm
# ceiling in RU. Keep the exception board- and region-specific and cap it at
# the radio module's rated maximum. Other boards and regions retain upstream
# limits.
radio_config_text = replace_once(
    radio_config_text,
    '    if ((power == 0) || ((power > newRegion->powerLimit) && !devicestate.owner.is_licensed))\n'
    '        power = newRegion->powerLimit;',
    '#if defined(E22_S3_N16R8)\n'
    '    const uint8_t configuredPowerLimit =\n'
    '        newRegion->code == meshtastic_Config_LoRaConfig_RegionCode_RU ? 22 : newRegion->powerLimit;\n'
    '#else\n'
    '    const uint8_t configuredPowerLimit = newRegion->powerLimit;\n'
    '#endif\n'
    '    if ((power == 0) || ((power > configuredPowerLimit) && !devicestate.owner.is_licensed))\n'
    '        power = configuredPowerLimit;',
)
radio_config_text = replace_once(
    radio_config_text,
    '    if (myRegion->powerLimit)\n'
    '        maxPower = myRegion->powerLimit;',
    '    if (myRegion->powerLimit)\n'
    '        maxPower = myRegion->powerLimit;\n'
    '#if defined(E22_S3_N16R8)\n'
    '    if (myRegion->code == meshtastic_Config_LoRaConfig_RegionCode_RU)\n'
    '        maxPower = 22;\n'
    '#endif',
)
radio_config.write_text(radio_config_text)
radio_text = replace_once(
    radio_text,
    '                    if (isChannelActive()) { // check if there is currently a LoRa packet on the channel',
    '#if defined(E22_S3_N16R8)\n'
    '                    if (getListenBeforeTalkEnabled() && isChannelActive()) { // optional persistent LBT/CAD\n'
    '#else\n'
    '                    if (isChannelActive()) { // check if there is currently a LoRa packet on the channel\n'
    '#endif',
)
radio_text = replace_once(
    radio_text,
    '                return;\n'
    '            }\n\n'
    '            // Note: we deliver _all_ packets to our router',
    '                return;\n'
    '            }\n'
    '#if defined(E22_S3_N16R8) && !MESHTASTIC_EXCLUDE_REPLYBOT\n'
    '            notifyNotificationReceive();\n'
    '#endif\n\n'
    '            // Note: we deliver _all_ packets to our router',
)
radio_text = replace_once(
    radio_text,
    '            lastTxStart = Time::getMillis();\n'
    '            printPacket("Started Tx", txp);',
    '            lastTxStart = Time::getMillis();\n'
    '            printPacket("Started Tx", txp);\n'
    '#if defined(E22_S3_N16R8) && !MESHTASTIC_EXCLUDE_REPLYBOT\n'
    '            notifyNotificationTransmit();\n'
    '#endif',
)
radio_interface.write_text(radio_text)

mqtt_text = mqtt.read_text()
mqtt_text = replace_once(
    mqtt_text,
    "    if (map_position_precision < 12 || map_position_precision > 15) {",
    "#if defined(E22_S3_N16R8)\n"
    "    // Keep this board's deliberately displaced OneMesh marker inside\n"
    "    // Goncharovsky Park. This affects MQTT Map Reports only; ordinary\n"
    "    // LoRa position sharing retains the per-channel precision policy.\n"
    "    if (map_position_precision < 12 || map_position_precision > 16) {\n"
    "#else\n"
    "    if (map_position_precision < 12 || map_position_precision > 15) {\n"
    "#endif",
)
mqtt.write_text(mqtt_text)
print(f"Dual-boot handlers and autonomous bots installed in {root}")
