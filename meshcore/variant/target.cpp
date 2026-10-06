#include <Arduino.h>
#include "target.h"
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <Preferences.h>
#include <WebServer.h>
#include <WiFi.h>

namespace {
constexpr char MESHCORE_MARKER[] = "MESHCORE-E22-TRIPLEBOOT-V1";
constexpr char REMOTE_RETURN_MARKER[] = "MESHCORE-E22-REMOTE-RETURN-V1";
constexpr char MESHTASTIC_MARKER[] = "MESHTASTIC-E22-DUALBOOT-V1";
// Both signed-in build markers live in the image metadata/string area near
// the beginning (currently below 8 KiB).  Avoid scanning megabytes of mapped
// flash from a live BLE task just to validate the recovery target.
constexpr size_t IMAGE_SCAN_LIMIT = 64 * 1024;
constexpr char PREF_NAMESPACE[] = "RNodeDualBoot";
constexpr char FALLBACK_AP_SSID[] = "BarbieNode-MeshCore";
WebServer remoteServer(80);
bool remoteControlStarted = false;
bool fallbackAPStarted = false;
bool meshtasticBootPending = false;
bool restartPending = false;
bool meshtasticReturnAvailable = false;

bool containsMarker(const esp_partition_t *partition, const char *marker)
{
  if (!partition)
    return false;
  uint8_t buffer[512];
  size_t matched = 0;
  const size_t markerLength = strlen(marker);
  const size_t limit = min(partition->size, IMAGE_SCAN_LIMIT);
  for (size_t offset = 0; offset < limit; offset += sizeof(buffer)) {
    const size_t count = min(sizeof(buffer), limit - offset);
    if (esp_partition_read(partition, offset, buffer, count) != ESP_OK)
      return false;
    for (size_t index = 0; index < count; ++index) {
      if (buffer[index] == static_cast<uint8_t>(marker[matched])) {
        if (++matched == markerLength)
          return true;
      } else {
        matched = buffer[index] == static_cast<uint8_t>(marker[0]) ? 1 : 0;
      }
    }
  }
  return false;
}

void keepRadioOff()
{
  pinMode(P_LORA_NSS, OUTPUT);
  pinMode(P_LORA_RESET, OUTPUT);
  pinMode(SX126X_RXEN, OUTPUT);
  pinMode(SX126X_TXEN, OUTPUT);
  digitalWrite(P_LORA_NSS, HIGH);
  digitalWrite(P_LORA_RESET, HIGH);
  digitalWrite(SX126X_RXEN, LOW);
  digitalWrite(SX126X_TXEN, LOW);
}

void checkMeshtasticRecovery()
{
  keepRadioOff();
  pinMode(PIN_USER_BTN, INPUT_PULLUP);
  const uint32_t started = millis();
  while (digitalRead(PIN_USER_BTN) == LOW && millis() - started < 600)
    delay(10);
  if (digitalRead(PIN_USER_BTN) != LOW || millis() - started < 500)
    return;

  const esp_partition_t *meshtastic = esp_partition_find_first(
      ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_0, nullptr);
  if (containsMarker(meshtastic, MESHTASTIC_MARKER) && esp_ota_set_boot_partition(meshtastic) == ESP_OK) {
    delay(100);
    ESP.restart();
  }
}

void delayedMeshtasticBoot(void *)
{
  // Let the HTTP/BLE response leave the device before touching OTA metadata.
  delay(5000);
  const esp_partition_t *meshtastic = esp_partition_find_first(
      ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_0, nullptr);
  const esp_partition_t *otadata = esp_partition_find_first(
      ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_DATA_OTA, nullptr);
  // Clearing OTA selection is the board's canonical app0 recovery operation
  // (the same content as Espressif's boot_app0.bin at 0xe000). The bootloader
  // then deterministically falls back to the validated first app, Meshtastic.
  if (!meshtasticReturnAvailable || !meshtastic || !otadata ||
      esp_partition_erase_range(otadata, 0, otadata->size) != ESP_OK) {
    meshtasticBootPending = false;
    vTaskDelete(nullptr);
    return;
  }
  delay(250);
  ESP.restart();
}

void delayedRestart(void *)
{
  // Return the HTTP response before restarting the current MeshCore image.
  delay(1500);
  ESP.restart();
}

void remoteControlLoop(void *)
{
  const uint32_t started = millis();
  for (;;) {
    remoteServer.handleClient();
    if (!fallbackAPStarted && WiFi.status() != WL_CONNECTED && millis() - started > 60000) {
      Preferences prefs;
      String password;
      if (prefs.begin(PREF_NAMESPACE, true)) {
        password = prefs.getString("ap_psk", "");
        if (password.length() < 8)
          password = prefs.getString("psk", "");
        prefs.end();
      }
      if (password.length() >= 8 && password.length() <= 63) {
        WiFi.mode(WIFI_AP_STA);
        fallbackAPStarted = WiFi.softAP(FALLBACK_AP_SSID, password.c_str());
      }
    }
    delay(10);
  }
}

void startRemoteControl()
{
  if (remoteControlStarted)
    return;
  remoteControlStarted = true;
  board.setInhibitSleep(true);
  const esp_partition_t *meshtastic = esp_partition_find_first(
      ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_0, nullptr);
  meshtasticReturnAvailable = containsMarker(meshtastic, MESHTASTIC_MARKER);

  Preferences prefs;
  String ssid;
  String password;
  if (prefs.begin(PREF_NAMESPACE, true)) {
    ssid = prefs.getString("ssid", "");
    password = prefs.getString("psk", "");
    prefs.end();
  }
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  // ESP32-S3 coexistence requires Wi-Fi modem sleep while BLE is enabled.
  WiFi.setSleep(true);
  if (ssid.length() > 0)
    WiFi.begin(ssid.c_str(), password.c_str());
  // Register TCP only now, after BLE initialization has fully settled. The
  // earlier setup-time registration could start the Wi-Fi driver too soon and
  // leave the ESP32-S3 before either BLE or the recovery web UI became usable.
  startBarbieNodeWiFiCompanion();

  remoteServer.on("/", HTTP_GET, []() {
    remoteServer.send(200, "text/html; charset=utf-8",
      "<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'>"
      "<title>BarbieNode MeshCore</title><style>body{font:18px system-ui;max-width:42rem;margin:3rem auto;padding:1rem}"
      "button{font:inherit;padding:.8rem 1.2rem}</style><h1>MeshCore работает</h1>"
      "<p>Связь MeshCore идёт по LoRa; телефон управляет нодой по BLE. Мощность настраивается в MeshCore Companion.</p>"
      "<form method=post action='/restart'><button>Перезагрузить MeshCore</button></form>"
      "<form method=post action='/boot/meshtastic'><button>Вернуться в Meshtastic</button></form>");
  });
  remoteServer.on("/status", HTTP_GET, []() {
    const String ip = WiFi.status() == WL_CONNECTED ? WiFi.localIP().toString() : WiFi.softAPIP().toString();
    remoteServer.send(200, "application/json",
      String("{\"mode\":\"meshcore\",\"ip\":\"") + ip +
      "\",\"meshtastic_return\":true,\"remote_restart\":true,\"ble_command\":\"meshtastic\",\"power_min_dbm\":-9,\"power_max_dbm\":22}");
  });
  remoteServer.on("/restart", HTTP_POST, []() {
    if (restartPending) {
      remoteServer.send(202, "text/plain; charset=utf-8", "Перезагрузка уже запущена");
      return;
    }
    restartPending = true;
    remoteServer.send(200, "text/plain; charset=utf-8", "Перезагружаю MeshCore…");
    xTaskCreate(delayedRestart, "restart-meshcore", 2048, nullptr, 2, nullptr);
  });
  remoteServer.on("/boot/meshtastic", HTTP_POST, []() {
    if (!requestMeshtasticBoot()) {
      remoteServer.send(409, "text/plain; charset=utf-8", "Проверенный Meshtastic app0 не найден");
      return;
    }
    remoteServer.send(200, "text/plain; charset=utf-8", "Загружаю Meshtastic…");
  });
  remoteServer.begin();
  xTaskCreate(remoteControlLoop, "meshcore-web", 4096, nullptr, 1, nullptr);
}

void delayedRemoteControlStart(void *)
{
  // radio_init() runs before companion_radio finishes bringing up BLE.  On
  // ESP32-S3 starting Wi-Fi there makes the later BLE coexistence enable abort.
  // Let setup() finish the BLE controller first, then add the recovery web UI.
  delay(10000);
  startRemoteControl();
  vTaskDelete(nullptr);
}
} // namespace

bool requestMeshtasticBoot()
{
  if (meshtasticBootPending)
    return true;
  if (!meshtasticReturnAvailable)
    return false;
  meshtasticBootPending = true;
  xTaskCreate(delayedMeshtasticBoot, "boot-meshtastic", 3072, nullptr, 2, nullptr);
  return true;
}

ESP32Board board;
static SPIClass spi(FSPI);
RADIO_CLASS radio = new Module(P_LORA_NSS, P_LORA_DIO_1, P_LORA_RESET, P_LORA_BUSY, spi);
WRAPPER_CLASS radio_driver(radio, board);
ESP32RTCClock fallback_clock;
AutoDiscoverRTCClock rtc_clock(fallback_clock);
SensorManager sensors;

bool radio_init()
{
  // Keep the project marker in the binary for Meshtastic's app2 validator.
  Serial.println(MESHCORE_MARKER);
  Serial.println(REMOTE_RETURN_MARKER);
  checkMeshtasticRecovery();
  xTaskCreate(delayedRemoteControlStart, "meshcore-web-start", 4096, nullptr, 1, nullptr);
  fallback_clock.begin();
  rtc_clock.begin(Wire);
  spi.begin(P_LORA_SCLK, P_LORA_MISO, P_LORA_MOSI, P_LORA_NSS);

  const int status = radio.begin(LORA_FREQ, LORA_BW, LORA_SF, LORA_CR,
                                 RADIOLIB_SX126X_SYNC_WORD_PRIVATE, LORA_TX_POWER, 8,
                                 SX126X_DIO3_TCXO_VOLTAGE);
  if (status != RADIOLIB_ERR_NONE)
    return false;

  radio.setRfSwitchPins(SX126X_RXEN, SX126X_TXEN);
  radio.setCRC(1);
  radio.setCurrentLimit(SX126X_CURRENT_LIMIT);
  radio.setRxBoostedGainMode(SX126X_RX_BOOSTED_GAIN);
  return true;
}

mesh::LocalIdentity radio_new_identity()
{
  RadioNoiseListener rng(radio);
  return mesh::LocalIdentity(&rng);
}
