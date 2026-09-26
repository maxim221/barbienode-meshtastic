#include "DualBootHandler.h"
#include "configuration.h"

#if defined(ARCH_ESP32) && defined(E22_S3_N16R8)

#include "mesh/NodeDB.h"
#include "mesh/http/WebServer.h"
#include "modules/ReplyBotModule.h"
#include <HTTPRequest.hpp>
#include <HTTPResponse.hpp>
#include <Preferences.h>
#include <Update.h>
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <esp_idf_version.h>
#include <mbedtls/sha256.h>
#include <WiFi.h>
#include <cctype>
#include <cstring>
#include <string>

using namespace httpsserver;

namespace {
constexpr char RNODE_MARKER[] = "RNode-E22-DUALBOOT-V1";
constexpr char MESHTASTIC_MARKER[] = "MESHTASTIC-E22-DUALBOOT-V1";
constexpr size_t RNODE_SCAN_LIMIT = 3 * 1024 * 1024;
constexpr char PORTABLE_AP_SSID[] = "BarbieNode-Portable";
constexpr char PREF_NAMESPACE[] = "RNodeDualBoot";
bool portableAPActive = false;

const esp_partition_t *rnodePartition()
{
    return esp_partition_find_first(ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_1, nullptr);
}

bool partitionContainsMarker(const esp_partition_t *partition)
{
    if (!partition)
        return false;

    uint8_t buffer[512];
    size_t matched = 0;
    const size_t markerLength = strlen(RNODE_MARKER);
    const size_t limit = partition->size < RNODE_SCAN_LIMIT ? partition->size : RNODE_SCAN_LIMIT;
    for (size_t offset = 0; offset < limit; offset += sizeof(buffer)) {
        size_t count = limit - offset < sizeof(buffer) ? limit - offset : sizeof(buffer);
        if (esp_partition_read(partition, offset, buffer, count) != ESP_OK)
            return false;
        for (size_t i = 0; i < count; i++) {
            if (buffer[i] == static_cast<uint8_t>(RNODE_MARKER[matched])) {
                if (++matched == markerLength)
                    return true;
            } else {
                matched = buffer[i] == static_cast<uint8_t>(RNODE_MARKER[0]) ? 1 : 0;
            }
        }
    }
    return false;
}

bool validRNodeImage(const esp_partition_t *partition, esp_app_desc_t &description)
{
    return partition && esp_ota_get_partition_description(partition, &description) == ESP_OK &&
           partitionContainsMarker(partition);
}

void textResponse(HTTPResponse *res, int status, const char *message)
{
    res->setStatusCode(status);
    res->setHeader("Content-Type", "text/plain; charset=utf-8");
    res->setHeader("Cache-Control", "no-store");
    res->print(message);
}

bool validShaHeader(const std::string &value)
{
    if (value.size() != 64)
        return false;
    for (char c : value)
        if (!isxdigit(static_cast<unsigned char>(c)))
            return false;
    return true;
}

std::string shaHex(const uint8_t *digest)
{
    static constexpr char hex[] = "0123456789abcdef";
    std::string result(64, '0');
    for (size_t i = 0; i < 32; ++i) {
        result[i * 2] = hex[digest[i] >> 4];
        result[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    return result;
}

void shaStart(mbedtls_sha256_context *context)
{
#if ESP_IDF_VERSION_MAJOR >= 5
    mbedtls_sha256_starts(context, 0);
#else
    mbedtls_sha256_starts_ret(context, 0);
#endif
}

void shaUpdate(mbedtls_sha256_context *context, const uint8_t *data, size_t length)
{
#if ESP_IDF_VERSION_MAJOR >= 5
    mbedtls_sha256_update(context, data, length);
#else
    mbedtls_sha256_update_ret(context, data, length);
#endif
}

void shaFinish(mbedtls_sha256_context *context, uint8_t *digest)
{
#if ESP_IDF_VERSION_MAJOR >= 5
    mbedtls_sha256_finish(context, digest);
#else
    mbedtls_sha256_finish_ret(context, digest);
#endif
}
} // namespace

void handleDualBootStatus(HTTPRequest *, HTTPResponse *res)
{
    const esp_partition_t *partition = rnodePartition();
    esp_app_desc_t description = {};
    bool installed = validRNodeImage(partition, description);
    const esp_partition_t *running = esp_ota_get_running_partition();

    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    Preferences prefs;
    bool portable = portableAPActive;
    if (prefs.begin(PREF_NAMESPACE, true)) {
        portable = portable || prefs.getBool("portable", false);
        prefs.end();
    }

    res->printf("{\"mode\":\"%s\",\"portable_ap\":%s,\"portable_ssid\":\"%s\",\"portable_ip\":\"192.168.4.1\","
                "\"dualboot_marker\":\"%s\","
                "\"rnode_installed\":%s,\"partition\":\"%s\",\"version\":\"%s\",\"running\":\"%s\"}",
                portable ? "meshtastic_ap" : "meshtastic", portable ? "true" : "false", PORTABLE_AP_SSID, MESHTASTIC_MARKER,
                installed ? "true" : "false", partition ? partition->label : "", installed ? description.version : "",
                running ? running->label : "");
}

void handleDualBootUpdateRNode(HTTPRequest *req, HTTPResponse *res)
{
    const esp_partition_t *running = esp_ota_get_running_partition();
    const esp_partition_t *target = esp_ota_get_next_update_partition(nullptr);
    const size_t contentLength = req->getContentLength();
    std::string expectedSha = req->getHeader("X-Firmware-SHA256");
    for (char &c : expectedSha)
        c = static_cast<char>(tolower(static_cast<unsigned char>(c)));

    if (!running || !target || strcmp(running->label, "app0") != 0 || strcmp(target->label, "app1") != 0) {
        textResponse(res, 409, "Разметка dual-boot не соответствует app0 → app1");
        return;
    }
    if (req->getHeader("X-Firmware-Target") != "rnode-app1") {
        textResponse(res, 400, "Неверный целевой раздел");
        return;
    }
    if (!validShaHeader(expectedSha) || contentLength < 65536 || contentLength > target->size) {
        textResponse(res, 400, "Некорректный SHA-256 или размер образа");
        return;
    }
    if (!Update.begin(contentLength, U_FLASH)) {
        textResponse(res, 500, "Не удалось открыть app1 для записи");
        return;
    }

    mbedtls_sha256_context sha;
    mbedtls_sha256_init(&sha);
    shaStart(&sha);
    uint8_t buffer[1024];
    size_t written = 0;
    size_t markerPos = 0;
    bool markerFound = false;
    const size_t markerLength = strlen(RNODE_MARKER);
    uint32_t lastProgress = millis();
    while (!req->requestComplete()) {
        const size_t count = req->readBytes(buffer, sizeof(buffer));
        if (count == 0) {
            if (millis() - lastProgress > 15000) {
                Update.abort();
                mbedtls_sha256_free(&sha);
                textResponse(res, 408, "Загрузка прервана; Meshtastic продолжает работать");
                return;
            }
            delay(1);
            continue;
        }
        lastProgress = millis();
        for (size_t i = 0; i < count && !markerFound; ++i) {
            if (buffer[i] == static_cast<uint8_t>(RNODE_MARKER[markerPos])) {
                if (++markerPos == markerLength)
                    markerFound = true;
            } else {
                markerPos = buffer[i] == static_cast<uint8_t>(RNODE_MARKER[0]) ? 1 : 0;
            }
        }
        shaUpdate(&sha, buffer, count);
        if (Update.write(buffer, count) != count) {
            Update.abort();
            mbedtls_sha256_free(&sha);
            textResponse(res, 500, "Ошибка записи app1; Meshtastic продолжает работать");
            return;
        }
        written += count;
        yield();
    }

    uint8_t digest[32];
    shaFinish(&sha, digest);
    mbedtls_sha256_free(&sha);
    if (written != contentLength || !markerFound || shaHex(digest) != expectedSha) {
        Update.abort();
        textResponse(res, 400, !markerFound ? "Это не образ RNode dual-boot для BarbieNode" : "SHA-256 или размер не совпал");
        return;
    }
    if (!Update.end()) {
        textResponse(res, 400, "ESP32 отклонила образ как некорректный");
        return;
    }
    if (esp_ota_set_boot_partition(running) != ESP_OK) {
        textResponse(res, 500, "RNode записан, но не удалось оставить Meshtastic активным");
        return;
    }
    esp_app_desc_t description = {};
    if (!validRNodeImage(target, description)) {
        textResponse(res, 400, "Записанный app1 не прошёл итоговую проверку RNode");
        return;
    }
    textResponse(res, 200, "RNode проверен и записан в app1. Meshtastic остаётся активным до отдельного переключения.");
}

void handleNotificationStatus(HTTPRequest *, HTTPResponse *res)
{
    const uint16_t unread = getNotificationUnreadState();
    const uint8_t general = unread & 0xffU;
    const uint8_t direct = (unread >> 8) & 0xffU;
    const char *color = general && direct ? "white" : direct ? "red" : general ? "blue" : "green";
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"general_channels\":%u,\"direct_channels\":%u,\"color\":\"%s\",\"interval_seconds\":20}",
                general, direct, color);
}

void handleNotificationRead(HTTPRequest *req, HTTPResponse *res)
{
    char scope[8] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(scope), sizeof(scope) - 1);
    while (count > 0 && (scope[count - 1] == '\r' || scope[count - 1] == '\n' || scope[count - 1] == ' '))
        scope[--count] = '\0';
    if (strcmp(scope, "all") == 0) {
        clearNotificationUnread(-1);
    } else if (count == 1 && scope[0] >= '0' && scope[0] <= '7') {
        clearNotificationUnread(scope[0] - '0');
    } else {
        textResponse(res, 400, "Укажите all или номер канала от 0 до 7");
        return;
    }
    textResponse(res, 200, "Прочитано");
}

bool startDualBootPortableAP(bool automaticFallback)
{
    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, true))
        return false;
    bool enabled = prefs.getBool("portable", false);
    String password = prefs.getString("ap_psk", "");
    prefs.end();
    if ((!enabled && !automaticFallback) || password.length() < 8 || password.length() > 63)
        return false;

    WiFi.persistent(false);
    WiFi.mode(WIFI_AP);
    WiFi.setSleep(false);
    IPAddress local(192, 168, 4, 1);
    IPAddress gateway(192, 168, 4, 1);
    IPAddress subnet(255, 255, 255, 0);
    if (!WiFi.softAPConfig(local, gateway, subnet))
        return false;
    portableAPActive = WiFi.softAP(PORTABLE_AP_SSID, password.c_str());
    return portableAPActive;
}

bool isDualBootPortableAPActive()
{
    return portableAPActive;
}

void handleDualBootPortableAP(HTTPRequest *req, HTTPResponse *res)
{
    char password[65] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(password), sizeof(password) - 1);
    while (count > 0 && (password[count - 1] == '\r' || password[count - 1] == '\n'))
        password[--count] = '\0';
    if (count < 8 || count > 63) {
        textResponse(res, 400, "Пароль точки доступа должен содержать от 8 до 63 символов");
        return;
    }
    for (size_t i = 0; i < count; i++) {
        if (static_cast<unsigned char>(password[i]) < 32 || static_cast<unsigned char>(password[i]) > 126) {
            textResponse(res, 400, "Для совместимости используйте в пароле печатные латинские символы и цифры");
            return;
        }
    }

    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, false)) {
        textResponse(res, 500, "Не удалось открыть настройки портативного режима");
        return;
    }
    bool saved = prefs.putString("ap_psk", password) == count;
    saved = prefs.putBool("portable", true) > 0 && saved;
    prefs.end();
    if (!saved) {
        textResponse(res, 500, "Не удалось сохранить настройки портативного режима");
        return;
    }

    textResponse(res, 200, "Точка BarbieNode-Portable включается. Подключитесь к ней и откройте http://192.168.4.1");
    if (webServerThread)
        webServerThread->requestRestart = (millis() / 1000) + 3;
}

void handleDualBootHomeWiFi(HTTPRequest *, HTTPResponse *res)
{
    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, false)) {
        textResponse(res, 500, "Не удалось открыть настройки портативного режима");
        return;
    }
    prefs.putBool("portable", false);
    prefs.end();
    textResponse(res, 200, "Портативная точка отключается. Плата вернётся в сохранённую домашнюю Wi-Fi сеть.");
    if (webServerThread)
        webServerThread->requestRestart = (millis() / 1000) + 3;
}

void handleDualBootRNode(HTTPRequest *, HTTPResponse *res)
{
    const esp_partition_t *partition = rnodePartition();
    esp_app_desc_t description = {};
    if (!validRNodeImage(partition, description)) {
        textResponse(res, 409, "В разделе app1 нет проверенного образа RNode");
        return;
    }
    if (!config.network.wifi_enabled || config.network.wifi_ssid[0] == '\0') {
        textResponse(res, 409, "Сначала включите Wi-Fi клиента Meshtastic");
        return;
    }

    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, false)) {
        textResponse(res, 500, "Не удалось открыть настройки dual-boot");
        return;
    }
    bool saved = prefs.putString("ssid", config.network.wifi_ssid) > 0;
    saved = prefs.putString("psk", config.network.wifi_psk) > 0 && saved;
    saved = prefs.putBool("armed", true) > 0 && saved;
    prefs.putBool("portable", false);
    prefs.end();
    if (!saved) {
        textResponse(res, 500, "Не удалось сохранить параметры Wi-Fi для RNode");
        return;
    }
    if (esp_ota_set_boot_partition(partition) != ESP_OK) {
        textResponse(res, 500, "Загрузчик отказался выбрать раздел RNode");
        return;
    }

    textResponse(res, 200, "RNode выбран. Плата перезагрузится через 3 секунды.");
    if (webServerThread)
        webServerThread->requestRestart = (millis() / 1000) + 3;
}

#else

void handleDualBootStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootRNode(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootPortableAP(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootHomeWiFi(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootUpdateRNode(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleNotificationStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleNotificationRead(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
bool startDualBootPortableAP(bool) { return false; }
bool isDualBootPortableAPActive() { return false; }

#endif
