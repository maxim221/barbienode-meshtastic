#include "DualBootHandler.h"
#include "configuration.h"

#if defined(ARCH_ESP32) && defined(E22_S3_N16R8)

#include "mesh/NodeDB.h"
#include "mesh/MeshService.h"
#include "mesh/http/WebServer.h"
#include "modules/ReplyBotModule.h"
#include "gps/RTC.h"
#include <HTTPRequest.hpp>
#include <HTTPResponse.hpp>
#include <Preferences.h>
#include <Update.h>
#include <esp_ota_ops.h>
#include <esp_partition.h>
#include <esp_flash.h>
#include <esp_idf_version.h>
#include <mbedtls/sha256.h>
#include <WiFi.h>
#include <array>
#include <cctype>
#include <cstring>
#include <string>

using namespace httpsserver;

namespace {
constexpr char RNODE_MARKER[] = "RNode-E22-DUALBOOT-V1";
constexpr char MESHCORE_MARKER[] = "MESHCORE-E22-TRIPLEBOOT-V1";
constexpr char MESHCORE_REMOTE_RETURN_MARKER[] = "MESHCORE-E22-REMOTE-RETURN-V1";
constexpr char MESHTASTIC_MARKER[] = "MESHTASTIC-E22-DUALBOOT-V1";
constexpr char TRIPLEBOOT_TABLE_SHA256[] = "396adfba55f3c186d714a26899b1aa521fbf32e37e3779735eb63bd64e8ffdd0";
constexpr size_t PARTITION_TABLE_OFFSET = 0x8000;
constexpr size_t PARTITION_TABLE_LENGTH = 0xC00;
constexpr size_t PARTITION_TABLE_SECTOR_LENGTH = 0x1000;
constexpr size_t IMAGE_SCAN_LIMIT = 3 * 1024 * 1024;
constexpr char PORTABLE_AP_SSID[] = "BarbieNode-Portable";
constexpr char PREF_NAMESPACE[] = "RNodeDualBoot";
constexpr uint32_t CLOCK_VALID_TIME_FLOOR = 946684800; // 2000-01-01
constexpr int TX_POWER_MIN_DBM = 2;
constexpr int TX_POWER_MAX_DBM = 22;
bool portableAPActive = false;
uint32_t lastClockSyncEpoch = 0;
int32_t lastClockDeltaSeconds = 0;
uint32_t lastClockCorrectedRecords = 0;
int8_t listenBeforeTalkState = -1;
std::array<uint8_t, PARTITION_TABLE_LENGTH> partitionMigrationTable = {};
size_t partitionMigrationReceived = 0;

const esp_partition_t *rnodePartition()
{
    return esp_partition_find_first(ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_1, nullptr);
}

const esp_partition_t *meshCorePartition()
{
    return esp_partition_find_first(ESP_PARTITION_TYPE_APP, ESP_PARTITION_SUBTYPE_APP_OTA_2, nullptr);
}

bool partitionContainsMarker(const esp_partition_t *partition, const char *marker)
{
    if (!partition || !marker || marker[0] == '\0')
        return false;

    uint8_t buffer[512];
    size_t matched = 0;
    const size_t markerLength = strlen(marker);
    const size_t limit = partition->size < IMAGE_SCAN_LIMIT ? partition->size : IMAGE_SCAN_LIMIT;
    for (size_t offset = 0; offset < limit; offset += sizeof(buffer)) {
        size_t count = limit - offset < sizeof(buffer) ? limit - offset : sizeof(buffer);
        if (esp_partition_read(partition, offset, buffer, count) != ESP_OK)
            return false;
        for (size_t i = 0; i < count; i++) {
            if (buffer[i] == static_cast<uint8_t>(marker[matched])) {
                if (++matched == markerLength)
                    return true;
            } else {
                matched = buffer[i] == static_cast<uint8_t>(marker[0]) ? 1 : 0;
            }
        }
    }
    return false;
}

bool validRNodeImage(const esp_partition_t *partition, esp_app_desc_t &description)
{
    return partition && esp_ota_get_partition_description(partition, &description) == ESP_OK &&
           partitionContainsMarker(partition, RNODE_MARKER);
}

bool validMeshCoreImage(const esp_partition_t *partition, esp_app_desc_t &description)
{
    return partition && esp_ota_get_partition_description(partition, &description) == ESP_OK &&
           partitionContainsMarker(partition, MESHCORE_MARKER) &&
           partitionContainsMarker(partition, MESHCORE_REMOTE_RETURN_MARKER);
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
    const esp_partition_t *rnode = rnodePartition();
    const esp_partition_t *meshcore = meshCorePartition();
    esp_app_desc_t rnodeDescription = {};
    esp_app_desc_t meshcoreDescription = {};
    bool rnodeInstalled = validRNodeImage(rnode, rnodeDescription);
    bool meshcoreInstalled = validMeshCoreImage(meshcore, meshcoreDescription);
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
                "\"rnode_installed\":%s,\"partition\":\"%s\",\"version\":\"%s\","
                "\"meshcore_installed\":%s,\"meshcore_remote_return\":%s,\"meshcore_partition\":\"%s\",\"meshcore_version\":\"%s\","
                "\"meshcore_power_min_dbm\":-9,\"meshcore_power_max_dbm\":22,\"running\":\"%s\"}",
                portable ? "meshtastic_ap" : "meshtastic", portable ? "true" : "false", PORTABLE_AP_SSID, MESHTASTIC_MARKER,
                rnodeInstalled ? "true" : "false", rnode ? rnode->label : "",
                rnodeInstalled ? rnodeDescription.version : "", meshcoreInstalled ? "true" : "false",
                meshcoreInstalled ? "true" : "false",
                meshcore ? meshcore->label : "", meshcoreInstalled ? meshcoreDescription.version : "",
                running ? running->label : "");
}

void handleDualBootUpdateRNode(HTTPRequest *req, HTTPResponse *res)
{
    const esp_partition_t *running = esp_ota_get_running_partition();
    const esp_partition_t *target = rnodePartition();
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
    // Triple-boot has two inactive candidates. Select RNode explicitly so the
    // generic Arduino OTA rotation cannot overwrite MeshCore app2.
    if (!Update.begin(contentLength, U_FLASH, -1, LOW, "app1")) {
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

void handleDualBootUpdateMeshCore(HTTPRequest *req, HTTPResponse *res)
{
    const esp_partition_t *running = esp_ota_get_running_partition();
    const esp_partition_t *target = meshCorePartition();
    const size_t contentLength = req->getContentLength();
    std::string expectedSha = req->getHeader("X-Firmware-SHA256");
    for (char &c : expectedSha)
        c = static_cast<char>(tolower(static_cast<unsigned char>(c)));

    if (!running || !target || strcmp(running->label, "app0") != 0 || strcmp(target->label, "app2") != 0) {
        textResponse(res, 409, "Разметка triple-boot не соответствует app0 → app2");
        return;
    }
    if (req->getHeader("X-Firmware-Target") != "meshcore-app2") {
        textResponse(res, 400, "Неверный целевой раздел");
        return;
    }
    if (!validShaHeader(expectedSha) || contentLength < 65536 || contentLength > target->size) {
        textResponse(res, 400, "Некорректный SHA-256 или размер образа");
        return;
    }

    esp_ota_handle_t updateHandle = 0;
    if (esp_ota_begin(target, contentLength, &updateHandle) != ESP_OK) {
        textResponse(res, 500, "Не удалось открыть app2 для записи");
        return;
    }

    mbedtls_sha256_context sha;
    mbedtls_sha256_init(&sha);
    shaStart(&sha);
    uint8_t buffer[1024];
    size_t written = 0;
    size_t markerPos = 0;
    bool markerFound = false;
    const size_t markerLength = strlen(MESHCORE_MARKER);
    uint32_t lastProgress = millis();
    while (!req->requestComplete()) {
        const size_t count = req->readBytes(buffer, sizeof(buffer));
        if (count == 0) {
            if (millis() - lastProgress > 15000) {
                esp_ota_abort(updateHandle);
                mbedtls_sha256_free(&sha);
                textResponse(res, 408, "Загрузка прервана; Meshtastic продолжает работать");
                return;
            }
            delay(1);
            continue;
        }
        lastProgress = millis();
        for (size_t i = 0; i < count && !markerFound; ++i) {
            if (buffer[i] == static_cast<uint8_t>(MESHCORE_MARKER[markerPos])) {
                if (++markerPos == markerLength)
                    markerFound = true;
            } else {
                markerPos = buffer[i] == static_cast<uint8_t>(MESHCORE_MARKER[0]) ? 1 : 0;
            }
        }
        shaUpdate(&sha, buffer, count);
        if (esp_ota_write(updateHandle, buffer, count) != ESP_OK) {
            esp_ota_abort(updateHandle);
            mbedtls_sha256_free(&sha);
            textResponse(res, 500, "Ошибка записи app2; Meshtastic продолжает работать");
            return;
        }
        written += count;
        yield();
    }

    uint8_t digest[32];
    shaFinish(&sha, digest);
    mbedtls_sha256_free(&sha);
    if (written != contentLength || !markerFound || shaHex(digest) != expectedSha) {
        esp_ota_abort(updateHandle);
        textResponse(res, 400,
                     !markerFound ? "Это не образ MeshCore triple-boot для BarbieNode" : "SHA-256 или размер не совпал");
        return;
    }
    if (esp_ota_end(updateHandle) != ESP_OK) {
        textResponse(res, 400, "ESP32 отклонила образ как некорректный");
        return;
    }
    if (esp_ota_set_boot_partition(running) != ESP_OK) {
        textResponse(res, 500, "MeshCore записан, но не удалось оставить Meshtastic активным");
        return;
    }
    esp_app_desc_t description = {};
    if (!validMeshCoreImage(target, description)) {
        textResponse(res, 400, "Записанный app2 не прошёл итоговую проверку MeshCore");
        return;
    }
    textResponse(res, 200, "MeshCore проверен и записан в app2. Meshtastic остаётся активным до отдельного переключения.");
}

void handlePartitionTableBackup(HTTPRequest *, HTTPResponse *res)
{
    std::array<uint8_t, PARTITION_TABLE_SECTOR_LENGTH> table = {};
    if (esp_flash_read(esp_flash_default_chip, table.data(), PARTITION_TABLE_OFFSET, table.size()) != ESP_OK) {
        textResponse(res, 500, "Не удалось прочитать таблицу разделов");
        return;
    }
    res->setHeader("Content-Type", "application/octet-stream");
    res->setHeader("Content-Disposition", "attachment; filename=barbienode-partitions-before-tripleboot.bin");
    res->setHeader("Cache-Control", "no-store");
    res->write(table.data(), table.size());
}

void handleTripleBootMigration(HTTPRequest *req, HTTPResponse *res)
{
    const esp_partition_t *running = esp_ota_get_running_partition();
    const esp_partition_t *app1 = rnodePartition();
    const esp_partition_t *app2 = meshCorePartition();
    const esp_partition_t *spiffs = esp_partition_find_first(
        ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_DATA_SPIFFS, "spiffs");
    esp_app_desc_t rnodeDescription = {};

    if (!running || strcmp(running->label, "app0") != 0 || running->address != 0x10000 ||
        running->size != 0x640000 || !app1 || app1->address != 0x650000 || app1->size != 0x640000 ||
        app2 || !spiffs || spiffs->address != 0xC90000 || spiffs->size != 0x360000 ||
        !validRNodeImage(app1, rnodeDescription)) {
        textResponse(res, 409, "Текущая разметка или RNode не соответствует проверенному dual-boot");
        return;
    }
    if (req->getHeader("X-Firmware-Target") != "barbienode-tripleboot-partitions" ||
        req->getHeader("X-Firmware-SHA256") != TRIPLEBOOT_TABLE_SHA256) {
        textResponse(res, 400, "Неверная цель или SHA-256 таблицы разделов");
        return;
    }

    if (req->getHeader("X-Partition-Commit") != "true") {
        const std::string offsetHeader = req->getHeader("X-Partition-Offset");
        char *end = nullptr;
        const unsigned long offset = strtoul(offsetHeader.c_str(), &end, 10);
        const size_t count = req->getContentLength();
        if (offsetHeader.empty() || end == offsetHeader.c_str() || *end != '\0' || count == 0 || count > 256 ||
            offset + count > partitionMigrationTable.size()) {
            textResponse(res, 400, "Некорректный offset или размер фрагмента таблицы");
            return;
        }
        if (offset == 0)
            partitionMigrationReceived = 0;
        if (offset != partitionMigrationReceived) {
            textResponse(res, 409, "Фрагменты таблицы должны поступать последовательно");
            return;
        }
        const size_t actual = req->readBytes(partitionMigrationTable.data() + offset, count);
        if (actual != count) {
            partitionMigrationReceived = 0;
            textResponse(res, 400, "Фрагмент таблицы получен не полностью; буфер сброшен");
            return;
        }
        partitionMigrationReceived += actual;
        res->setHeader("Content-Type", "application/json");
        res->setHeader("Cache-Control", "no-store");
        res->printf("{\"received\":%u,\"required\":%u}", static_cast<unsigned>(partitionMigrationReceived),
                    static_cast<unsigned>(partitionMigrationTable.size()));
        return;
    }
    if (req->getContentLength() != 0 || partitionMigrationReceived != partitionMigrationTable.size()) {
        textResponse(res, 409, "Таблица разделов ещё не загружена полностью");
        return;
    }

    uint8_t digest[32];
    mbedtls_sha256_context sha;
    mbedtls_sha256_init(&sha);
    shaStart(&sha);
    shaUpdate(&sha, partitionMigrationTable.data(), partitionMigrationTable.size());
    shaFinish(&sha, digest);
    mbedtls_sha256_free(&sha);
    if (shaHex(digest) != TRIPLEBOOT_TABLE_SHA256) {
        partitionMigrationReceived = 0;
        textResponse(res, 400, "Содержимое таблицы разделов не прошло проверку SHA-256");
        return;
    }

    // Register the protected primary partition-table sector and use ESP-IDF's
    // dedicated OTA path.  Unlike generic raw flash writes, this path safely
    // toggles dangerous-write protection and validates partition-table data.
    const esp_partition_t *tableSector = nullptr;
    if (esp_partition_register_external(nullptr, PARTITION_TABLE_OFFSET, PARTITION_TABLE_SECTOR_LENGTH,
                                        "PrimaryPrtTable", ESP_PARTITION_TYPE_PARTITION_TABLE,
                                        ESP_PARTITION_SUBTYPE_PARTITION_TABLE_PRIMARY, &tableSector) != ESP_OK) {
        textResponse(res, 500, "Не удалось зарегистрировать сектор таблицы разделов");
        return;
    }
    esp_ota_handle_t tableUpdate = 0;
    esp_err_t writeResult = esp_ota_begin(tableSector, OTA_WITH_SEQUENTIAL_WRITES, &tableUpdate);
    if (writeResult == ESP_OK)
        writeResult = esp_ota_write(tableUpdate, partitionMigrationTable.data(), partitionMigrationTable.size());
    if (writeResult == ESP_OK)
        writeResult = esp_ota_end(tableUpdate);
    else if (tableUpdate != 0)
        esp_ota_abort(tableUpdate);
    if (writeResult != ESP_OK) {
        esp_partition_deregister_external(tableSector);
        textResponse(res, 500, "Ошибка записи таблицы разделов; не перезагружайте плату");
        return;
    }

    std::array<uint8_t, PARTITION_TABLE_LENGTH> verify = {};
    if (esp_partition_read_raw(tableSector, 0, verify.data(), verify.size()) != ESP_OK ||
        memcmp(partitionMigrationTable.data(), verify.data(), partitionMigrationTable.size()) != 0) {
        esp_partition_deregister_external(tableSector);
        textResponse(res, 500, "Проверка записанной таблицы разделов не пройдена; не перезагружайте плату");
        return;
    }
    esp_partition_deregister_external(tableSector);
    partitionMigrationReceived = 0;
    textResponse(res, 200, "Triple-boot таблица записана и проверена. Выполните отдельную перезагрузку.");
}

void handleNotificationStatus(HTTPRequest *, HTTPResponse *res)
{
    const uint16_t unread = getNotificationUnreadState();
    const uint8_t general = unread & 0xffU;
    const uint8_t direct = (unread >> 8) & 0xffU;
    const char *color = general && direct ? "white" : direct ? "red" : general ? "blue" : "green";
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"general_channels\":%u,\"direct_channels\":%u,\"color\":\"%s\",\"interval_seconds\":20,"
                "\"rx_packets\":%lu,\"tx_packets\":%lu}",
                general, direct, color, static_cast<unsigned long>(getNotificationReceiveCount()),
                static_cast<unsigned long>(getNotificationTransmitCount()));
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

void handlePingBotStatus(HTTPRequest *, HTTPResponse *res)
{
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"enabled\":%s}", getPingBotEnabled() ? "true" : "false");
}

void handlePingBotToggle(HTTPRequest *req, HTTPResponse *res)
{
    char value[8] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(value), sizeof(value) - 1);
    while (count > 0 && (value[count - 1] == '\r' || value[count - 1] == '\n' || value[count - 1] == ' '))
        value[--count] = '\0';
    bool enabled;
    if (strcmp(value, "true") == 0 || strcmp(value, "1") == 0 || strcmp(value, "on") == 0) {
        enabled = true;
    } else if (strcmp(value, "false") == 0 || strcmp(value, "0") == 0 || strcmp(value, "off") == 0) {
        enabled = false;
    } else {
        textResponse(res, 400, "Укажите true или false");
        return;
    }
    if (!setPingBotEnabled(enabled)) {
        textResponse(res, 500, "Не удалось сохранить состояние Ping-бота");
        return;
    }
    textResponse(res, 200, enabled ? "Ping-бот включён" : "Ping-бот выключен");
}

void handleClockStatus(HTTPRequest *, HTTPResponse *res)
{
    const uint32_t now = getValidTime(RTCQualityDevice, false);
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"time\":%lu,\"quality\":%u,\"last_sync\":%lu,\"last_delta_seconds\":%ld,"
                "\"corrected_archive_records\":%lu}",
                static_cast<unsigned long>(now), static_cast<unsigned>(getRTCQuality()),
                static_cast<unsigned long>(lastClockSyncEpoch), static_cast<long>(lastClockDeltaSeconds),
                static_cast<unsigned long>(lastClockCorrectedRecords));
}

void handleClockSync(HTTPRequest *req, HTTPResponse *res)
{
    char value[24] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(value), sizeof(value) - 1);
    while (count > 0 && isspace(static_cast<unsigned char>(value[count - 1])))
        value[--count] = '\0';
    char *end = nullptr;
    const unsigned long requested = strtoul(value, &end, 10);
    if (count == 0 || end == value || *end != '\0' || requested < 1700000000UL || requested > 4102444800UL) {
        textResponse(res, 400, "Укажите Unix-время от доверенного локального источника");
        return;
    }

    const uint32_t before = getValidTime(RTCQualityDevice, false);
    struct timeval tv = {static_cast<time_t>(requested), 0};
    const RTCSetResult result = perhapsSetRTC(RTCQualityNTP, &tv, true);
    if (result != RTCSetResultSuccess) {
        textResponse(res, 500, "Плата отклонила установку времени");
        return;
    }
    lastClockSyncEpoch = requested;
    lastClockDeltaSeconds = before >= CLOCK_VALID_TIME_FLOOR
                                ? static_cast<int32_t>(static_cast<int64_t>(requested) - static_cast<int64_t>(before))
                                : 0;
    lastClockCorrectedRecords = normalizeNightbotArchiveTimestamps(requested);

    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"ok\":true,\"time\":%lu,\"previous_time\":%lu,\"delta_seconds\":%ld,"
                "\"corrected_archive_records\":%lu}",
                requested, static_cast<unsigned long>(before), static_cast<long>(lastClockDeltaSeconds),
                static_cast<unsigned long>(lastClockCorrectedRecords));
}

void handleTxPowerStatus(HTTPRequest *, HTTPResponse *res)
{
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"configured_dbm\":%d,\"min_dbm\":%d,\"max_dbm\":%d}",
                static_cast<int>(config.lora.tx_power), TX_POWER_MIN_DBM, TX_POWER_MAX_DBM);
}

void handleTxPowerSet(HTTPRequest *req, HTTPResponse *res)
{
    char value[8] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(value), sizeof(value) - 1);
    while (count > 0 && isspace(static_cast<unsigned char>(value[count - 1])))
        value[--count] = '\0';
    char *end = nullptr;
    const long requested = strtol(value, &end, 10);
    if (count == 0 || end == value || *end != '\0' || requested < TX_POWER_MIN_DBM || requested > TX_POWER_MAX_DBM) {
        textResponse(res, 400, "Допустимая мощность этой платы: 2–22 dBm");
        return;
    }
    if (!service) {
        textResponse(res, 503, "Служба конфигурации платы ещё не готова");
        return;
    }

    config.lora.tx_power = static_cast<int32_t>(requested);
    service->reloadConfig(SEGMENT_CONFIG);

    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"ok\":true,\"requested_dbm\":%ld,\"applied_dbm\":%d}", requested,
                static_cast<int>(config.lora.tx_power));
}

bool getListenBeforeTalkEnabled()
{
    if (listenBeforeTalkState >= 0)
        return listenBeforeTalkState != 0;
    Preferences prefs;
    bool enabled = true;
    if (prefs.begin(PREF_NAMESPACE, true)) {
        enabled = prefs.getBool("lbt", true);
        prefs.end();
    }
    listenBeforeTalkState = enabled ? 1 : 0;
    return enabled;
}

bool setListenBeforeTalkEnabled(bool enabled)
{
    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, false))
        return false;
    const bool saved = prefs.putBool("lbt", enabled) > 0;
    prefs.end();
    if (saved)
        listenBeforeTalkState = enabled ? 1 : 0;
    return saved;
}

void handleListenBeforeTalkStatus(HTTPRequest *, HTTPResponse *res)
{
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"enabled\":%s,\"method\":\"CAD\",\"persistent\":true}",
                getListenBeforeTalkEnabled() ? "true" : "false");
}

void handleListenBeforeTalkSet(HTTPRequest *req, HTTPResponse *res)
{
    char value[8] = {};
    size_t count = req->readBytes(reinterpret_cast<uint8_t *>(value), sizeof(value) - 1);
    while (count > 0 && isspace(static_cast<unsigned char>(value[count - 1])))
        value[--count] = '\0';
    bool enabled;
    if (strcmp(value, "true") == 0 || strcmp(value, "1") == 0 || strcmp(value, "on") == 0) {
        enabled = true;
    } else if (strcmp(value, "false") == 0 || strcmp(value, "0") == 0 || strcmp(value, "off") == 0) {
        enabled = false;
    } else {
        textResponse(res, 400, "Укажите true или false");
        return;
    }
    if (!setListenBeforeTalkEnabled(enabled)) {
        textResponse(res, 500, "Не удалось сохранить режим LBT");
        return;
    }
    res->setHeader("Content-Type", "application/json");
    res->setHeader("Cache-Control", "no-store");
    res->printf("{\"ok\":true,\"enabled\":%s,\"method\":\"CAD\"}", enabled ? "true" : "false");
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

bool isDualBootPortableAPUnattended()
{
    return portableAPActive && WiFi.softAPgetStationNum() == 0;
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

void handleDualBootMeshCore(HTTPRequest *, HTTPResponse *res)
{
    const esp_partition_t *partition = meshCorePartition();
    esp_app_desc_t description = {};
    if (!validMeshCoreImage(partition, description)) {
        textResponse(res, 409, "В разделе app2 нет проверенного образа MeshCore");
        return;
    }
    if (!config.network.wifi_enabled || config.network.wifi_ssid[0] == '\0') {
        textResponse(res, 409, "Сначала включите Wi-Fi клиента Meshtastic: он нужен для веб-возврата");
        return;
    }

    Preferences prefs;
    if (!prefs.begin(PREF_NAMESPACE, false)) {
        textResponse(res, 500, "Не удалось открыть настройки удалённого возврата");
        return;
    }
    bool saved = prefs.putString("ssid", config.network.wifi_ssid) > 0;
    saved = prefs.putString("psk", config.network.wifi_psk) > 0 && saved;
    prefs.putBool("portable", false);
    prefs.end();
    if (!saved) {
        textResponse(res, 500, "Не удалось сохранить Wi-Fi для веб-возврата из MeshCore");
        return;
    }
    if (esp_ota_set_boot_partition(partition) != ESP_OK) {
        textResponse(res, 500, "Загрузчик отказался выбрать раздел MeshCore");
        return;
    }

    textResponse(res, 200, "MeshCore выбран. Плата останется на этом Wi-Fi; на её веб-странице будет кнопка возврата в Meshtastic.");
    if (webServerThread)
        webServerThread->requestRestart = (millis() / 1000) + 3;
}

#else

void handleDualBootStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootRNode(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootMeshCore(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootPortableAP(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootHomeWiFi(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootUpdateRNode(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleDualBootUpdateMeshCore(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handlePartitionTableBackup(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleTripleBootMigration(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleNotificationStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleNotificationRead(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handlePingBotStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handlePingBotToggle(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleListenBeforeTalkStatus(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
void handleListenBeforeTalkSet(httpsserver::HTTPRequest *, httpsserver::HTTPResponse *) {}
bool getListenBeforeTalkEnabled() { return true; }
bool setListenBeforeTalkEnabled(bool) { return false; }
bool startDualBootPortableAP(bool) { return false; }
bool isDualBootPortableAPActive() { return false; }
bool isDualBootPortableAPUnattended() { return false; }

#endif
