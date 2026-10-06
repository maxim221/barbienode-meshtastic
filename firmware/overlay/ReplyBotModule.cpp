#include "configuration.h"
#if !MESHTASTIC_EXCLUDE_REPLYBOT

#include "ReplyBotModule.h"
#include "DualBootHandler.h"

#include "Channels.h"
#include "FSCommon.h"
#include "MeshService.h"
#include "NodeDB.h"
#include "SPILock.h"
#include "SafeFile.h"
#include "gps/RTC.h"
#include "mesh/MeshTypes.h"

#include <Adafruit_NeoPixel.h>
#include <Arduino.h>
#include <Preferences.h>
#include <atomic>
#include <cctype>
#include <cstring>

namespace
{
constexpr uint32_t STATE_MAGIC = 0x4e424f54; // NBOT
constexpr uint16_t STATE_VERSION = 1;
constexpr uint32_t NIGHT_START_EPOCH = 1790280000; // 2026-09-24 23:00 MSK
constexpr uint32_t NIGHT_END_EPOCH = 1790312400;   // 2026-09-25 08:00 MSK
constexpr uint32_t BEACON_INTERVAL_SECONDS = 60 * 60;
// Deliberately off in the published and reproducible source. Enabling a
// periodic public RF transmission requires a separate explicit decision.
constexpr bool ENABLE_SCHEDULED_BEACON = false;
constexpr uint32_t THREAD_INTERVAL_MS = 5000;
constexpr uint32_t HEARTBEAT_PULSE_MS = 250;
constexpr uint32_t HEARTBEAT_REST_MS = 20 * 1000 - HEARTBEAT_PULSE_MS;
constexpr uint32_t PORTABLE_RAINBOW_STEP_MS = 120;
constexpr uint16_t PORTABLE_RAINBOW_HUE_STEP = 768;
constexpr uint8_t PORTABLE_RAINBOW_VALUE = 20;
constexpr size_t MAX_LOG_BYTES = 256 * 1024;

constexpr const char *STATE_PATH = "/nightbot.state";
constexpr const char *LOG_PATH = "/static/nightbot.jsonl";
constexpr const char *OLD_LOG_PATH = "/static/nightbot.previous.jsonl";
constexpr const char *SENT_LOG_PATH = "/static/nightbot.sent.jsonl";
constexpr uint32_t VALID_ARCHIVE_TIME_FLOOR = 946684800; // 2000-01-01
constexpr uint32_t ARCHIVE_FUTURE_TOLERANCE = 5 * 60;
constexpr const char *BEACON_TEXT = "Привет! Спишь?";
constexpr const char *AWAY_TEXT = "Сейчас я не у компьютера. Постараюсь связаться с вами, когда вернусь";
constexpr const char *PING_CHANNEL_NAME = "Ping";
constexpr const char *PING_LOCATION = "Бутырский";
constexpr uint32_t PING_GLOBAL_COOLDOWN_MS = 30 * 1000;
constexpr uint32_t PING_SENDER_COOLDOWN_MS = 5 * 60 * 1000;
constexpr uint8_t PING_COOLDOWN_SLOTS = 16;
constexpr const char *NOTIFICATION_PREF_NAMESPACE = "BarbieNotify";
constexpr const char *NOTIFICATION_PREF_KEY = "unread";
constexpr const char *PING_BOT_PREF_KEY = "pingbot";
constexpr const char *PING_BOT_STATE_PATH = "/pingbot.state";
constexpr uint8_t NOTIFICATION_LED_PIN = 48;

std::atomic<uint16_t> notificationUnread{0};
std::atomic<uint32_t> notificationTransmitCount{0};
std::atomic<uint32_t> notificationReceiveCount{0};
std::atomic<bool> pingBotEnabled{true};
std::atomic<bool> pingBotStateLoaded{false};
Adafruit_NeoPixel notificationPixel(1, NOTIFICATION_LED_PIN, NEO_GRB + NEO_KHZ800);

struct PingCooldownEntry {
    uint32_t sender = 0;
    uint32_t lastReplyMs = 0;
};

PingCooldownEntry pingCooldown[PING_COOLDOWN_SLOTS];
uint8_t pingCooldownCursor = 0;
uint32_t lastPingReplyMs = 0;

void persistNotificationUnread(uint16_t value)
{
    Preferences prefs;
    if (prefs.begin(NOTIFICATION_PREF_NAMESPACE, false)) {
        prefs.putUShort(NOTIFICATION_PREF_KEY, value);
        prefs.end();
    }
}

void loadNotificationUnread()
{
    Preferences prefs;
    if (prefs.begin(NOTIFICATION_PREF_NAMESPACE, true)) {
        notificationUnread.store(prefs.getUShort(NOTIFICATION_PREF_KEY, 0));
        pingBotEnabled.store(prefs.getBool(PING_BOT_PREF_KEY, true));
        pingBotStateLoaded.store(true);
        prefs.end();
    }
#ifdef FSCom
    concurrency::LockGuard guard(spiLock);
    if (FSCom.exists(PING_BOT_STATE_PATH)) {
        File file = FSCom.open(PING_BOT_STATE_PATH, FILE_O_READ);
        if (file) {
            const int stored = file.read();
            file.close();
            if (stored == 0 || stored == 1) {
                pingBotEnabled.store(stored == 1);
                pingBotStateLoaded.store(true);
            }
        }
    }
#endif
}

void markNotificationUnread(uint8_t channel, bool direct)
{
    if (channel >= 8)
        return;
    const uint16_t bit = static_cast<uint16_t>(1U << channel) << (direct ? 8 : 0);
    const uint16_t before = notificationUnread.fetch_or(bit);
    if ((before & bit) == 0)
        persistNotificationUnread(notificationUnread.load());
}

bool isExactPing(const uint8_t *payload, size_t length)
{
    while (length && std::isspace(static_cast<unsigned char>(*payload))) {
        ++payload;
        --length;
    }
    while (length && std::isspace(static_cast<unsigned char>(payload[length - 1])))
        --length;
    if (length != 4)
        return false;
    constexpr char expected[] = "ping";
    for (size_t i = 0; i < length; ++i) {
        if (std::tolower(static_cast<unsigned char>(payload[i])) != expected[i])
            return false;
    }
    return true;
}

bool pingRateLimited(uint32_t sender)
{
    const uint32_t now = millis();
    if (lastPingReplyMs && static_cast<uint32_t>(now - lastPingReplyMs) < PING_GLOBAL_COOLDOWN_MS)
        return true;
    for (auto &entry : pingCooldown) {
        if (entry.sender != sender)
            continue;
        if (static_cast<uint32_t>(now - entry.lastReplyMs) < PING_SENDER_COOLDOWN_MS)
            return true;
        entry.lastReplyMs = now;
        lastPingReplyMs = now;
        return false;
    }
    pingCooldown[pingCooldownCursor] = {sender, now};
    pingCooldownCursor = (pingCooldownCursor + 1) % PING_COOLDOWN_SLOTS;
    lastPingReplyMs = now;
    return false;
}

void writeJsonString(File &file, const uint8_t *value, size_t length)
{
    file.write('"');
    for (size_t i = 0; i < length; ++i) {
        const uint8_t c = value[i];
        switch (c) {
        case '"':
            file.print("\\\"");
            break;
        case '\\':
            file.print("\\\\");
            break;
        case '\n':
            file.print("\\n");
            break;
        case '\r':
            file.print("\\r");
            break;
        case '\t':
            file.print("\\t");
            break;
        default:
            if (c >= 0x20) {
                file.write(c);
            } else {
                file.printf("\\u%04x", c);
            }
        }
    }
    file.write('"');
}

bool archiveTimestamp(const String &line, uint32_t &value, int &start, int &end)
{
    start = line.indexOf("\"ts\":");
    if (start < 0)
        return false;
    start += 5;
    end = start;
    while (end < static_cast<int>(line.length()) && isdigit(static_cast<unsigned char>(line[end])))
        ++end;
    if (end == start)
        return false;
    value = strtoul(line.substring(start, end).c_str(), nullptr, 10);
    return true;
}

bool copyArchiveFile(const char *sourcePath, const char *backupPath)
{
    if (FSCom.exists(backupPath))
        return true;
    File source = FSCom.open(sourcePath, FILE_O_READ);
    File backup = FSCom.open(backupPath, FILE_O_WRITE);
    if (!source || !backup) {
        if (source)
            source.close();
        if (backup)
            backup.close();
        return false;
    }
    uint8_t buffer[512];
    while (source.available()) {
        const size_t count = source.read(buffer, sizeof(buffer));
        if (count == 0 || backup.write(buffer, count) != count) {
            source.close();
            backup.close();
            FSCom.remove(backupPath);
            return false;
        }
    }
    source.close();
    backup.flush();
    backup.close();
    return true;
}

uint32_t normalizeArchiveFile(const char *path, const char *temporaryPath, const char *backupPath, uint32_t exactEpoch)
{
    if (!FSCom.exists(path))
        return 0;

    File scan = FSCom.open(path, FILE_O_READ);
    if (!scan)
        return 0;
    uint32_t previousAnchor = 0, nextAnchor = 0, firstFuture = 0, lastFuture = 0;
    uint32_t futureCount = 0, lastValid = 0;
    bool blockEnded = false, multipleBlocks = false, nonCanonicalLineEnding = false;
    while (scan.available()) {
        String line = scan.readStringUntil('\n');
        nonCanonicalLineEnding = nonCanonicalLineEnding || line.endsWith("\r\r");
        uint32_t timestamp = 0;
        int start = 0, end = 0;
        if (!archiveTimestamp(line, timestamp, start, end))
            continue;
        const bool future = timestamp > exactEpoch + ARCHIVE_FUTURE_TOLERANCE;
        if (future) {
            if (blockEnded) {
                multipleBlocks = true;
                break;
            }
            if (futureCount == 0) {
                previousAnchor = lastValid;
                firstFuture = timestamp;
            }
            lastFuture = timestamp;
            ++futureCount;
        } else if (timestamp >= VALID_ARCHIVE_TIME_FLOOR) {
            if (futureCount != 0 && nextAnchor == 0) {
                nextAnchor = timestamp;
                blockEnded = true;
            }
            lastValid = timestamp;
        }
    }
    scan.close();
    if ((futureCount == 0 && !nonCanonicalLineEnding) || multipleBlocks)
        return 0;

    uint32_t rangeStart = 0, rangeEnd = 0;
    if (futureCount != 0) {
        rangeStart = previousAnchor ? previousAnchor + 1 : 0;
        rangeEnd = nextAnchor && nextAnchor > rangeStart + 1 ? nextAnchor - 1 : exactEpoch;
        if (rangeStart == 0) {
            const uint32_t observedSpan = lastFuture >= firstFuture ? lastFuture - firstFuture : 0;
            rangeStart = rangeEnd > observedSpan ? rangeEnd - observedSpan : VALID_ARCHIVE_TIME_FLOOR;
        }
        if (rangeEnd <= rangeStart)
            return 0;
    }

    if (!copyArchiveFile(path, backupPath))
        return 0;
    FSCom.remove(temporaryPath);
    File input = FSCom.open(path, FILE_O_READ);
    File output = FSCom.open(temporaryPath, FILE_O_WRITE);
    if (!input || !output) {
        if (input)
            input.close();
        if (output)
            output.close();
        FSCom.remove(temporaryPath);
        return 0;
    }

    uint32_t corrected = 0;
    const uint64_t sourceSpan = lastFuture > firstFuture ? static_cast<uint64_t>(lastFuture - firstFuture) : 0;
    const uint64_t targetSpan = static_cast<uint64_t>(rangeEnd - rangeStart);
    while (input.available()) {
        String line = input.readStringUntil('\n');
        while (line.endsWith("\r"))
            line.remove(line.length() - 1);
        uint32_t timestamp = 0;
        int start = 0, end = 0;
        if (archiveTimestamp(line, timestamp, start, end) && timestamp > exactEpoch + ARCHIVE_FUTURE_TOLERANCE) {
            uint32_t normalized;
            if (sourceSpan == 0) {
                normalized = rangeStart + (targetSpan * corrected) / (futureCount > 1 ? futureCount - 1 : 1);
            } else {
                normalized = rangeStart + (targetSpan * static_cast<uint64_t>(timestamp - firstFuture)) / sourceSpan;
            }
            line = line.substring(0, start) + String(normalized) + line.substring(end);
            ++corrected;
        }
        output.println(line);
    }
    input.close();
    output.flush();
    output.close();
    if (corrected != futureCount) {
        FSCom.remove(temporaryPath);
        return 0;
    }
    FSCom.remove(path);
    if (!FSCom.rename(temporaryPath, path)) {
        // Keep the immutable pre-sync backup even if replacing the live file
        // fails, and restore a copy of it for normal archive reads.
        copyArchiveFile(backupPath, path);
        FSCom.remove(temporaryPath);
        return 0;
    }
    return corrected;
}
} // namespace

uint16_t getNotificationUnreadState() { return notificationUnread.load(); }
uint32_t getNotificationTransmitCount() { return notificationTransmitCount.load(); }
uint32_t getNotificationReceiveCount() { return notificationReceiveCount.load(); }
bool getPingBotEnabled()
{
    // Module setup can run before Preferences/NVS is ready on some boots.
    // Retry lazily so a persisted false value is never replaced by the default.
    if (!pingBotStateLoaded.load())
        loadNotificationUnread();
    return pingBotEnabled.load();
}

bool setPingBotEnabled(bool enabled)
{
    Preferences prefs;
    if (!prefs.begin(NOTIFICATION_PREF_NAMESPACE, false))
        return false;
    const size_t written = prefs.putBool(PING_BOT_PREF_KEY, enabled);
    prefs.end();
    if (written == 0)
        return false;
#ifdef FSCom
    SafeFile file(PING_BOT_STATE_PATH, true);
    {
        concurrency::LockGuard guard(spiLock);
        if (file.write(enabled ? 1 : 0) != 1)
            return false;
    }
    if (!file.close())
        return false;
#endif
    pingBotEnabled.store(enabled);
    pingBotStateLoaded.store(true);
    return true;
}

void clearNotificationUnread(int8_t channel)
{
    uint16_t before = 0;
    uint16_t after = 0;
    if (channel >= 0 && channel < 8) {
        const uint16_t mask = static_cast<uint16_t>((1U << channel) | (1U << (channel + 8)));
        before = notificationUnread.fetch_and(static_cast<uint16_t>(~mask));
        after = before & static_cast<uint16_t>(~mask);
    } else {
        before = notificationUnread.exchange(0);
    }
    if (before != after)
        persistNotificationUnread(after);
}

void notifyNotificationTransmit()
{
    notificationTransmitCount.fetch_add(1);
}

void notifyNotificationReceive() { notificationReceiveCount.fetch_add(1); }

uint32_t normalizeNightbotArchiveTimestamps(uint32_t exactEpoch)
{
#ifdef FSCom
    concurrency::LockGuard guard(spiLock);
    uint32_t corrected = 0;
    corrected += normalizeArchiveFile(LOG_PATH, "/static/nightbot.clock.tmp", "/static/nightbot.before-clock-sync.jsonl", exactEpoch);
    corrected += normalizeArchiveFile(OLD_LOG_PATH, "/static/nightbot.previous.clock.tmp",
                                      "/static/nightbot.previous.before-clock-sync.jsonl", exactEpoch);
    corrected += normalizeArchiveFile(SENT_LOG_PATH, "/static/nightbot.sent.clock.tmp",
                                      "/static/nightbot.sent.before-clock-sync.jsonl", exactEpoch);
    return corrected;
#else
    return 0;
#endif
}

ReplyBotModule::ReplyBotModule()
    : SinglePortModule("nightbot", meshtastic_PortNum_TEXT_MESSAGE_APP), concurrency::OSThread("NightBot", THREAD_INTERVAL_MS)
{
    isPromiscuous = true;
}

void ReplyBotModule::setup()
{
    loadState();
    loadNotificationUnread();
    notificationPixel.begin();
    notificationPixel.clear();
    notificationPixel.show();
    appendLog("boot", getValidTime(RTCQuality::RTCQualityDevice, false), nodeDB->getNodeNum(), nodeDB->getNodeNum(), 0, 0, 0,
              nullptr, 0);
}

bool ReplyBotModule::wantPacket(const meshtastic_MeshPacket *p)
{
    return p && p->which_payload_variant == meshtastic_MeshPacket_decoded_tag &&
           p->decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP;
}

bool ReplyBotModule::isActive(uint32_t epoch) const
{
    return epoch >= NIGHT_START_EPOCH && epoch < NIGHT_END_EPOCH;
}

bool ReplyBotModule::alreadyReplied(uint32_t sender) const
{
    for (uint16_t i = 0; i < state.repliedCount && i < MAX_REPLIED_SENDERS; ++i) {
        if (state.repliedSenders[i] == sender)
            return true;
    }
    return false;
}

void ReplyBotModule::rememberReply(uint32_t sender)
{
    if (alreadyReplied(sender))
        return;
    if (state.repliedCount < MAX_REPLIED_SENDERS) {
        state.repliedSenders[state.repliedCount++] = sender;
    } else {
        memmove(&state.repliedSenders[0], &state.repliedSenders[1], sizeof(state.repliedSenders) - sizeof(uint32_t));
        state.repliedSenders[MAX_REPLIED_SENDERS - 1] = sender;
    }
    saveState();
}

void ReplyBotModule::loadState()
{
    memset(&state, 0, sizeof(state));
    state.magic = STATE_MAGIC;
    state.version = STATE_VERSION;

#ifdef FSCom
    concurrency::LockGuard guard(spiLock);
    if (!FSCom.exists(STATE_PATH))
        return;
    File file = FSCom.open(STATE_PATH, FILE_O_READ);
    if (!file)
        return;
    PersistentState loaded = {};
    const size_t read = file.readBytes(reinterpret_cast<char *>(&loaded), sizeof(loaded));
    file.close();
    if (read == sizeof(loaded) && loaded.magic == STATE_MAGIC && loaded.version == STATE_VERSION &&
        loaded.repliedCount <= MAX_REPLIED_SENDERS) {
        state = loaded;
    }
#endif
}

void ReplyBotModule::saveState()
{
#ifdef FSCom
    SafeFile file(STATE_PATH, true);
    {
        concurrency::LockGuard guard(spiLock);
        file.write(reinterpret_cast<const uint8_t *>(&state), sizeof(state));
    }
    if (!file.close())
        LOG_ERROR("NightBot: unable to persist state");
#endif
}

void ReplyBotModule::appendLog(const char *event, uint32_t epoch, uint32_t from, uint32_t to, uint8_t channel, int16_t rssi,
                               float snr, const uint8_t *text, size_t textLength)
{
#ifdef FSCom
    concurrency::LockGuard guard(spiLock);
    FSCom.mkdir("/static");
    if (FSCom.exists(LOG_PATH)) {
        File current = FSCom.open(LOG_PATH, FILE_O_READ);
        const size_t size = current ? current.size() : 0;
        if (current)
            current.close();
        if (size >= MAX_LOG_BYTES) {
            if (FSCom.exists(OLD_LOG_PATH))
                FSCom.remove(OLD_LOG_PATH);
            FSCom.rename(LOG_PATH, OLD_LOG_PATH);
        }
    }

    File file = FSCom.open(LOG_PATH, FILE_APPEND);
    if (!file) {
        LOG_ERROR("NightBot: unable to append log");
        return;
    }
    file.printf("{\"ts\":%u,\"event\":\"%s\",\"from\":\"!%08x\",\"to\":", epoch, event, from);
    if (isBroadcast(to)) {
        file.print("\"^all\"");
    } else {
        file.printf("\"!%08x\"", to);
    }
    file.printf(",\"channel\":%u,\"rssi\":%d,\"snr\":%.2f,\"text\":", channel, rssi, snr);
    writeJsonString(file, text ? text : reinterpret_cast<const uint8_t *>(""), text ? textLength : 0);
    file.println("}");
    file.flush();
    file.close();
#endif
}

bool ReplyBotModule::sendText(uint32_t dest, uint8_t channel, const char *text, bool wantAck, const char *event, uint32_t epoch)
{
    meshtastic_MeshPacket *packet = allocDataPacket();
    if (!packet)
        return false;
    packet->to = dest;
    packet->channel = channel;
    packet->want_ack = wantAck;
    // Human-authored text uses HIGH priority in Meshtastic. Keep automatic
    // replies explicitly in BACKGROUND so they cannot jump ahead of a message
    // submitted by the user while the channel is busy.
    packet->priority = meshtastic_MeshPacket_Priority_BACKGROUND;
    packet->decoded.want_response = false;
    packet->decoded.dest = dest;
    packet->decoded.payload.size = strnlen(text, sizeof(packet->decoded.payload.bytes));
    memcpy(packet->decoded.payload.bytes, text, packet->decoded.payload.size);

    const ErrorCode result = service->sendToMesh(packet, RX_SRC_LOCAL, true);
    if (result != ERRNO_OK && result != ERRNO_SHOULD_RELEASE) {
        LOG_WARN("NightBot: send rejected, error=%d", result);
        return false;
    }
    appendLog(event, epoch, nodeDB->getNodeNum(), dest, channel, 0, 0, reinterpret_cast<const uint8_t *>(text), strlen(text));
    return true;
}

ProcessMessage ReplyBotModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    const uint32_t ourNode = nodeDB->getNodeNum();
    if (mp.from == 0 || mp.from == ourNode || mp.decoded.payload.size == 0)
        return ProcessMessage::CONTINUE;

    const uint32_t epoch = getValidTime(RTCQuality::RTCQualityDevice, false);
    appendLog("rx", epoch, mp.from, mp.to, mp.channel, mp.rx_rssi, mp.rx_snr, mp.decoded.payload.bytes,
              mp.decoded.payload.size);

    const bool isDirectMessage = mp.to == ourNode;
    markNotificationUnread(mp.channel, isDirectMessage);

    // Meshtastic text packets do not carry a reply/thread reference. Conservatively
    // treat the first text heard after our beacon as a response and stop the campaign.
    if (state.lastBeaconEpoch != 0 && !state.campaignStopped) {
        state.campaignStopped = 1;
        saveState();
        LOG_INFO("NightBot: response heard; public campaign stopped");
    }

    // The community Ping channel is a public connectivity test. Reply only to
    // an exact broadcast "Ping" on that named channel. The strict match and
    // rate limits prevent bot-to-bot loops and airtime floods.
    const bool isPingChannel = isBroadcast(mp.to) && strcasecmp(channels.getName(mp.channel), PING_CHANNEL_NAME) == 0;
    if (getPingBotEnabled() && isPingChannel && isExactPing(mp.decoded.payload.bytes, mp.decoded.payload.size) &&
        config.lora.tx_enabled &&
        !pingRateLimited(mp.from)) {
        char reply[128];
        char hopsText[8];
        if (mp.hop_start > 0 && mp.hop_start >= mp.hop_limit) {
            const uint8_t hops = mp.hop_start - mp.hop_limit;
            snprintf(hopsText, sizeof(hopsText), "%u", hops);
        } else {
            strlcpy(hopsText, "?", sizeof(hopsText));
        }
        if (mp.has_rx_rssi) {
            snprintf(reply, sizeof(reply), "🛜 Pong![!%08x] %s🐰 · %s · RSSI %d · SNR %.1f (barbieNode)", mp.from,
                     hopsText, PING_LOCATION, mp.rx_rssi, mp.rx_snr);
        } else {
            snprintf(reply, sizeof(reply), "🛜 Pong![!%08x] %s🐰 · %s · RSSI ? · SNR ? (barbieNode)", mp.from,
                     hopsText, PING_LOCATION);
        }
        sendText(NODENUM_BROADCAST, mp.channel, reply, false, "pong", epoch);
    }

    if (isDirectMessage && isActive(epoch) && !alreadyReplied(mp.from) && config.lora.tx_enabled) {
        if (sendText(mp.from, mp.channel, AWAY_TEXT, true, "autoreply", epoch))
            rememberReply(mp.from);
    }
    return ProcessMessage::CONTINUE;
}

void ReplyBotModule::showHeartbeat()
{
    const uint16_t unread = getNotificationUnreadState();
    const bool general = (unread & 0x00ffU) != 0;
    const bool direct = (unread & 0xff00U) != 0;
    uint32_t color;
    if (general && direct)
        color = notificationPixel.Color(8, 8, 8);
    else if (direct)
        color = notificationPixel.Color(12, 0, 0);
    else if (general)
        color = notificationPixel.Color(0, 0, 12);
    else
        color = notificationPixel.Color(0, 12, 0);
    notificationPixel.setPixelColor(0, color);
    notificationPixel.show();
    heartbeatLit = true;
}

void ReplyBotModule::hideHeartbeat()
{
    notificationPixel.clear();
    notificationPixel.show();
    heartbeatLit = false;
}

void ReplyBotModule::showPortableRainbow()
{
    const uint32_t color = notificationPixel.gamma32(
        notificationPixel.ColorHSV(rainbowHue, 255, PORTABLE_RAINBOW_VALUE));
    notificationPixel.setPixelColor(0, color);
    notificationPixel.show();
    rainbowHue += PORTABLE_RAINBOW_HUE_STEP;
    rainbowActive = true;
    heartbeatLit = false;
}

int32_t ReplyBotModule::runOnce()
{
    if (isDualBootPortableAPUnattended()) {
        showPortableRainbow();
        return PORTABLE_RAINBOW_STEP_MS;
    }
    if (rainbowActive) {
        rainbowActive = false;
        notificationPixel.clear();
        notificationPixel.show();
    }
    if (heartbeatLit) {
        hideHeartbeat();
        return HEARTBEAT_REST_MS;
    }

    showHeartbeat();
    const uint32_t epoch = getValidTime(RTCQuality::RTCQualityNTP, false);
    if (ENABLE_SCHEDULED_BEACON && epoch && isActive(epoch) && !state.campaignStopped && config.lora.tx_enabled &&
        (state.lastBeaconEpoch == 0 || epoch - state.lastBeaconEpoch >= BEACON_INTERVAL_SECONDS)) {
        if (sendText(NODENUM_BROADCAST, channels.getPrimaryIndex(), BEACON_TEXT, false, "beacon", epoch)) {
            state.lastBeaconEpoch = epoch;
            saveState();
        }
    }
    return HEARTBEAT_PULSE_MS;
}

#endif
