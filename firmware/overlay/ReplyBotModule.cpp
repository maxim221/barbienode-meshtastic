#include "configuration.h"
#if !MESHTASTIC_EXCLUDE_REPLYBOT

#include "ReplyBotModule.h"

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
// Deliberately off in the public source. Enabling periodic RF transmissions
// requires an explicit local decision and coordination with the radio community.
constexpr bool ENABLE_SCHEDULED_BEACON = false;
constexpr uint32_t THREAD_INTERVAL_MS = 5000;
constexpr uint32_t HEARTBEAT_PULSE_MS = 250;
constexpr uint32_t HEARTBEAT_REST_MS = 20 * 1000 - HEARTBEAT_PULSE_MS;
constexpr size_t MAX_LOG_BYTES = 256 * 1024;

constexpr const char *STATE_PATH = "/nightbot.state";
constexpr const char *LOG_PATH = "/static/nightbot.jsonl";
constexpr const char *OLD_LOG_PATH = "/static/nightbot.previous.jsonl";
constexpr const char *BEACON_TEXT = "Привет! Спишь?";
constexpr const char *AWAY_TEXT = "Сейчас я не у компьютера. Постараюсь связаться с вами, когда вернусь";
constexpr const char *PING_CHANNEL_NAME = "Ping";
constexpr const char *PING_LOCATION = "local node";
constexpr uint32_t PING_GLOBAL_COOLDOWN_MS = 30 * 1000;
constexpr uint32_t PING_SENDER_COOLDOWN_MS = 5 * 60 * 1000;
constexpr uint8_t PING_COOLDOWN_SLOTS = 16;
constexpr const char *NOTIFICATION_PREF_NAMESPACE = "BarbieNotify";
constexpr const char *NOTIFICATION_PREF_KEY = "unread";
constexpr uint8_t NOTIFICATION_LED_PIN = 48;

std::atomic<uint16_t> notificationUnread{0};
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
    if (!prefs.begin(NOTIFICATION_PREF_NAMESPACE, true))
        return;
    notificationUnread.store(prefs.getUShort(NOTIFICATION_PREF_KEY, 0));
    prefs.end();
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
} // namespace

uint16_t getNotificationUnreadState() { return notificationUnread.load(); }

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
    if (isPingChannel && isExactPing(mp.decoded.payload.bytes, mp.decoded.payload.size) && config.lora.tx_enabled &&
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

int32_t ReplyBotModule::runOnce()
{
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
