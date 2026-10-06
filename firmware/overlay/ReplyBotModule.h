#pragma once

#include "configuration.h"
#if !MESHTASTIC_EXCLUDE_REPLYBOT

#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "mesh/generated/meshtastic/mesh.pb.h"

// Low byte: broadcast-message channel bits. High byte: direct-message channel bits.
uint16_t getNotificationUnreadState();
uint32_t getNotificationTransmitCount();
uint32_t getNotificationReceiveCount();
bool getPingBotEnabled();
bool setPingBotEnabled(bool enabled);
void clearNotificationUnread(int8_t channel);
void notifyNotificationTransmit();
void notifyNotificationReceive();
uint32_t normalizeNightbotArchiveTimestamps(uint32_t exactEpoch);

class ReplyBotModule : public SinglePortModule, private concurrency::OSThread
{
  public:
    ReplyBotModule();
    void setup() override;
    bool wantPacket(const meshtastic_MeshPacket *p) override;
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
  protected:
    int32_t runOnce() override;

  private:
    static constexpr uint8_t MAX_REPLIED_SENDERS = 32;

    struct PersistentState {
        uint32_t magic;
        uint16_t version;
        uint16_t repliedCount;
        uint32_t lastBeaconEpoch;
        uint8_t campaignStopped;
        uint8_t reserved[3];
        uint32_t repliedSenders[MAX_REPLIED_SENDERS];
    };

    PersistentState state = {};
    bool heartbeatLit = false;
    bool rainbowActive = false;
    uint16_t rainbowHue = 0;

    bool isActive(uint32_t epoch) const;
    bool alreadyReplied(uint32_t sender) const;
    void rememberReply(uint32_t sender);
    void loadState();
    void saveState();
    void appendLog(const char *event, uint32_t epoch, uint32_t from, uint32_t to, uint8_t channel, int16_t rssi, float snr,
                   const uint8_t *text, size_t textLength);
    bool sendText(uint32_t dest, uint8_t channel, const char *text, bool wantAck, const char *event, uint32_t epoch);
    void showHeartbeat();
    void hideHeartbeat();
    void showPortableRainbow();
};

#endif
