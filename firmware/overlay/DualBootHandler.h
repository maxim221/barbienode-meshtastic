#pragma once

namespace httpsserver {
class HTTPRequest;
class HTTPResponse;
}

void handleDualBootStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootRNode(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootMeshCore(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootPortableAP(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootHomeWiFi(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootUpdateRNode(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootUpdateMeshCore(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handlePartitionTableBackup(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleTripleBootMigration(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleNotificationStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleNotificationRead(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handlePingBotStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handlePingBotToggle(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleClockStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleClockSync(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleTxPowerStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleTxPowerSet(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleListenBeforeTalkStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleListenBeforeTalkSet(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
bool getListenBeforeTalkEnabled();
bool setListenBeforeTalkEnabled(bool enabled);
bool startDualBootPortableAP(bool automaticFallback = false);
bool isDualBootPortableAPActive();
bool isDualBootPortableAPUnattended();
