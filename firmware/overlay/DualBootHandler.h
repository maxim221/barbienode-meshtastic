#pragma once

namespace httpsserver {
class HTTPRequest;
class HTTPResponse;
}

void handleDualBootStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootRNode(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootPortableAP(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootHomeWiFi(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleDualBootUpdateRNode(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleNotificationStatus(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
void handleNotificationRead(httpsserver::HTTPRequest *req, httpsserver::HTTPResponse *res);
bool startDualBootPortableAP(bool automaticFallback = false);
bool isDualBootPortableAPActive();
