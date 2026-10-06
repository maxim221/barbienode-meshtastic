#!/usr/bin/env python3
"""Serve the Meshtastic single-page application without external dependencies."""

from __future__ import annotations

import json
import math
import os
import statistics
import threading
import time
import uuid
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlsplit
from urllib.request import Request, urlopen

try:
    import paho.mqtt.client as mqtt
except ImportError:  # The UI reports this clearly until the deployment dependency is installed.
    mqtt = None


WEB_ROOT = Path(os.environ.get("WEB_ROOT", "/opt/meshtastic-web")).resolve()
BIND_HOST = os.environ.get("BIND_HOST", "192.168.1.19")
PORT = int(os.environ.get("PORT", "8080"))
DEVICE_URL = os.environ.get("DEVICE_URL", "http://192.168.1.31").rstrip("/")
NODE_CACHE_PATH = Path(os.environ.get("NODE_CACHE_PATH", "/var/lib/barbienode-web/node-cache.json"))
NODE_CACHE_LOCK = threading.Lock()
MAX_NODE_CACHE_BYTES = 4 * 1024 * 1024
PROJECT_OWNER_LONG_NAME = "BarbieNode 💅"
PROJECT_OWNER_SHORT_NAME = "db8c"
AIM_MEASUREMENTS_PATH = Path(os.environ.get("AIM_MEASUREMENTS_PATH", "/var/lib/barbienode-web/aim-measurements.json"))
AIM_MEASUREMENTS_LOCK = threading.Lock()
MAX_AIM_MEASUREMENTS_BYTES = 1024 * 1024
LINK_QUALITY_PATH = Path(os.environ.get("LINK_QUALITY_PATH", "/var/lib/barbienode-link-quality/events.jsonl"))
DELIVERY_STATUS_PATH = Path(os.environ.get("DELIVERY_STATUS_PATH", "/var/lib/barbienode-link-quality/deliveries.jsonl"))
LINK_QUALITY_WINDOW_SECONDS = 24 * 60 * 60
LINK_QUALITY_BIN_SECONDS = 15 * 60
OWN_LOCATION_PATH = Path(os.environ.get("OWN_LOCATION_PATH", "/var/lib/barbienode-web/own-location.json"))
PING_SCHEDULE_PATH = Path(os.environ.get("PING_SCHEDULE_PATH", "/var/lib/barbienode-web/ping-schedule.json"))
PING_PROGRESS_PATH = Path(os.environ.get("PING_PROGRESS_PATH", "/var/lib/barbienode-link-quality/ping-progress.json"))
LOCAL_STATE_LOCK = threading.Lock()
MQTT_CONFIG_PATH = Path(os.environ.get("MQTT_CONFIG_PATH", "/var/lib/barbienode-web/mqtt-config.json"))
MQTT_MESSAGES_PATH = Path(os.environ.get("MQTT_MESSAGES_PATH", "/var/lib/barbienode-web/mqtt-messages.jsonl"))
SERVER_SENT_MESSAGES_PATH = Path(os.environ.get("SERVER_SENT_MESSAGES_PATH", "/var/lib/barbienode-web/server-sent-messages.jsonl"))
SERVER_SENT_MESSAGES_LOCK = threading.Lock()
MAX_SERVER_SENT_MESSAGES_BYTES = 1024 * 1024
RADIO_PROFILE_MEMORY_PATH = Path(os.environ.get("RADIO_PROFILE_MEMORY_PATH", "/var/lib/barbienode-web/radio-profile-memory.json"))
LORA_SEND_URL = os.environ.get("LORA_SEND_URL", "http://127.0.0.1:8765/send")
MESHCORE_BRIDGE_URL = os.environ.get("MESHCORE_BRIDGE_URL", "http://127.0.0.1:8766").rstrip("/")
RETICULUM_BRIDGE_URL = os.environ.get("RETICULUM_BRIDGE_URL", "http://127.0.0.1:8767").rstrip("/")
RNODE_MODE_CONTROL_URL = os.environ.get("RNODE_MODE_CONTROL_URL", "http://127.0.0.1:8768").rstrip("/")
PROXY_PREFIXES = (
    "/api/",
    "/json/",
    "/dualboot/",
    "/nightbot",
    "/notifications/",
    "/pingbot/",
    "/clock/",
    "/radio/",
    "/restart",
    "/upload",
)

CLOCK_SYNC_INTERVAL_SECONDS = int(os.environ.get("CLOCK_SYNC_INTERVAL_SECONDS", "900"))
CLOCK_SYNC_RETRY_SECONDS = int(os.environ.get("CLOCK_SYNC_RETRY_SECONDS", "30"))
ONEMESH_MESSAGES_URL = os.environ.get("ONEMESH_MESSAGES_URL", "https://map.onemesh.ru/api/v1/text-messages")
ONEMESH_ROOT_TOPIC = os.environ.get("ONEMESH_ROOT_TOPIC", "msh/RU/MSK")
ONEMESH_CACHE_SECONDS = max(5, int(os.environ.get("ONEMESH_CACHE_SECONDS", "10")))
ONEMESH_MESSAGE_COUNT = min(500, max(1, int(os.environ.get("ONEMESH_MESSAGE_COUNT", "200"))))


def uint32(value: object) -> int:
    try:
        return int(value or 0) & 0xFFFFFFFF
    except (TypeError, ValueError, OverflowError):
        return 0


class OneMeshPublicChat:
    """Read-only view of the public chat already decoded by the OneMesh map."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.cached_messages: list[dict] = []
        self.last_attempt = 0.0
        self.last_success = 0
        self.last_error = ""

    @staticmethod
    def _timestamp(value: object) -> int:
        try:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
        except (TypeError, ValueError, OverflowError):
            return int(time.time())

    @classmethod
    def _normalize(cls, payload: object) -> list[dict]:
        rows = payload.get("text_messages", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            raise ValueError("OneMesh response does not contain text_messages")
        output = []
        for source in rows:
            if not isinstance(source, dict):
                continue
            text = str(source.get("text", "")).strip()
            if not text:
                continue
            try:
                node_num = int(source.get("from", 0)) & 0xFFFFFFFF
            except (TypeError, ValueError):
                node_num = 0
            sender_id = f"!{node_num:08x}" if node_num else ""
            channel = str(source.get("channel_id", "публичный канал"))[:80]
            identifier = str(source.get("id", "")).strip() or uuid.uuid4().hex
            output.append({
                "id": f"onemesh-{identifier}",
                "ts": cls._timestamp(source.get("created_at")),
                "direction": "rx",
                "sender": sender_id or "OneMesh",
                "senderId": sender_id,
                "text": text[:500],
                "topic": f"OneMesh · Москва · {channel}",
                "source": "onemesh-api",
            })
        return output

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self.lock:
            if now - self.last_attempt < ONEMESH_CACHE_SECONDS:
                return self._result()
            self.last_attempt = now
            try:
                query = urlencode({"root_topic": ONEMESH_ROOT_TOPIC, "order": "desc", "count": ONEMESH_MESSAGE_COUNT})
                request = Request(
                    f"{ONEMESH_MESSAGES_URL}?{query}",
                    headers={"Accept": "application/json", "User-Agent": "BarbieNode/OneMesh-read-only"},
                )
                with urlopen(request, timeout=8) as response:
                    payload = json.loads(response.read(2 * 1024 * 1024))
                self.cached_messages = self._normalize(payload)
                self.last_success = int(time.time())
                self.last_error = ""
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
                self.last_error = str(error)[:300]
            return self._result()

    def _result(self) -> dict:
        return {
            "status": {
                "available": bool(self.last_success),
                "rootTopic": ONEMESH_ROOT_TOPIC,
                "updatedAt": self.last_success,
                "stale": bool(self.last_error and self.cached_messages),
                "error": self.last_error,
            },
            "messages": list(self.cached_messages),
        }


class SeparateMqttChat:
    """A deliberately separate MQTT text stream that never invokes the LoRa device."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.client = None
        self.connected = False
        self.last_error = ""
        self.config = self._read_config()
        if self.config.get("enabled"):
            threading.Thread(target=self.connect, name="separate-mqtt-start", daemon=True).start()

    @staticmethod
    def _read_config() -> dict:
        try:
            value = json.loads(MQTT_CONFIG_PATH.read_text())
            return value if isinstance(value, dict) else {"enabled": False}
        except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
            return {"enabled": False}

    def _save_config(self) -> None:
        MQTT_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = MQTT_CONFIG_PATH.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.config, ensure_ascii=False, separators=(",", ":")) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, MQTT_CONFIG_PATH)

    @staticmethod
    def validate_topic(topic: str) -> str:
        topic = topic.strip().strip("/")
        if not topic or len(topic) > 240 or "+" in topic or "#" in topic:
            raise ValueError("use an exact topic without wildcards")
        return topic

    def public_status(self) -> dict:
        with self.lock:
            return {
                "available": mqtt is not None,
                "enabled": bool(self.config.get("enabled")),
                "connected": self.connected,
                "host": self.config.get("host", ""),
                "port": self.config.get("port", 8883),
                "topic": self.config.get("topic", "msh/RU/MSK/2/json/MediumFast"),
                "username": self.config.get("username", ""),
                "profile": self.config.get("profile", "manual"),
                "hasPassword": bool(self.config.get("password")),
                "tls": bool(self.config.get("tls", True)),
                "error": self.last_error,
            }

    def messages(self, limit: int = 300) -> list[dict]:
        try:
            lines = MQTT_MESSAGES_PATH.read_text().splitlines()[-limit:]
        except (FileNotFoundError, PermissionError, OSError):
            return []
        output = []
        for line in lines:
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    output.append(row)
            except (ValueError, json.JSONDecodeError):
                continue
        return output

    def _append(self, row: dict) -> None:
        MQTT_MESSAGES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with self.lock, MQTT_MESSAGES_PATH.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    def disconnect(self, persist: bool = True) -> None:
        with self.lock:
            client, self.client = self.client, None
            self.connected = False
            if persist:
                self.config["enabled"] = False
                self._save_config()
        if client is not None:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:
                pass

    def save(self, source: dict, enabled: bool | None = None) -> None:
        host = str(source.get("host", "")).strip()
        if not host or len(host) > 253 or "://" in host or any(char.isspace() for char in host):
            raise ValueError("invalid broker host")
        port = int(source.get("port", 8883))
        if not 1 <= port <= 65535:
            raise ValueError("invalid broker port")
        topic = self.validate_topic(str(source.get("topic", "")))
        username = str(source.get("username", ""))[:256]
        password = str(source.get("password", ""))
        if len(password) > 1024:
            raise ValueError("password is too long")
        with self.lock:
            previous_password = str(self.config.get("password", ""))
            self.config = {
                "enabled": bool(self.config.get("enabled")) if enabled is None else enabled,
                "profile": str(source.get("profile", "manual"))[:64],
                "host": host, "port": port, "topic": topic,
                "username": username, "password": password or previous_password,
                "tls": bool(source.get("tls", True)), "savedAt": int(time.time()),
            }
            self._save_config()

    def configure(self, source: dict) -> None:
        self.save(source, enabled=True)
        self.connect()

    def connect(self) -> None:
        if mqtt is None:
            with self.lock:
                self.last_error = "На Orange Pi не установлен модуль paho-mqtt."
            return
        self.disconnect(persist=False)
        with self.lock:
            config = dict(self.config)
        if not config.get("enabled"):
            return
        try:
            try:
                client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"barbienode-web-{uuid.uuid4().hex[:10]}")
            except (AttributeError, TypeError):
                client = mqtt.Client(client_id=f"barbienode-web-{uuid.uuid4().hex[:10]}")
            if config.get("username"):
                client.username_pw_set(str(config["username"]), str(config.get("password", "")))
            if config.get("tls"):
                client.tls_set()

            def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
                code = int(getattr(reason_code, "value", reason_code))
                with self.lock:
                    self.connected = code == 0
                    self.last_error = "" if code == 0 else f"Брокер отклонил подключение: {reason_code}"
                if code == 0:
                    topic = str(config["topic"])
                    _client.subscribe(topic + "/#" if "/2/json/" in topic.lower() else topic, qos=1)

            def on_disconnect(_client, _userdata, *args):
                with self.lock:
                    self.connected = False
                    if self.config.get("enabled") and not self.last_error:
                        self.last_error = "Соединение с брокером потеряно; выполняется переподключение."

            def on_message(_client, _userdata, message):
                try:
                    decoded = json.loads(message.payload.decode("utf-8"))
                    if not isinstance(decoded, dict):
                        raise ValueError("not an object")
                    if str(decoded.get("type", "")).lower() == "sendtext":
                        return
                    text = str(decoded.get("text", decoded.get("payload", ""))).strip()
                    if not text:
                        return
                    identifier = str(decoded.get("id", "")) or uuid.uuid4().hex
                    if any(str(row.get("id")) == identifier for row in self.messages(30)):
                        return
                    row = {
                        "id": identifier, "ts": int(decoded.get("ts", time.time())), "direction": "rx",
                        "sender": str(decoded.get("sender", decoded.get("from", "MQTT")))[:160], "senderId": str(decoded.get("senderId", decoded.get("from", "")))[:80],
                        "text": text[:500], "topic": str(message.topic), "source": "broker",
                    }
                    self._append(row)
                except (UnicodeDecodeError, ValueError, TypeError, json.JSONDecodeError):
                    return

            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            with self.lock:
                self.client = client
                self.last_error = ""
            client.connect_async(str(config["host"]), int(config["port"]), keepalive=60)
            client.loop_start()
        except Exception as error:
            with self.lock:
                self.last_error = f"Ошибка MQTT: {error}"
                self.connected = False

    def publish(self, text: str, sender: str, sender_id: str) -> dict:
        text = text.strip()
        if not text or len(text) > 500:
            raise ValueError("message must contain 1–500 characters")
        with self.lock:
            client, topic, connected = self.client, str(self.config.get("topic", "")), self.connected
        if not client or not connected:
            raise RuntimeError("MQTT is not connected")
        row = {
            "id": uuid.uuid4().hex, "ts": int(time.time()), "direction": "tx",
            "sender": sender[:160] or "BarbieNode", "senderId": sender_id[:80], "text": text,
            "topic": topic, "source": "broker",
        }
        if "/2/json/" in topic.lower():
            raw_id = sender_id.removeprefix("!")
            try:
                from_node = int(raw_id, 16) if raw_id else 0
            except ValueError:
                from_node = 0
            if not from_node:
                raise ValueError("Meshtastic JSON publish requires the local node ID")
            payload = {"from": from_node, "type": "sendtext", "payload": text}
        else:
            payload = {"protocol": "barbienode-chat-v1", "id": row["id"], "ts": row["ts"], "sender": row["sender"], "senderId": row["senderId"], "text": text}
        result = client.publish(topic, json.dumps(payload, ensure_ascii=False, separators=(",", ":")), qos=1, retain=False)
        if int(result.rc) != 0:
            raise RuntimeError(f"publish failed with code {result.rc}")
        self._append(row)
        return row


MQTT_CHAT = SeparateMqttChat()
ONEMESH_CHAT = OneMeshPublicChat()


class SPAHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, directory=str(WEB_ROOT), **kwargs)

    def send_head(self):  # type: ignore[no-untyped-def]
        request_path = unquote(urlsplit(self.path).path)
        candidate = (WEB_ROOT / request_path.lstrip("/")).resolve()
        try:
            candidate.relative_to(WEB_ROOT)
        except ValueError:
            self.send_error(404)
            return None

        if not candidate.exists() and "." not in Path(request_path).name:
            self.path = "/index.html"
        return super().send_head()

    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _read_json_file(path: Path, fallback: object) -> object:
        try:
            return json.loads(path.read_text())
        except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
            return fallback

    @staticmethod
    def _write_json_file(path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)

    def _is_device_request(self) -> bool:
        return urlsplit(self.path).path.startswith(PROXY_PREFIXES)

    @staticmethod
    def _device_is_meshcore() -> bool:
        try:
            with urlopen(f"{DEVICE_URL}/status", timeout=2) as response:
                if "application/json" not in response.headers.get("Content-Type", ""):
                    return False
                payload = json.loads(response.read(4096))
                return isinstance(payload, dict) and payload.get("mode") == "meshcore"
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
            return False

    @staticmethod
    def _device_is_rnode() -> bool:
        try:
            with urlopen(f"{DEVICE_URL}/api/status", timeout=2) as response:
                if "application/json" not in response.headers.get("Content-Type", ""):
                    return False
                payload = json.loads(response.read(4096))
                return isinstance(payload, dict) and payload.get("mode") == "rnode"
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
            return False

    @staticmethod
    def _device_is_meshtastic() -> bool:
        try:
            with urlopen(f"{DEVICE_URL}/dualboot/status", timeout=3) as response:
                payload = json.loads(response.read(4096))
                return isinstance(payload, dict) and payload.get("mode") == "meshtastic"
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
            return False

    def _send_meshcore_return_page(self, message: str = "") -> None:
        notice = f'<p class="notice">{message}</p>' if message else ""
        body = """<!doctype html><html lang=ru><meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1'>
<title>BarbieNode · MeshCore Messenger</title><style>
:root{--bg:#07111f;--panel:#0d1a2b;--panel2:#12233a;--line:#253b57;--text:#eef5ff;--muted:#92a6bf;--green:#31d08b;--blue:#64a8ff;--red:#ff6b78;--app-height:100dvh}
*{box-sizing:border-box}html,body{height:100%;min-height:100%;overflow:hidden}body{display:flex;flex-direction:column;height:var(--app-height);margin:0;background:var(--bg);color:var(--text);font:15px system-ui,-apple-system,sans-serif}
header{display:flex;gap:12px;align-items:center;padding:14px 18px;border-bottom:1px solid var(--line);position:sticky;top:0;background:#07111ff2;z-index:3}
header strong{font-size:19px}header small{display:block;color:var(--muted)}.pill{margin-left:auto;border:1px solid var(--line);padding:6px 10px;border-radius:999px;color:var(--muted);white-space:nowrap}
.pill.ok{color:var(--green);border-color:#267a59}.tabs{display:flex;gap:4px;padding:8px 14px;border-bottom:1px solid var(--line);background:#091625;position:sticky;top:61px;z-index:3}.tab{background:transparent;color:var(--muted);border:1px solid transparent}.tab.active{background:var(--panel2);color:var(--text);border-color:var(--line)}.tab .badge{margin-left:6px}
.panel{display:none;min-height:0}.panel.active{display:block;flex:1;overflow:auto}#chatPanel.active{display:grid;grid-template-columns:280px minmax(0,1fr);height:auto;min-height:0;overflow:hidden}.settings{max-width:920px;margin:0 auto;padding:22px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.settings .wide{grid-column:1/-1}
aside{padding:14px;border-right:1px solid var(--line);min-width:0}aside.left{min-height:0;overflow:auto}h2{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin:17px 3px 7px}
.list-switch{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:5px}.list-switch button{background:transparent;color:var(--muted);border:1px solid var(--line);padding:7px}.list-switch button.active{background:var(--panel2);color:var(--text)}.mark-all-read{width:100%;margin-bottom:10px;padding:6px!important;background:transparent!important;color:var(--muted)!important;border:1px solid var(--line)!important;font-size:12px!important}.list-pane[hidden]{display:none}.list-pane-head{display:flex;align-items:center;justify-content:space-between;gap:8px}.list-pane-head h2{margin:5px 3px}.list-pane-head button{padding:5px 8px;font-size:12px}.channel-form{display:grid;gap:7px;margin:8px 0 12px;padding:9px;border:1px solid var(--line);border-radius:10px;background:#091625}.channel-form[hidden]{display:none}.channel-form label{display:grid;gap:3px;color:var(--muted);font-size:12px}.channel-form input,.channel-form select{width:100%}.channel-form .row{margin:0}.channel-secret-note{font-size:11px;color:var(--muted)}.search{width:100%;margin:5px 0}.channel,.contact{display:block;width:100%;text-align:left;padding:10px;margin:4px 0;border:1px solid transparent;border-radius:11px;background:transparent;color:var(--text)}.channel:hover,.channel.active,.contact:hover,.contact.active{background:var(--panel2);border-color:var(--line)}
.nav-title{display:flex;align-items:center;gap:7px}.nav-title span:first-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.nav-title time{margin-left:auto;color:var(--muted);font-size:11px}.preview{display:block;color:var(--muted);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:3px}.hash{font:11px ui-monospace,monospace;color:#7891ae}.badge{display:inline-flex;min-width:20px;height:20px;padding:0 6px;border-radius:999px;align-items:center;justify-content:center;background:var(--red);color:#fff;font-size:11px;font-weight:800}.nav-title .badge{margin-left:auto}.nav-title .badge+time{margin-left:0}
.chat{display:flex;flex-direction:column;min-width:0;min-height:0;overflow:hidden}.chat-head{flex:none;padding:12px 18px;border-bottom:1px solid var(--line);background:#091625}.chat-head strong{display:block;font-size:17px}.chat-head small{color:var(--muted)}#messages{flex:1;min-height:0;padding:18px;display:flex;flex-direction:column;gap:10px;overflow:auto;overscroll-behavior:contain}
.msg{max-width:78%;background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:10px 12px}.msg.tx{align-self:flex-end;background:#123329;border-color:#24664f}.msg .meta{display:flex;gap:8px;color:var(--muted);font-size:12px;margin-bottom:5px;flex-wrap:wrap}.msg .meta .scope{color:#ffe28a;text-transform:lowercase}.msg .text{white-space:pre-wrap;overflow-wrap:anywhere}.msg-actions{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:6px}.msg-buttons{display:flex;align-items:center;gap:4px;flex-wrap:wrap}.delivery{font-size:11px;color:var(--muted);text-align:right}.delivery.delivered{color:var(--green)}.reply-button,.repeat-button,.details-button{padding:3px 7px!important;border:0!important;background:transparent!important;color:var(--blue)!important;font-size:11px;font-weight:600!important}
form.send{display:grid;flex:none;grid-template-columns:minmax(0,1fr) auto auto auto;gap:8px;padding:10px 14px calc(6px + env(safe-area-inset-bottom));border-top:1px solid var(--line);background:var(--bg)}.reply-preview{grid-column:1/-1;display:flex;align-items:center;gap:8px;min-width:0;padding:7px 9px;border-left:3px solid var(--blue);border-radius:7px;background:var(--panel2);color:var(--muted);font-size:12px}.reply-preview[hidden]{display:none}.reply-preview>div{display:grid;min-width:0;flex:1}.reply-preview b{color:var(--blue)}.reply-preview span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.reply-preview button{padding:2px 7px;background:transparent;color:var(--text);font-size:18px}.meshpic-link{display:inline-flex;align-items:center;justify-content:center;min-width:78px;padding:7px 9px;border:1px solid var(--line);border-radius:9px;background:var(--panel2);color:var(--text);font-weight:700;text-decoration:none;white-space:nowrap}.meshpic-link:hover,.meshpic-link:focus-visible{border-color:var(--blue);background:#173253;outline:none}.emoji-toggle{min-width:42px;padding:7px 9px!important;background:var(--panel2)!important;color:var(--text)!important;border:1px solid var(--line)!important;font-size:19px!important}.emoji-toggle[aria-expanded=true]{border-color:var(--blue)!important;background:#173253!important}.emoji-panel{grid-column:1/-1;display:grid;grid-template-columns:repeat(auto-fill,minmax(38px,1fr));gap:4px;max-height:168px;overflow:auto;padding:7px;border:1px solid var(--line);border-radius:10px;background:var(--panel);box-shadow:0 -8px 24px #0005}.emoji-panel[hidden]{display:none}.emoji-panel button{padding:5px!important;min-height:36px;background:transparent!important;color:var(--text)!important;border:1px solid transparent!important;font-size:21px!important}.emoji-panel button:hover,.emoji-panel button:focus-visible{background:var(--panel2)!important;border-color:var(--line)!important}.compose-meta{grid-column:1/-1;display:flex;justify-content:space-between;gap:8px;color:var(--muted);font-size:12px}.compose-meta .over{color:var(--red)}textarea,input,select{background:#081524;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:9px}textarea{width:100%;resize:none}button{font:inherit;border:0;border-radius:9px;padding:9px 12px;background:var(--green);color:#052116;font-weight:700;cursor:pointer}button.secondary{background:var(--panel2);color:var(--text);border:1px solid var(--line)}button.danger{background:#51232c;color:#fff}button:disabled{opacity:.45;cursor:not-allowed}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px}.card label{display:block;margin:8px 0}.card input,.card select{width:100%}.row{display:flex;gap:7px;flex-wrap:wrap}.muted{color:var(--muted)}.notice{margin:10px 18px;color:var(--green)}#feedback{min-height:24px;padding:8px 18px;color:var(--muted);border-bottom:1px solid var(--line)}code{color:#9ed2ff}dialog{width:min(620px,calc(100% - 24px));max-height:85dvh;overflow:auto;background:var(--panel);color:var(--text);border:1px solid var(--line);border-radius:14px;padding:16px}dialog::backdrop{background:#000a}.dialog-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:12px}.dialog-head button{padding:3px 9px;background:transparent;color:var(--text);font-size:22px}.message-details{display:grid;grid-template-columns:minmax(120px,.7fr) minmax(0,1.3fr);gap:7px 12px;margin:0}.message-details dt{color:var(--muted)}.message-details dd{margin:0;overflow-wrap:anywhere;font-family:ui-monospace,monospace}.message-details .wide{grid-column:1/-1;font-family:inherit;white-space:pre-wrap}
@media(max-width:760px){header{position:relative;flex:none;padding:10px 13px}header small{display:none}.tabs{position:relative;top:auto;flex:none;overflow:auto;padding:6px 9px}.tab{white-space:nowrap}#feedback{flex:none;min-height:0;padding:5px 13px}#chatPanel.active{display:flex;flex-direction:column}aside.left{flex:0 1 26%;max-height:210px;padding:9px 12px;border:0;border-bottom:1px solid var(--line)}aside.left h2{margin:10px 3px 5px}.channel,.contact{padding:7px 9px}.chat{flex:1;min-height:0}.chat-head{position:relative;top:auto;z-index:2;padding:8px 13px}#messages{height:auto;padding:11px}.msg{max-width:92%}form.send{padding:8px 10px calc(5px + env(safe-area-inset-bottom))}.compose-meta span:first-child{display:none}.settings{display:block;padding:14px}.settings .card{margin-bottom:12px}}
</style><header><div><strong>BarbieNode · MeshCore</strong><small>Orange Pi ↔ локальная сеть ↔ нода ↔ LoRa</small></div><span id=status class=pill>подключение…</span></header>__NOTICE__
<nav class=tabs><button class="tab active" data-tab=chatPanel data-list=channels>Каналы <span id=channelUnread></span></button><button class=tab data-tab=chatPanel data-list=contacts>Личные <span id=directUnread></span></button><button class=tab data-tab=radioPanel>Радио и позиция</button><button class=tab data-tab=systemPanel>Система</button></nav><div id=feedback></div>
<main id=chatPanel class="panel active"><aside class=left><div class=list-switch><button id=showChannels type=button class=active>Каналы</button><button id=showContacts type=button>Личные <span id=contactCount></span></button></div><button id=markAllRead type=button class=mark-all-read>✓ Прочитать всё</button><div id=channelPane class=list-pane><div class=list-pane-head><h2>Каналы</h2><button id=showChannelForm type=button class=secondary>+ Добавить</button></div><form id=channelForm class=channel-form hidden><label>Слот<select id=channelSlot></select></label><label>Имя<input id=channelName maxlength=32 placeholder='#район' required></label><label>Ключ, 16 байт HEX<input id=channelSecret type=password inputmode=text autocomplete=off placeholder='для приватного канала'></label><span class=channel-secret-note>Для публичного канала с именем, начинающимся на #, ключ вычисляется нодой. Приватный ключ после сохранения не показывается.</span><div class=row><button id=saveChannel type=submit>Сохранить</button><button id=removeChannel type=button class=danger>Удалить слот</button><button id=cancelChannel type=button class=secondary>Отмена</button></div></form><div id=channels><span class=muted>загрузка…</span></div></div><div id=contactPane class=list-pane hidden><input id=search class=search type=search placeholder='Поиск личных сообщений…' autocomplete=off><h2>Личные сообщения</h2><div id=contacts><span class=muted>загрузка…</span></div></div></aside>
<section class=chat><div class=chat-head><strong id=chatTitle>Выберите чат</strong><small id=chatHint>Каналы и личные сообщения MeshCore</small></div><div id=messages><p class=muted>Читаю локальный архив…</p></div><form id=send class=send><div id=replyPreview class=reply-preview hidden><div><b id=replyAuthor></b><span id=replyText></span></div><button id=cancelReply type=button aria-label='Отменить ответ'>×</button></div><div id=emojiPanel class=emoji-panel hidden aria-label='Панель эмодзи'><button type=button data-emoji='😀'>😀</button><button type=button data-emoji='😄'>😄</button><button type=button data-emoji='😂'>😂</button><button type=button data-emoji='😊'>😊</button><button type=button data-emoji='😉'>😉</button><button type=button data-emoji='😍'>😍</button><button type=button data-emoji='🤔'>🤔</button><button type=button data-emoji='😎'>😎</button><button type=button data-emoji='🥳'>🥳</button><button type=button data-emoji='😢'>😢</button><button type=button data-emoji='😡'>😡</button><button type=button data-emoji='👍'>👍</button><button type=button data-emoji='👎'>👎</button><button type=button data-emoji='👌'>👌</button><button type=button data-emoji='🙏'>🙏</button><button type=button data-emoji='👏'>👏</button><button type=button data-emoji='👋'>👋</button><button type=button data-emoji='🤝'>🤝</button><button type=button data-emoji='💪'>💪</button><button type=button data-emoji='❤️'>❤️</button><button type=button data-emoji='🔥'>🔥</button><button type=button data-emoji='✨'>✨</button><button type=button data-emoji='🎉'>🎉</button><button type=button data-emoji='💡'>💡</button><button type=button data-emoji='⚠️'>⚠️</button><button type=button data-emoji='✅'>✅</button><button type=button data-emoji='❌'>❌</button><button type=button data-emoji='📡'>📡</button><button type=button data-emoji='📻'>📻</button><button type=button data-emoji='🛰️'>🛰️</button><button type=button data-emoji='🛜'>🛜</button><button type=button data-emoji='📍'>📍</button><button type=button data-emoji='🗺️'>🗺️</button><button type=button data-emoji='🤖'>🤖</button><button type=button data-emoji='🐰'>🐰</button><button type=button data-emoji='🚀'>🚀</button></div><textarea id=text rows=2 placeholder='Короткое сообщение…' required></textarea><a class=meshpic-link href='https://meshpic.org/' target=_blank rel='noopener noreferrer' aria-label='Открыть MeshPic в новой вкладке' title='Загрузить изображение через Интернет и скопировать короткую ссылку'>🖼 Фото</a><button id=emojiToggle class=emoji-toggle type=button aria-label='Открыть панель эмодзи' aria-expanded=false aria-controls=emojiPanel>😊</button><button id=sendButton>Отправить</button><div class=compose-meta><span>Фото загружается через Интернет; по LoRa отправляется только вставленная ссылка</span><span id=byteCount>0 / 133 байт</span></div></form></section>
</main>
<section id=radioPanel class=panel><div class=settings><div class=card><strong>Радио</strong><p id=radio class=muted>загрузка…</p><label>Мощность, dBm <input id=power type=number min=-9 max=22></label><button id=savePower class=secondary>Сохранить мощность</button><label>Регион сообщений (scope) <input id=scope maxlength=30 placeholder=msk></label><button id=saveScope class=secondary>Сохранить регион</button><p class=muted>Для Бутырского района: <code>msk</code>. Это транспортная метка пакетов, а не регион радиочастот и не IATA-код MOW Мешкартеля.</p></div>
<div class=card><strong>Приглашение в канал</strong><label>Имя канала <input id=channelInviteName maxlength=32 autocomplete=off placeholder='нужно только при вставке одного секрета'></label><label>Ссылка или секрет <input id=channelInvite type=password autocomplete=off placeholder='meshcore://channel/add?... или 32 HEX-символа'></label><button id=importChannelInvite class=secondary>Добавить в свободный слот</button><p class=muted>Полная ссылка уже содержит имя. Если вставляете только секрет, заполните имя выше. Данные разбираются локально; перед записью Orange Pi создаёт закрытую резервную копию, ключ после импорта не отображается.</p></div>
<div class=card><strong>Позиция advert</strong><label>Широта <input id=lat type=number min=-90 max=90 step=.000001></label><label>Долгота <input id=lon type=number min=-180 max=180 step=.000001></label><label><input id=share type=checkbox style='width:auto'> публиковать позицию в advert</label><div class=row><button id=savePosition class=secondary>Сохранить</button><button id=floodAdvert>Flood advert</button></div><p class=muted>Сохранение ничего не передаёт. Flood advert передаётся только отдельной кнопкой.</p></div></div></section>
<section id=systemPanel class=panel><div class=settings><div class=card><strong>Уведомления</strong><p id=notificationState class=muted>Проверяю поддержку браузера…</p><button id=enableNotifications class=secondary>Включить уведомления</button><p class=muted>Уведомления появляются только для новых входящих личных сообщений. На обычном HTTP некоторые браузеры разрешают их только после добавления сайта на домашний экран или через HTTPS.</p></div>
<div class=card><strong>Wi‑Fi ноды</strong><dl class=message-details><dt>Имя в Wi‑Fi</dt><dd id=wifiNodeName>загрузка…</dd><dt>Имя в MeshCore</dt><dd id=meshcoreNodeName>загрузка…</dd><dt>IP-адрес</dt><dd id=wifiNodeIp>загрузка…</dd><dt>Wi‑Fi MAC</dt><dd id=wifiNodeMac>загрузка…</dd></dl><p class=muted>Это MAC Wi‑Fi-интерфейса ноды, полученный Orange Pi из активного TCP-соединения. BLE использует соседний, но другой MAC-адрес.</p></div>
<div class=card><strong>Регионы сообщений</strong><p id=regionState class=muted>Проверяю…</p><label>Выбрать scope<select id=regionSelect><option value=msk>msk</option><option value=mow>mow</option><option value=ru>ru</option></select></label><div class=row><button id=applyRegion class=secondary disabled>Применить регион</button><button id=disableRegions class=danger disabled>Отключить регионы</button></div><p class=muted>Меняет региональную метку <code>scope</code> будущих flood-сообщений. Частота, профиль радиомодема и регион RU не меняются; отдельной передачи по LoRa нет. Другой scope можно ввести в разделе «Радио и позиция».</p></div>
<div class=card><strong>Ping-бот MeshCore</strong><p id=botState class=muted>Проверяю…</p><p class=muted>Формат: <code>🛜 Pong @[отправитель] N🐰 · Бутырский · RSSI · SNR (BarbieNode💅)</code></p><button id=botToggle class=secondary disabled>Изменить</button><p class=muted>Точный <code>Ping</code> в личном сообщении или канале <code>#connections</code>. Ответ идёт туда же. Пауза 5 минут на отправителя, не более 10 ответов в час.</p></div>
<div class=card><strong>Управление нодой</strong><button id=rebootNode class=secondary>Перезагрузить MeshCore</button><p class=muted>Перезапускает текущую прошивку без переключения режима и без передачи по LoRa.</p><form method=post action='/meshcore/boot/meshtastic'><button class=danger>Вернуться в Meshtastic</button></form></div>
<div class="card wide"><strong>Источник сообщений</strong><p>В этом архиве — только пакеты LoRa, полученные самой нодой через Companion-мост. MeshCoreTel и MQTT сюда не подмешиваются.</p><p class=muted>«Принято нодой» не означает, что сообщение получил другой участник.</p></div></div></section>
<dialog id=messageDetailsDialog><div class=dialog-head><strong id=messageDetailsTitle>Подробности сообщения</strong><button id=closeMessageDetails type=button aria-label=Закрыть>×</button></div><dl id=messageDetails class=message-details></dl></dialog>
<script>
const $=id=>document.getElementById(id);const encoder=new TextEncoder();let target=null,archive=[],channels=[],contacts=[],reads={},connected=false,lastRendered='',knownIds=null,currentPanel='chatPanel',botEnabled=false,replyTo=null,maxChannels=8,currentList='channels';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function linkify(value){const source=String(value??''),pattern=/(?:https?:[/][/]|meshcore:[/][/])[^\\s<>"']+/gi;let html='',last=0;for(const match of source.matchAll(pattern)){let url=match[0],tail='';while(/[),.;!?]$/.test(url)){tail=url.slice(-1)+tail;url=url.slice(0,-1)}html+=esc(source.slice(last,match.index))+`<a href="${esc(url)}" target=_blank rel="noopener noreferrer">${esc(url)}</a>`+esc(tail);last=Number(match.index)+match[0].length}return html+esc(source.slice(last))}
const keyOf=(kind,value)=>kind+':'+String(value);const sameContact=(a,b)=>String(a||'').startsWith(String(b||''))||String(b||'').startsWith(String(a||''));
const belongs=(m,t)=>t&&(t.kind==='channel'?m.kind==='channel'&&Number(m.channel)===Number(t.value):m.kind==='contact'&&sameContact(m.contact,t.value));
const stamp=value=>value?new Date(value*1000).toLocaleString('ru-RU',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'';
async function api(path,options){const r=await fetch('/meshcore/api'+path,options);const j=await r.json().catch(()=>({error:r.statusText}));if(!r.ok)throw Error(j.error||r.statusText);return j}
function lastMessage(kind,value){return archive.filter(m=>belongs(m,{kind,value})).at(-1)}
function unreadCount(kind,value){const seen=Number(reads[keyOf(kind,value)]??reads.__baseline??0);return archive.filter(m=>m.direction==='rx'&&belongs(m,{kind,value})&&Number(m.receivedAt||m.timestamp||0)>seen).length}
function navButton(item,kind){const value=kind==='channel'?item.index:item.publicKey,last=lastMessage(kind,value),unread=unreadCount(kind,value),active=target&&keyOf(target.kind,target.value)===keyOf(kind,value);return `<button class="${kind} ${active?'active':''}" data-kind="${kind}" data-value="${esc(value)}" data-name="${esc(item.name)}"><span class=nav-title><span>${esc(item.name)}</span>${unread?`<span class=badge>${unread>99?'99+':unread}</span>`:''}${last?`<time>${stamp(last.timestamp||last.receivedAt)}</time>`:''}</span>${kind==='channel'?`<span class=hash>${esc(item.hash||'')}</span>`:''}${last?`<span class=preview>${last.direction==='tx'?'Вы: ':''}${esc(last.text)}</span>`:'<span class=preview>Сообщений пока нет</span>'}</button>`}
function renderNav(){const q=$('search').value.trim().toLocaleLowerCase('ru');$('channels').innerHTML=channels.map(x=>navButton(x,'channel')).join('')||'<span class=muted>Нет каналов</span>';const shown=contacts.filter(x=>!q||String(x.name).toLocaleLowerCase('ru').includes(q)||String(x.publicKey).toLowerCase().includes(q)).sort((a,b)=>{const au=unreadCount('contact',a.publicKey),bu=unreadCount('contact',b.publicKey),am=lastMessage('contact',a.publicKey),bm=lastMessage('contact',b.publicKey);return bu-au||Number(bm?.timestamp||bm?.receivedAt||0)-Number(am?.timestamp||am?.receivedAt||0)||String(a.name).localeCompare(String(b.name),'ru')});$('contactCount').textContent=`(${shown.length}/${contacts.length})`;$('contacts').innerHTML=shown.map(x=>navButton(x,'contact')).join('')||'<span class=muted>Ничего не найдено</span>';document.querySelectorAll('[data-kind]').forEach(b=>b.onclick=()=>choose(b.dataset.kind,b.dataset.value,b.dataset.name));const channelTotal=channels.reduce((n,x)=>n+unreadCount('channel',x.index),0),directTotal=contacts.reduce((n,x)=>n+unreadCount('contact',x.publicKey),0),total=channelTotal+directTotal;$('channelUnread').innerHTML=channelTotal?`<span class=badge>${channelTotal>99?'99+':channelTotal}</span>`:'';$('directUnread').innerHTML=directTotal?`<span class=badge>${directTotal>99?'99+':directTotal}</span>`:'';$('markAllRead').disabled=total===0;document.title=(total?`(${total}) `:'')+'BarbieNode · MeshCore Messenger';renderChannelSlots()}
function switchList(list){currentList=list;$('channelPane').hidden=list!=='channels';$('contactPane').hidden=list!=='contacts';$('showChannels').classList.toggle('active',list==='channels');$('showContacts').classList.toggle('active',list==='contacts');document.querySelectorAll('.tab[data-tab="chatPanel"]').forEach(x=>x.classList.toggle('active',currentPanel==='chatPanel'&&x.dataset.list===list));try{localStorage.setItem('meshcore-list',list)}catch{}}
function renderChannelSlots(){const select=$('channelSlot'),current=select.value,slotCount=Math.max(maxChannels,channels.reduce((n,x)=>Math.max(n,Number(x.index)+1),0));select.innerHTML=Array.from({length:slotCount},(_,index)=>{const channel=channels.find(x=>Number(x.index)===index);return `<option value="${index}">${index}: ${esc(channel?.name||'свободен')}</option>`}).join('');if([...select.options].some(x=>x.value===current))select.value=current;else{const free=Array.from({length:slotCount},(_,i)=>i).find(i=>!channels.some(x=>Number(x.index)===i));select.value=String(free??0)}}
async function markTargetRead(){if(!target||document.visibilityState!=='visible'||currentPanel!=='chatPanel')return;const latest=archive.filter(m=>m.direction==='rx'&&belongs(m,target)).reduce((n,m)=>Math.max(n,Number(m.receivedAt||m.timestamp||0)),0),key=keyOf(target.kind,target.value),seen=Number(reads[key]??reads.__baseline??0);if(!latest||latest<=seen)return;reads[key]=latest;renderNav();try{await api('/read',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind:target.kind,value:target.value,seenAt:latest})})}catch(e){$('feedback').textContent='Не удалось сохранить отметку прочтения: '+e.message}}
function choose(kind,value,name){if(replyTo&&!belongs(replyTo,{kind,value}))clearReply();target={kind,value,name};try{localStorage.setItem('meshcore-chat',JSON.stringify(target))}catch{};$('chatTitle').textContent=(kind==='channel'?'# ':'')+name;$('chatHint').textContent=kind==='channel'?'Публичный канал · подтверждения получателей нет':'Личный диалог · статус ACK показывается под сообщением';renderNav();renderMessages(true);updateComposer();void markTargetRead();if(innerWidth<681)$('chatTitle').scrollIntoView({behavior:'smooth',block:'nearest'})}
function delivery(m){if(m.direction!=='tx')return '<span>Получено по LoRa</span>';if(m.kind==='channel')return '<span>Передано в эфир · без подтверждения</span>';if(m.delivery==='delivered')return `<span class=delivered>✓ Доставлено${m.roundTripMs!=null?' · '+esc(m.roundTripMs)+' мс':''}</span>`;return '<span>Принято нодой · ACK пока нет</span>'}
function renderMessages(force=false){if(!target){$('messages').innerHTML='<p class=muted>Выберите канал или контакт слева.</p>';return}const box=$('messages'),wasBottom=box.scrollHeight-box.scrollTop-box.clientHeight<80,current=keyOf(target.kind,target.value),channelNewestFirst=target.kind==='channel',rows=archive.filter(m=>belongs(m,target));if(channelNewestFirst)rows.reverse();box.innerHTML=rows.length?rows.map(m=>`<article class="msg ${m.direction==='tx'?'tx':''}"><div class=meta><b>${esc(m.direction==='tx'?(m.sender||'Мы'):(m.sender||'Неизвестный'))}</b><span>${stamp(m.timestamp||m.receivedAt)}</span><span>${m.direction==='tx'?'LoRa TX':'LoRa RX'}</span>${m.scope?`<span class=scope>${esc(m.scope)}</span>`:''}${m.bot?'<span>БОТ</span>':''}${m.snr!=null?`<span>SNR ${esc(m.snr)}</span>`:''}${m.rssi!=null?`<span>RSSI ${esc(m.rssi)}</span>`:''}${m.pathLength!=null?`<span>${esc(m.pathLength)} хоп.</span>`:''}${m.localTextHash?`<span title="Локальный хеш текста, не хеш MeshCoreTel">#${esc(m.localTextHash)}</span>`:''}</div><div class=text>${linkify(m.text)}</div><div class=msg-actions><div class=msg-buttons>${m.direction==='tx'?(m.bot?'':`<button type=button class=repeat-button data-repeat-index="${archive.indexOf(m)}">⟳ Повторить</button>`):`<button type=button class=reply-button data-reply-index="${archive.indexOf(m)}">↩ Ответить</button>`}<button type=button class=details-button data-details-index="${archive.indexOf(m)}">ⓘ Подробности</button></div><div class="delivery ${m.delivery==='delivered'?'delivered':''}">${delivery(m)}</div></div></article>`).join(''):'<p class=muted>Сообщений здесь пока нет.</p>';box.querySelectorAll('[data-reply-index]').forEach(button=>button.onclick=()=>startReply(archive[Number(button.dataset.replyIndex)]));box.querySelectorAll('[data-repeat-index]').forEach(button=>button.onclick=()=>repeatMessage(archive[Number(button.dataset.repeatIndex)]));box.querySelectorAll('[data-details-index]').forEach(button=>button.onclick=()=>showMessageDetails(archive[Number(button.dataset.detailsIndex)]));if(channelNewestFirst){if(force||current!==lastRendered)box.scrollTop=0}else if(force||wasBottom||current!==lastRendered)box.scrollTop=box.scrollHeight;lastRendered=current}
function detailRow(label,value,wide=false){const shown=value===undefined||value===null||value===''?'неизвестно':String(value);return `<dt${wide?' class=wide':''}>${esc(label)}</dt><dd${wide?' class=wide':''}>${esc(shown)}</dd>`}
function resolvedPath(m){const raw=String(m.path||'').replace(/[^0-9a-f]/gi,'').toLowerCase(),bytes=Number(m.pathHashSize)||(Number.isFinite(Number(m.pathHashMode))?Number(m.pathHashMode)+1:0),step=bytes*2;if(!raw||!step||raw.length%step)return '';const hops=[];for(let offset=0;offset<raw.length;offset+=step){const hash=raw.slice(offset,offset+step),matches=contacts.filter(c=>String(c.publicKey||'').toLowerCase().startsWith(hash));if(matches.length===1)hops.push(`${hash} — ${matches[0].name||'без имени'}`);else if(matches.length>1)hops.push(`${hash} — ⚠ несколько совпадений: ${matches.map(c=>c.name||'без имени').join(', ')}`);else hops.push(`${hash} — неизвестный ретранслятор`)}const start=m.sender||'отправитель';return [`${start} — источник`,...hops,`BarbieNode — приёмник`].join('\\n↓\\n')}
function showMessageDetails(m){if(!m)return;const when=m.timestamp||m.receivedAt,where=m.kind==='channel'?`${m.channelName||'Канал'} · слот ${m.channel}`:(m.recipient||m.sender||m.contact),direction=m.direction==='tx'?'исходящее · LoRa TX':'входящее · LoRa RX',namedPath=resolvedPath(m);$('messageDetailsTitle').textContent=m.direction==='tx'?'Исходящее сообщение':'Входящее сообщение';$('messageDetails').innerHTML=detailRow('Направление',direction)+detailRow('Время пакета',when?new Date(when*1000).toLocaleString('ru-RU'):'')+detailRow('Принято архивом',m.receivedAt?new Date(m.receivedAt*1000).toLocaleString('ru-RU'):'')+detailRow('Канал / контакт',where)+detailRow('Отправитель',m.sender)+detailRow('Регион scope',m.scope)+detailRow('Transport code',m.transportCode)+detailRow('RSSI',m.rssi!=null?`${m.rssi} dBm`:'')+detailRow('SNR',m.snr!=null?`${m.snr} dB`:'')+detailRow('Переходы',m.pathLength)+detailRow('Маршрут',m.path)+detailRow('Маршрут с именами',namedPath,true)+detailRow('Тип маршрута',m.routeType)+detailRow('Размер хеша пути',m.pathHashSize)+detailRow('Режим хеша пути',m.pathHashMode)+detailRow('Попытка',m.attempt)+detailRow('Хеш пакета',m.packetHash)+detailRow('Локальный хеш текста',m.localTextHash)+detailRow('ID архива',m.id)+detailRow('Статус',m.direction==='tx'?(m.delivery||'принято нодой'):'получено нодой')+detailRow('Источник',m.source)+detailRow('Текст',m.text,true);$('messageDetailsDialog').showModal()}
function clipBytes(value,limit){let out='';for(const char of String(value)){if(encoder.encode(out+char).length>limit)break;out+=char}return out}
function replyPrefix(){if(!replyTo||target?.kind==='contact'||replyTo.direction==='tx')return '';const author=clipBytes(replyTo.sender||'Неизвестный',40);return `@[${author}] `}
function composedText(){return replyPrefix()+$('text').value.trim()}
function startReply(message){if(!message)return;replyTo=message;$('replyAuthor').textContent=`Ответ: ${message.direction==='tx'?'Мы':(message.sender||'Неизвестный')}`;$('replyText').textContent=String(message.text||'').replace(/\\s+/g,' ').trim();$('replyPreview').hidden=false;updateComposer();$('text').focus()}
function repeatMessage(message){if(!message||message.direction!=='tx'||message.bot)return;clearReply();$('text').value=String(message.text||'');updateComposer();$('feedback').textContent='Текст подставлен. Проверьте его и нажмите «Отправить» для повторной передачи.';$('text').focus()}
function clearReply(focus=false){replyTo=null;$('replyPreview').hidden=true;$('replyAuthor').textContent='';$('replyText').textContent='';updateComposer();if(focus)$('text').focus()}
function toggleEmojiPanel(force){const panel=$('emojiPanel'),open=force===undefined?panel.hidden:Boolean(force);panel.hidden=!open;$('emojiToggle').setAttribute('aria-expanded',String(open));$('emojiToggle').setAttribute('aria-label',open?'Закрыть панель эмодзи':'Открыть панель эмодзи')}
function insertEmoji(value){const box=$('text'),start=Number.isInteger(box.selectionStart)?box.selectionStart:box.value.length,end=Number.isInteger(box.selectionEnd)?box.selectionEnd:start;box.setRangeText(String(value),start,end,'end');updateComposer();box.focus()}
function updateComposer(){const ownBytes=encoder.encode($('text').value.trim()).length,bytes=encoder.encode(composedText()).length,mention=Boolean(replyPrefix()),valid=ownBytes>0&&bytes<=133&&target&&connected;$('byteCount').textContent=`${bytes} / 133 байт${mention?' с упоминанием':''}`;$('byteCount').className=bytes>133?'over':'';$('sendButton').disabled=!valid}
function openPanel(id,list){currentPanel=id;if(list)switchList(list);document.querySelectorAll('.panel').forEach(x=>x.classList.toggle('active',x.id===id));document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.tab===id&&(!x.dataset.list||x.dataset.list===currentList)));try{localStorage.setItem('meshcore-panel',id)}catch{}if(id==='chatPanel')void markTargetRead()}
function notificationStatus(){if(!('Notification'in window)){$('notificationState').textContent='Этот браузер не поддерживает системные уведомления.';$('enableNotifications').disabled=true;return}const state=Notification.permission;$('notificationState').textContent=state==='granted'?'Уведомления о новых личных сообщениях включены.':state==='denied'?'Браузер запретил уведомления. Разрешение меняется в настройках сайта.':'Уведомления пока не включены.';$('enableNotifications').disabled=state==='granted'||state==='denied'}
function processNotifications(rows){if(knownIds===null){knownIds=new Set(rows.map(m=>m.id));return}const fresh=rows.filter(m=>m.id&&!knownIds.has(m.id)&&m.direction==='rx'&&m.kind==='contact');rows.forEach(m=>{if(m.id)knownIds.add(m.id)});if(!('Notification'in window)||Notification.permission!=='granted')return;fresh.forEach(m=>{if(document.visibilityState==='visible'&&target&&belongs(m,target)&&currentPanel==='chatPanel')return;const contact=contacts.find(x=>sameContact(x.publicKey,m.contact)),name=m.sender||contact?.name||'Личное сообщение MeshCore';const note=new Notification(name,{body:String(m.text||'').slice(0,180),tag:'meshcore-'+m.id});note.onclick=()=>{window.focus();openPanel('chatPanel','contacts');if(contact)choose('contact',contact.publicKey,contact.name);note.close()}})}
async function refresh(){try{const [s,c,k,m,r,b]=await Promise.all([api('/status'),api('/channels'),api('/contacts'),api('/messages?limit=500'),api('/reads'),api('/bot')]);connected=Boolean(s.connected);maxChannels=Math.max(1,Math.min(16,Number(s.device?.max_channels)||8));botEnabled=Boolean(b.enabled);$('botState').textContent=botEnabled?`Включён · ${b.repliesInWindow}/${b.globalLimit} ответов за последний час`:'Выключен';$('botToggle').textContent=botEnabled?'Выключить бота':'Включить бота';$('botToggle').disabled=false;const link=(s.transport||'companion').toUpperCase(),defaultScope=String(s.defaultScope||''),regionSelect=$('regionSelect');$('status').textContent=connected?link+' подключено':link+' недоступно';$('status').className='pill '+(connected?'ok':'');$('regionState').textContent=defaultScope?`Включён регион «${defaultScope}».`:'Регионы отключены.';if(defaultScope&&![...regionSelect.options].some(option=>option.value===defaultScope))regionSelect.add(new Option(defaultScope,defaultScope));if(defaultScope)regionSelect.value=defaultScope;$('applyRegion').disabled=!connected;$('disableRegions').disabled=!connected||!defaultScope;$('radio').innerHTML=`${esc(s.self?.name||'')}<br>${esc(s.self?.radio_freq||'—')} МГц · BW ${esc(s.self?.radio_bw||'—')} · SF${esc(s.self?.radio_sf||'—')} · CR${esc(s.self?.radio_cr||'—')} · scope ${esc(defaultScope||'не задан')}${s.error?'<br>'+esc(s.error):''}`;if(document.activeElement!==$('power'))$('power').value=s.self?.tx_power??'';if(document.activeElement!==$('scope'))$('scope').value=defaultScope;if(document.activeElement!==$('lat'))$('lat').value=s.self?.adv_lat||'';if(document.activeElement!==$('lon'))$('lon').value=s.self?.adv_lon||'';$('share').checked=Boolean(s.self?.adv_loc_policy);channels=c.channels;contacts=k.contacts;processNotifications(m.messages);archive=m.messages;reads=r.reads||{};if(!target){try{const saved=JSON.parse(localStorage.getItem('meshcore-chat')||'null');const item=saved?.kind==='channel'?channels.find(x=>String(x.index)===String(saved.value)):contacts.find(x=>sameContact(x.publicKey,saved?.value));if(item)target={kind:saved.kind,value:saved.value,name:item.name}}catch{}if(!target&&channels.length)target={kind:'channel',value:channels[0].index,name:channels[0].name};if(!target&&contacts.length)target={kind:'contact',value:contacts[0].publicKey,name:contacts[0].name}}if(target){$('chatTitle').textContent=(target.kind==='channel'?'# ':'')+target.name;$('chatHint').textContent=target.kind==='channel'?'Новые сообщения сверху · подтверждения получателей нет':'Личный диалог · статус ACK показывается под сообщением'}renderNav();renderMessages();updateComposer();void markTargetRead()}catch(e){connected=false;$('status').textContent='ошибка';$('status').className='pill';$('regionState').textContent='Не удалось прочитать состояние регионов.';$('applyRegion').disabled=true;$('disableRegions').disabled=true;$('feedback').textContent=e.message;updateComposer()}}
$('search').oninput=renderNav;$('showChannels').onclick=()=>switchList('channels');$('showContacts').onclick=()=>switchList('contacts');$('closeMessageDetails').onclick=()=>$('messageDetailsDialog').close();$('messageDetailsDialog').onclick=e=>{if(e.target===$('messageDetailsDialog'))$('messageDetailsDialog').close()};$('markAllRead').onclick=async()=>{const button=$('markAllRead');button.disabled=true;try{const result=await api('/read-all',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});reads={__baseline:Number(result.seenAt)||Math.floor(Date.now()/1000)};renderNav();$('feedback').textContent='Все текущие сообщения отмечены прочитанными. Передачи по LoRa не было.'}catch(e){$('feedback').textContent='Не удалось отметить сообщения: '+e.message;renderNav()}};$('showChannelForm').onclick=()=>{$('channelForm').hidden=false;renderChannelSlots();$('channelName').focus()};$('cancelChannel').onclick=()=>{$('channelForm').reset();$('channelSecret').value='';$('channelForm').hidden=true};$('channelForm').onsubmit=async e=>{e.preventDefault();const index=Number($('channelSlot').value),existing=channels.find(x=>Number(x.index)===index),name=$('channelName').value.trim();if(existing&&!confirm(`Заменить канал «${existing.name}» в слоте ${index}? Старый ключ этого слота будет утрачен.`))return;$('saveChannel').disabled=true;try{await api('/channel',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({index,name,secret:$('channelSecret').value,replace:Boolean(existing)})});$('channelForm').reset();$('channelSecret').value='';$('channelForm').hidden=true;$('feedback').textContent=`Канал «${name}» сохранён в слоте ${index}. Передачи по LoRa не было.`;await refresh();choose('channel',index,name)}catch(e){$('feedback').textContent=e.message}finally{$('saveChannel').disabled=false}};$('text').oninput=updateComposer;$('cancelReply').onclick=()=>clearReply(true);$('emojiToggle').onclick=()=>toggleEmojiPanel();$('emojiPanel').querySelectorAll('[data-emoji]').forEach(button=>button.onclick=()=>insertEmoji(button.dataset.emoji));$('text').onkeydown=e=>{if(e.key==='Escape'){toggleEmojiPanel(false);return}if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();if(!$('sendButton').disabled)$('send').requestSubmit()}};
$('send').onsubmit=async e=>{e.preventDefault();if(!target)return;const text=composedText();$('feedback').textContent='Передаю ноде…';$('sendButton').disabled=true;try{const result=await api('/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind:target.kind,[target.kind]:target.value,text})});$('text').value='';toggleEmojiPanel(false);clearReply();$('feedback').textContent=target.kind==='contact'?(result.receivedByPeer?'Получено подтверждение доставки.':'Нода передала сообщение; ожидаем ACK адресата.'):'Нода передала broadcast в LoRa; подтверждения отдельных получателей у канала нет.';await refresh()}catch(x){$('feedback').textContent=x.message;updateComposer()}};
$('savePower').onclick=async()=>{try{await api('/power',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({value:Number($('power').value)})});$('feedback').textContent='Мощность сохранена; передачи не было.';await refresh()}catch(e){$('feedback').textContent=e.message}};
$('saveScope').onclick=async()=>{const scope=$('scope').value.trim().replace(/^#/,'').toLowerCase();if(!scope||!confirm(`Установить регион сообщений «${scope}»? Он будет добавляться к будущим flood-пакетам.`))return;$('saveScope').disabled=true;try{await api('/scope',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({scope})});$('feedback').textContent=`Регион сообщений «${scope}» сохранён. Отдельной передачи по LoRa не было.`;await refresh()}catch(e){$('feedback').textContent=e.message}finally{$('saveScope').disabled=false}};
$('applyRegion').onclick=async()=>{const scope=$('regionSelect').value;if(!scope||!confirm(`Выбрать регион сообщений «${scope}»? Он будет применён к будущим flood-сообщениям.`))return;const button=$('applyRegion');button.disabled=true;try{await api('/scope',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({scope})});$('feedback').textContent=`Регион сообщений «${scope}» сохранён. Отдельной передачи по LoRa не было.`;await refresh()}catch(e){$('feedback').textContent='Не удалось выбрать регион: '+e.message;await refresh()}};
$('disableRegions').onclick=async()=>{if(!confirm('Отключить региональную метку scope для будущих flood-сообщений? Частота и радиопрофиль не изменятся.'))return;const button=$('disableRegions');button.disabled=true;try{await api('/scope',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({scope:''})});$('feedback').textContent='Регионы сообщений отключены. Отдельной передачи по LoRa не было.';await refresh()}catch(e){$('feedback').textContent='Не удалось отключить регионы: '+e.message;await refresh()}};
$('savePosition').onclick=async()=>{try{await api('/position',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({lat:Number($('lat').value),lon:Number($('lon').value),share:$('share').checked})});$('feedback').textContent='Позиция сохранена локально; advert не отправлялся.';await refresh()}catch(e){$('feedback').textContent=e.message}};
$('rebootNode').onclick=async()=>{if(!confirm('Перезагрузить ноду в текущем режиме MeshCore?'))return;$('feedback').textContent='Отправляю команду перезагрузки…';try{const r=await fetch('/meshcore/restart',{method:'POST'});const j=await r.json().catch(()=>({error:r.statusText}));if(!r.ok)throw Error(j.error||r.statusText);$('feedback').textContent='Команда принята. Нода вернётся в MeshCore примерно через 10–30 секунд.'}catch(e){$('feedback').textContent=e.message}};
$('botToggle').onclick=async()=>{const enabled=!botEnabled;if(enabled&&!confirm('Включить автоматические ответы на точный Ping в личке и #connections? Ответы передаются по LoRa с установленными лимитами.'))return;$('botToggle').disabled=true;try{const b=await api('/bot',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled})});botEnabled=b.enabled;$('feedback').textContent=botEnabled?'Ping-бот MeshCore включён. Само включение ничего не передало.':'Ping-бот MeshCore выключен.';await refresh()}catch(e){$('feedback').textContent=e.message}finally{$('botToggle').disabled=false}};
$('floodAdvert').onclick=async()=>{if(!confirm('Отправить один Flood advert по LoRa?'))return;try{await api('/advert',{method:'POST',headers:{'Content-Type':'application/json'},body:'{"flood":true}'});$('feedback').textContent='Один Flood advert принят нодой.'}catch(e){$('feedback').textContent=e.message}};
function parseChannelInvite(value){const raw=String(value||'').trim();let name='',secret='';if(/^[0-9a-f]{32}$/i.test(raw)){name=$('channelInviteName').value.trim();secret=raw;if(!name)throw Error('При вставке одного секрета укажите имя канала выше')}else{let url;try{url=new URL(raw)}catch{throw Error('Вставьте полную meshcore:// ссылку или 32-символьный HEX-секрет')}if(url.protocol!=='meshcore:'||url.hostname!=='channel'||url.pathname!=='/add')throw Error('Ожидалась ссылка meshcore://channel/add');name=(url.searchParams.get('name')||'').trim();secret=(url.searchParams.get('secret')||'').trim()}if(!name||encoder.encode(name).length>32)throw Error('Некорректное имя канала');if(!/^[0-9a-f]{32}$/i.test(secret))throw Error('Ключ приглашения должен содержать 32 HEX-символа');return{name,secret}}
$('importChannelInvite').onclick=async()=>{const button=$('importChannelInvite');button.disabled=true;try{const invite=parseChannelInvite($('channelInvite').value),status=await api('/status'),limit=Math.max(1,Math.min(64,Number(status.device?.max_channels)||maxChannels)),index=Array.from({length:limit},(_,i)=>i).find(i=>!channels.some(x=>Number(x.index)===i));if(index===undefined)throw Error('Свободных слотов нет — сначала удалите ненужный канал');await api('/channel',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({index,name:invite.name,secret:invite.secret})});$('channelInvite').value='';$('channelInviteName').value='';$('feedback').textContent=`Канал «${invite.name}» добавлен в слот ${index}. Передачи по LoRa не было.`;await refresh();choose('channel',index,invite.name)}catch(e){$('feedback').textContent=e.message}finally{$('channelInvite').value='';button.disabled=false}};
$('removeChannel').onclick=async()=>{const index=Number($('channelSlot').value),existing=channels.find(x=>Number(x.index)===index);if(!existing){$('feedback').textContent=`Слот ${index} уже свободен.`;return}if(!confirm(`Удалить канал «${existing.name}» из слота ${index}? Перед удалением будет создана резервная копия.`))return;$('removeChannel').disabled=true;try{await api('/channel/remove',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({index})});$('channelForm').reset();$('channelSecret').value='';$('channelForm').hidden=true;target=null;try{localStorage.removeItem('meshcore-chat')}catch{}$('feedback').textContent=`Канал «${existing.name}» удалён из слота ${index}. Передачи по LoRa не было.`;await refresh()}catch(e){$('feedback').textContent=e.message}finally{$('removeChannel').disabled=false}};
document.querySelectorAll('.tab').forEach(button=>button.onclick=()=>openPanel(button.dataset.tab,button.dataset.list));$('enableNotifications').onclick=async()=>{if(!('Notification'in window))return;try{await Notification.requestPermission()}catch(e){$('feedback').textContent='Браузер не разрешил запрос уведомлений: '+e.message}notificationStatus()};document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')void markTargetRead()});const fitViewport=()=>document.documentElement.style.setProperty('--app-height',`${Math.round(window.visualViewport?.height||window.innerHeight)}px`);window.addEventListener('resize',fitViewport);window.visualViewport?.addEventListener('resize',fitViewport);window.visualViewport?.addEventListener('scroll',fitViewport);fitViewport();let savedPanel='chatPanel';try{const value=localStorage.getItem('meshcore-panel');if(['chatPanel','radioPanel','systemPanel'].includes(value))savedPanel=value;const list=localStorage.getItem('meshcore-list');if(['channels','contacts'].includes(list))currentList=list}catch{}switchList(currentList);openPanel(savedPanel,currentList);notificationStatus();refresh();setInterval(refresh,5000);
</script>
<script>
async function refreshWifiIdentity(){
  try{
    const s=await api('/status');
    $('wifiNodeName').textContent=s.wifi?.name||'не задано';
    $('meshcoreNodeName').textContent=s.self?.name||'неизвестно';
    $('wifiNodeIp').textContent=s.wifi?.ip||'неизвестно';
    $('wifiNodeMac').textContent=s.wifi?.mac||'неизвестно';
  }catch(e){
    $('wifiNodeName').textContent='недоступно';
    $('meshcoreNodeName').textContent='недоступно';
    $('wifiNodeIp').textContent='недоступно';
    $('wifiNodeMac').textContent='недоступно';
  }
}
refreshWifiIdentity();setInterval(refreshWifiIdentity,5000);
</script>
</html>""".replace("__NOTICE__", notice).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_rnode_page(self) -> None:
        body = """<!doctype html><html lang=ru><meta charset=utf-8>
<meta name=viewport content='width=device-width,initial-scale=1'>
<title>BarbieNode · RNode</title><style>
:root{--bg:#07111f;--panel:#0d1a2b;--panel2:#12233a;--line:#253b57;--text:#eef5ff;--muted:#92a6bf;--green:#31d08b;--blue:#64a8ff;--red:#ff6b78;--amber:#f5b942}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px system-ui,-apple-system,sans-serif}header{display:flex;gap:12px;align-items:center;padding:14px 18px;border-bottom:1px solid var(--line);position:sticky;top:0;background:#07111ff2;z-index:3}header strong{font-size:19px}header small{display:block;color:var(--muted)}.pill{margin-left:auto;border:1px solid var(--line);padding:6px 10px;border-radius:999px;color:var(--muted);white-space:nowrap}.pill.ok{color:var(--green);border-color:#267a59}.pill.warn{color:var(--amber);border-color:#806424}
.tabs{display:flex;gap:4px;padding:8px 14px;border-bottom:1px solid var(--line);background:#091625}.tab{background:transparent;color:var(--muted);border:1px solid transparent}.tab.active{background:var(--panel2);color:var(--text);border-color:var(--line)}.panel{display:none}.panel.active{display:block}#chat.active{display:grid;grid-template-columns:290px minmax(0,1fr);height:calc(100vh - 143px)}.settings{max-width:920px;margin:0 auto;padding:22px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.wide{grid-column:1/-1}.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:15px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.metric{background:#081524;border:1px solid var(--line);border-radius:10px;padding:11px}.metric span{display:block;color:var(--muted);font-size:12px}.metric b{display:block;margin-top:3px}.card label{display:block;margin:10px 0 5px}.card input,.card select{width:100%;background:#081524;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:9px}.row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}button{font:inherit;border:0;border-radius:9px;padding:9px 12px;background:var(--green);color:#052116;font-weight:700;cursor:pointer}button.secondary{background:var(--panel2);color:var(--text);border:1px solid var(--line)}button.danger{background:#51232c;color:#fff}button:disabled{opacity:.45;cursor:not-allowed}.muted{color:var(--muted)}.ok{color:var(--green)}.bad{color:var(--red)}#feedback{min-height:38px;padding:10px 18px;color:var(--muted);border-bottom:1px solid var(--line)}code,.address{color:#9ed2ff;font:12px ui-monospace,monospace;overflow-wrap:anywhere}
.contacts{border-right:1px solid var(--line);padding:14px;overflow:auto}.contacts h2{font-size:12px;text-transform:uppercase;color:var(--muted)}.contact{display:block;width:100%;text-align:left;background:transparent;color:var(--text);border:1px solid transparent;margin:5px 0}.contact.active,.contact:hover{background:var(--panel2);border-color:var(--line)}.contact small{display:block;color:var(--muted);overflow:hidden;text-overflow:ellipsis}.conversation{display:flex;flex-direction:column;min-width:0}.chathead{padding:12px 16px;border-bottom:1px solid var(--line)}#messages{padding:16px;overflow:auto;display:flex;flex:1;flex-direction:column;gap:9px}.msg{max-width:78%;background:var(--panel);border:1px solid var(--line);border-radius:13px;padding:10px}.msg.tx{align-self:flex-end;background:#123329;border-color:#24664f}.meta,.delivery{font-size:11px;color:var(--muted);margin-bottom:5px}.delivery{margin:5px 0 0;text-align:right}.delivery.good{color:var(--green)}.composer{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:12px;border-top:1px solid var(--line)}textarea{background:#081524;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:9px;resize:none}.counter{grid-column:1/-1;color:var(--muted);font-size:12px;text-align:right}.counter.over{color:var(--red)}.announce-wrap{max-width:1180px;margin:0 auto;padding:18px}.announce-help{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:13px;margin-bottom:12px}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:12px}.announce-table{width:100%;border-collapse:collapse;background:var(--panel);min-width:840px}.announce-table th,.announce-table td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}.announce-table th{position:sticky;top:0;background:#102039;color:var(--muted);font-size:12px}.announce-table tr:last-child td{border-bottom:0}.source{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:3px 7px;white-space:nowrap;font-size:12px}.source.lora{color:var(--green);border-color:#267a59}.source.archive{color:var(--amber);border-color:#806424}.signal{white-space:nowrap}.announce-name{font-weight:700}.announce-address{display:block;color:var(--muted);font:11px ui-monospace,monospace;margin-top:3px}
@media(max-width:680px){header small{display:none}.tabs{overflow:auto}.tab{white-space:nowrap}.settings{display:block;padding:14px}.card{margin-bottom:12px}.grid{grid-template-columns:1fr}#chat.active{display:flex;height:auto;min-height:calc(100vh - 143px);flex-direction:column}.contacts{border:0;border-bottom:1px solid var(--line);max-height:35vh}.conversation{min-height:60vh}#messages{min-height:38vh}.msg{max-width:92%}}
</style><header><div><strong>BarbieNode · RNode</strong><small>Orange Pi ↔ TCP 7633 ↔ RNode ↔ LoRa</small></div><span id=status class=pill>подключение…</span></header>
<nav class=tabs><a class=tab href='https://192.168.1.19:9337/'>Чаты MeshChatX →</a><button class="tab active" data-tab=overview>Состояние</button><button class=tab data-tab=announces>Анонсы LoRa</button><button class=tab data-tab=radio>Радио</button><button class=tab data-tab=system>Система</button></nav><div id=feedback>Чаты, контакты и история теперь хранятся единым сервером MeshChatX на Orange Pi.</div>
<section id=chat class=panel><aside class=contacts><h2>Мой LXMF-адрес</h2><div id=myAddress class=address>загрузка…</div><div class=row><button id=copyAddress class=secondary>Копировать</button><button id=announce>Объявить адрес</button></div><p class=muted>Объявление — отдельная передача LoRa. Автоматических объявлений нет.</p><h2>Контакты из LoRa-объявлений</h2><div id=contactList><span class=muted>Пока пусто</span></div></aside><div class=conversation><div class=chathead><b id=chatName>Выберите контакт</b><div id=chatAddress class=address></div></div><div id=messages><p class=muted>Сообщения появятся после выбора контакта.</p></div><form id=send class=composer><textarea id=text rows=2 maxlength=240 placeholder='Короткое LXMF-сообщение…'></textarea><button id=sendButton disabled>Отправить</button><span id=counter class=counter>0 / 240 байт</span></form></div></section>
<section id=overview class="panel active"><div class=settings><div class="card wide"><h2>RNode / Reticulum</h2><p><a class=button-link href='https://192.168.1.19:9337/'>Открыть MeshChatX</a></p><div class=grid><div class=metric><span>Радио</span><b id=radioState>…</b></div><div class=metric><span>Reticulum TCP</span><b id=hostState>…</b></div><div class=metric><span>LXMF</span><b id=bridgeState>MeshChatX</b></div><div class=metric><span>Последний RSSI</span><b id=rssi>…</b></div></div></div><div class="card wide"><strong>Только LoRa</strong><p>TCP используется только внутри домашней сети между Orange Pi и нодой. Интернет-шлюзы Reticulum не настроены. Автоматические объявления выключены.</p></div></div></section>
<section id=announces class=panel><div class=announce-wrap><div class=announce-help><strong>Журнал LXMF-анонсов</strong><p class=muted>Зелёная метка означает, что новый анонс зафиксирован при единственном активном интерфейсе Reticulum — локальной RNode. Старые строки из базы MeshChatX помечены как архив: их исходный транспорт задним числом доказать нельзя. RSSI и SNR относятся к принятому радиопакету; пустое значение означает «неизвестно».</p><span id=announceJournalState class=muted>Загрузка…</span></div><div class=table-wrap><table class=announce-table><thead><tr><th>Время</th><th>Узел</th><th>Источник</th><th>Сигнал</th><th>Тип</th><th>Счётчик</th></tr></thead><tbody id=announceRows><tr><td colspan=6 class=muted>Загрузка…</td></tr></tbody></table></div></div></section>
<section id=radio class=panel><div class=settings><form id=radioForm class="card wide"><h2>Параметры LoRa</h2><label>Частота, Гц</label><input name=freq id=freq type=number min=850000000 max=930000000 required><label>Полоса</label><select name=bw id=bw><option>7800</option><option>10400</option><option>15600</option><option>20800</option><option>31250</option><option>41700</option><option>62500</option><option>125000</option><option>250000</option><option>500000</option></select><div class=grid><div><label>Spreading Factor</label><select name=sf id=sf><option>7</option><option>8</option><option>9</option><option>10</option><option>11</option><option>12</option></select></div><div><label>Coding Rate</label><select name=cr id=cr><option>5</option><option>6</option><option>7</option><option>8</option></select></div></div><label>Мощность, dBm</label><input name=txp id=txp type=number min=2 max=22 required><div class=row><button id=saveRadio>Сохранить и применить</button><button id=backupRadio type=button class=secondary>Скачать резервную копию</button></div><p class=muted>Перед изменением интерфейс обязательно скачивает текущий профиль. Сохранение параметров само по себе не отправляет пакет LoRa. При подключённом Reticulum-клиенте изменение будет отклонено.</p></form></div></section>
<section id=system class=panel><div class=settings><div class=card><h2>Вернуться в Meshtastic</h2><p>Проверенный образ Meshtastic находится в <code>app0</code>. Команда остановит RNode, выберет app0 и перезагрузит плату; затем этот же адрес автоматически покажет интерфейс Meshtastic.</p><button id=bootMeshtastic class=danger>Вернуться в Meshtastic</button></div><div class=card><h2>Удалённое восстановление</h2><p>Если RNode не подключится к сохранённому домашнему Wi‑Fi за 90 секунд, прошивка автоматически вернётся в Meshtastic. Физическая кнопка не является штатным способом переключения.</p></div></div></section>
<script>
const $=id=>document.getElementById(id);const encoder=new TextEncoder();let loaded=false,backupReady=false,current={},bridge={},contacts=[],archive=[],selected=null;const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(path,options){const r=await fetch(path,{cache:'no-store',...options}),text=await r.text();if(!r.ok){try{throw Error(JSON.parse(text).error||text)}catch(e){throw e}}try{return JSON.parse(text)}catch{return text}}
const rapi=(path,options)=>api('/reticulum/api'+path,options);const stamp=v=>v?new Date(v*1000).toLocaleString('ru-RU'):'—';
function openPanel(id){document.querySelectorAll('.panel').forEach(x=>x.classList.toggle('active',x.id===id));document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x.dataset.tab===id))}
function signal(value,suffix){return value===null||value===undefined||value===''?'—':`${esc(value)} ${suffix}`}
function renderAnnounces(payload){const rows=Array.isArray(payload?.events)?payload.events:[];$('announceJournalState').textContent=payload?.rnodeOnly?'Активен только RNode-интерфейс · новые события можно однозначно маркировать LoRa RX':'Включён дополнительный Reticulum-интерфейс · источник новых событий нельзя считать только LoRa';$('announceRows').innerHTML=rows.length?rows.map(row=>`<tr><td>${stamp(row.receivedAt)}</td><td><span class=announce-name>${esc(row.name||'Без имени')}</span><span class=announce-address>${esc(row.destinationHash||'')}</span></td><td><span class="source ${row.source==='lora-rnode'?'lora':'archive'}">${row.source==='lora-rnode'?'LoRa RX · RNode':'Архив MeshChatX'}</span></td><td class=signal>RSSI ${signal(row.rssi,'dBm')}<br>SNR ${signal(row.snr,'dB')}<br>Q ${signal(row.quality,'')}</td><td>${esc(row.aspect||'—')}</td><td>${esc(row.announceCount||1)}</td></tr>`).join(''):'<tr><td colspan=6 class=muted>Анонсов пока нет.</td></tr>'}
async function refreshAnnounces(){try{renderAnnounces(await api('/rnode/announces?limit=300'))}catch(e){$('announceJournalState').textContent='Журнал пока недоступен: '+e.message}}
function renderContacts(){contactList.innerHTML=contacts.length?contacts.map(c=>`<button class="contact ${selected===c.address?'active':''}" data-address="${esc(c.address)}"><b>${esc(c.name||c.address.slice(0,12))}</b><small>${esc(c.address)} · ${c.lastAnnounce?'LoRa '+stamp(c.lastAnnounce):'из архива'}</small></button>`).join(''):'<span class=muted>Контактов пока нет. Они появятся после принятых по LoRa LXMF-объявлений.</span>';document.querySelectorAll('.contact').forEach(b=>b.onclick=()=>choose(b.dataset.address))}
function choose(address){selected=address;const c=contacts.find(x=>x.address===address);$('chatName').textContent=c?.name||address.slice(0,12);$('chatAddress').textContent=address;renderContacts();renderMessages();updateComposer()}
function renderMessages(){if(!selected){$('messages').innerHTML='<p class=muted>Выберите контакт слева.</p>';return}const rows=archive.filter(m=>m.contact===selected);$('messages').innerHTML=rows.length?rows.map(m=>`<article class="msg ${m.direction==='tx'?'tx':''}"><div class=meta>${m.direction==='tx'?'LoRa TX':'LoRa RX'} · ${stamp(m.timestamp||m.receivedAt)}</div><div>${esc(m.text||'')}</div><div class="delivery ${m.delivery==='delivered'?'good':''}">${m.direction==='rx'?'Принято нодой по LoRa':m.delivery==='delivered'?'✓ Доставлено адресату':m.delivery==='failed'?'Ошибка доставки':'Поставлено в очередь; подтверждения пока нет'}</div></article>`).join(''):'<p class=muted>Сообщений с этим контактом пока нет.</p>';$('messages').scrollTop=$('messages').scrollHeight}
function updateComposer(){const n=encoder.encode($('text').value.trim()).length;$('counter').textContent=`${n} / 240 байт`;$('counter').className='counter'+(n>240?' over':'');$('sendButton').disabled=!selected||!bridge.rnodeOnline||n<1||n>240}
async function refresh(){try{const s=await api('/api/status');current=s;$('status').textContent=s.host?'RNode + MeshChatX':'RNode ожидается';$('status').className='pill '+(s.host?'ok':'warn');$('radioState').textContent=s.radio?'приём включён':(s.radio_error?'ошибка запуска':'ожидает настройки');$('radioState').className=s.radio?'ok':(s.radio_error?'bad':'');$('hostState').textContent=s.host?'подключён':'ожидание';$('bridgeState').textContent='MeshChatX :9337';$('rssi').textContent=Number(s.rssi)<=-292?'нет данных':`${s.rssi} dBm`;if(!loaded){$('freq').value=s.freq||868825000;$('bw').value=s.bw||125000;$('sf').value=s.sf||10;$('cr').value=s.cr||7;$('txp').value=s.txp===255?18:Math.max(2,Math.min(22,Number(s.txp)||18));loaded=true}}catch(e){$('status').textContent='RNode недоступен';$('status').className='pill warn';$('feedback').textContent=e.message}await refreshAnnounces()}
function downloadBackup(){const data={savedAt:new Date().toISOString(),mode:'rnode',frequency:Number(current.freq),bandwidth:Number(current.bw),spreadingFactor:Number(current.sf),codingRate:Number(current.cr),txPower:Number(current.txp),marker:current.dualboot_marker||''},blob=new Blob([JSON.stringify(data,null,2)+'\\n'],{type:'application/json'}),a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`barbienode-rnode-radio-${new Date().toISOString().replace(/[:.]/g,'-')}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);backupReady=true;$('feedback').textContent='Резервная копия текущего профиля скачана.'}
$('backupRadio').onclick=downloadBackup;$('radioForm').onsubmit=async e=>{e.preventDefault();if(!backupReady){downloadBackup();$('feedback').textContent='Сначала скачана резервная копия. Проверьте файл и нажмите «Сохранить» ещё раз.';return}if(!confirm('Применить новые параметры RNode? Радиопрофиль изменится, но отдельный пакет LoRa не передаётся.'))return;$('saveRadio').disabled=true;$('feedback').textContent='Сохраняю параметры…';try{const body=new URLSearchParams(new FormData(e.currentTarget));const text=await api('/api/radio',{method:'POST',body});$('feedback').textContent=String(text);backupReady=false;loaded=false;await refresh()}catch(x){$('feedback').textContent='Ошибка: '+x.message}finally{$('saveRadio').disabled=false}};
$('copyAddress').onclick=async()=>{try{await navigator.clipboard.writeText(bridge.address||'');$('feedback').textContent='LXMF-адрес скопирован.'}catch(e){$('feedback').textContent='Не удалось скопировать: '+e.message}};
$('announce').onclick=async()=>{if(!confirm('Передать одно LXMF-объявление по LoRa? Это займёт эфирное время.'))return;$('announce').disabled=true;try{await rapi('/announce',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});$('feedback').textContent='Одно объявление принято локальным LXMF-мостом для передачи по LoRa.'}catch(e){$('feedback').textContent='Ошибка: '+e.message}finally{$('announce').disabled=false}};
$('text').oninput=updateComposer;$('send').onsubmit=async e=>{e.preventDefault();if(!selected)return;$('sendButton').disabled=true;try{const result=await rapi('/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({contact:selected,text:$('text').value})});if(result.identityPending){$('feedback').textContent=result.pathRequested?'Identity контакта запрошена по LoRa. Текст сохранён — подождите объявления и нажмите «Отправить» ещё раз.':'Запрос identity уже отправлен. Текст сохранён; дождитесь ответа контакта и повторите отправку.'}else{$('text').value='';$('feedback').textContent=`Сообщение ${result.id} поставлено в LoRa-очередь; доставка ещё не подтверждена.`;await refresh()}}catch(x){$('feedback').textContent='Ошибка: '+x.message}finally{updateComposer()}};
$('bootMeshtastic').onclick=async()=>{if(!confirm('Остановить MeshChatX и загрузить проверенный Meshtastic app0? Возврат выполняется удалённо на этом же адресе.'))return;$('bootMeshtastic').disabled=true;$('feedback').textContent='Освобождаю RNode…';try{await api('/rnode/prepare-boot',{method:'POST'});let text='',lastError;for(let attempt=0;attempt<6&&!text;attempt++){await new Promise(resolve=>setTimeout(resolve,1200));$('feedback').textContent='Переключаюсь в Meshtastic…';try{text=await api('/api/boot/meshtastic',{method:'POST'})}catch(e){lastError=e}}if(!text)throw lastError||Error('RNode не освободил TCP-сеанс');$('feedback').textContent=String(text)+' Ожидаю загрузку…';setTimeout(()=>location.replace('/'),9000)}catch(e){$('feedback').textContent='Ошибка: '+e.message;$('bootMeshtastic').disabled=false}};
document.querySelectorAll('button.tab').forEach(button=>button.onclick=()=>openPanel(button.dataset.tab));refresh();setInterval(refresh,3000);
</script></html>""".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _proxy_meshcore_bridge(self) -> None:
        parsed = urlsplit(self.path)
        suffix = parsed.path.removeprefix("/meshcore/api") or "/status"
        target = MESHCORE_BRIDGE_URL + suffix + (("?" + parsed.query) if parsed.query else "")
        body = None
        if self.command in {"POST", "PUT"}:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 16 * 1024:
                self.send_error(413, "MeshCore request too large")
                return
            body = self.rfile.read(length)
        request = Request(
            target, data=body, method=self.command,
            headers={"Content-Type": self.headers.get("Content-Type", "application/json")},
        )
        try:
            with urlopen(request, timeout=30) as response:
                response_body = response.read(2 * 1024 * 1024)
                status = response.status
                content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
        except HTTPError as error:
            response_body = error.read(512 * 1024)
            status = error.code
            content_type = error.headers.get("Content-Type", "application/json; charset=utf-8")
        except (URLError, TimeoutError, OSError) as error:
            self._send_json({"error": f"MeshCore BLE bridge unavailable: {error}"}, status=503)
            return
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(response_body)

    def _proxy_reticulum_bridge(self) -> None:
        parsed = urlsplit(self.path)
        suffix = parsed.path.removeprefix("/reticulum/api") or "/status"
        target = RETICULUM_BRIDGE_URL + suffix + (("?" + parsed.query) if parsed.query else "")
        body = None
        if self.command in {"POST", "PUT"}:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 16 * 1024:
                self.send_error(413, "Reticulum request too large")
                return
            body = self.rfile.read(length)
        request = Request(
            target, data=body, method=self.command,
            headers={"Content-Type": self.headers.get("Content-Type", "application/json")},
        )
        try:
            with urlopen(request, timeout=30) as response:
                response_body = response.read(2 * 1024 * 1024)
                status = response.status
                content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
        except HTTPError as error:
            response_body = error.read(512 * 1024)
            status = error.code
            content_type = error.headers.get("Content-Type", "application/json; charset=utf-8")
        except (URLError, TimeoutError, OSError) as error:
            self._send_json({"error": f"Локальный Reticulum-мост недоступен: {error}"}, status=503)
            return
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(response_body)

    def _prepare_rnode_boot(self) -> None:
        request = Request(
            RNODE_MODE_CONTROL_URL + "/prepare-boot",
            data=b"{}",
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=25) as response:
                response_body = response.read(64 * 1024)
                status = response.status
        except HTTPError as error:
            response_body = error.read(64 * 1024)
            status = error.code
        except (URLError, TimeoutError, OSError) as error:
            self._send_json({"error": f"Не удалось освободить RNode: {error}"}, status=503)
            return
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(response_body)

    def _proxy_rnode_announces(self) -> None:
        parsed = urlsplit(self.path)
        target = RNODE_MODE_CONTROL_URL + "/announces"
        if parsed.query:
            target += "?" + parsed.query
        try:
            with urlopen(Request(target, method="GET"), timeout=5) as response:
                response_body = response.read(2 * 1024 * 1024)
                status = response.status
        except HTTPError as error:
            response_body = error.read(256 * 1024)
            status = error.code
        except (URLError, TimeoutError, OSError) as error:
            self._send_json({"error": f"Журнал анонсов недоступен: {error}"}, status=503)
            return
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(response_body)

    def _boot_meshcore_to_meshtastic(self) -> None:
        if self._device_is_meshtastic():
            self._send_meshcore_return_page("Meshtastic уже загружен. Можно обновить страницу.")
            return
        try:
            request = Request(f"{DEVICE_URL}/boot/meshtastic", data=b"", method="POST")
            with urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise OSError(f"device returned HTTP {response.status}")
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            # The switch can complete between rendering the MeshCore page and
            # handling its form POST. Meshtastic intentionally has no
            # /boot/meshtastic route, so its 404 is success in this race.
            if self._device_is_meshtastic():
                self._send_meshcore_return_page("Meshtastic уже загружен. Можно обновить страницу.")
                return
            self.send_error(502, f"MeshCore return command failed: {error}")
            return
        self._send_meshcore_return_page(
            "Команда принята. Meshtastic загружается; обновите эту страницу примерно через 20 секунд."
        )

    def _restart_meshcore(self) -> None:
        """Prefer the node's Wi-Fi reset route, with BLE as the live fallback."""
        errors = []
        try:
            request = Request(f"{DEVICE_URL}/restart", data=b"", method="POST")
            with urlopen(request, timeout=5) as response:
                if response.status != 200:
                    raise OSError(f"device returned HTTP {response.status}")
            self._send_json({"ok": True, "restarting": True, "transport": "wifi", "transmitted": False})
            return
        except (HTTPError, URLError, TimeoutError, OSError) as error:
            errors.append(f"Wi-Fi: {error}")
        try:
            request = Request(
                f"{MESHCORE_BRIDGE_URL}/reboot", data=b"{}", method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read(64 * 1024))
            self._send_json(payload)
            return
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as error:
            errors.append(f"BLE: {error}")
        self._send_json({"error": "Не удалось перезагрузить ноду; " + "; ".join(errors)}, status=503)

    def _send_node_cache(self) -> None:
        with NODE_CACHE_LOCK:
            try:
                body = NODE_CACHE_PATH.read_bytes()
            except FileNotFoundError:
                body = b'{"savedAt":0,"nodes":[]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _save_node_cache(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > MAX_NODE_CACHE_BYTES:
            self.send_error(413, "Invalid node cache size")
            return
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict) or not isinstance(payload.get("nodes"), list):
                raise ValueError("nodes must be a list")
            if len(payload["nodes"]) > 2000:
                raise ValueError("too many nodes")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            self.send_error(400, f"Invalid node cache: {error}")
            return
        NODE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = NODE_CACHE_PATH.with_suffix(".tmp")
        with NODE_CACHE_LOCK:
            try:
                previous = json.loads(NODE_CACHE_PATH.read_text())
            except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
                previous = {}
            own_node_num = uint32(payload.get("ownNodeNum"))
            if not own_node_num and isinstance(previous, dict):
                own_node_num = uint32(previous.get("ownNodeNum"))
            if not own_node_num:
                candidates = []
                for node in payload["nodes"]:
                    if not isinstance(node, dict) or not isinstance(node.get("user"), dict):
                        continue
                    node_num = uint32(node.get("num"))
                    user = node["user"]
                    if (node_num and user.get("id") == f"!{node_num:08x}"
                            and user.get("longName") == PROJECT_OWNER_LONG_NAME
                            and user.get("shortName") == PROJECT_OWNER_SHORT_NAME):
                        candidates.append(node_num)
                if len(candidates) == 1:
                    own_node_num = candidates[0]
            if own_node_num:
                payload["ownNodeNum"] = own_node_num
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
            temporary.write_bytes(encoded)
            os.replace(temporary, NODE_CACHE_PATH)
        self.send_response(204)
        self.end_headers()

    def _send_aim_measurements(self) -> None:
        with AIM_MEASUREMENTS_LOCK:
            try:
                body = AIM_MEASUREMENTS_PATH.read_bytes()
            except FileNotFoundError:
                body = b'{"version":2,"savedAt":0,"samples":[]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _save_aim_measurements(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > MAX_AIM_MEASUREMENTS_BYTES:
            self.send_error(413, "Invalid aiming measurements size")
            return
        try:
            payload = json.loads(self.rfile.read(length))
            source = payload.get("samples") if isinstance(payload, dict) else None
            if not isinstance(source, list) or len(source) > 2000:
                raise ValueError("samples must be a list of at most 2000 items")
            samples = []
            for item in source:
                if not isinstance(item, dict):
                    raise ValueError("invalid sample")
                ts, heading, node, rssi = float(item.get("ts")), float(item.get("heading")), int(item.get("from")), float(item.get("rssi"))
                snr = item.get("snr")
                snr = None if snr is None else float(snr)
                if not all(map(math.isfinite, (ts, heading, rssi))) or not node or not -200 <= rssi <= 50 or snr is not None and not math.isfinite(snr):
                    raise ValueError("sample values out of range")
                sample = {"ts": int(ts), "heading": heading % 360, "from": node & 0xFFFFFFFF, "rssi": rssi}
                if snr is not None:
                    sample["snr"] = snr
                samples.append(sample)
            version = int(payload.get("version", 1))
            if version < 1:
                raise ValueError("version must be a positive integer")
            normalized = {"version": version, "savedAt": int(payload.get("savedAt", 0)), "samples": samples}
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self.send_error(400, f"Invalid aiming measurements: {error}")
            return
        encoded = json.dumps(normalized, ensure_ascii=False, separators=(",", ":")).encode()
        AIM_MEASUREMENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = AIM_MEASUREMENTS_PATH.with_suffix(".tmp")
        with AIM_MEASUREMENTS_LOCK:
            temporary.write_bytes(encoded)
            os.replace(temporary, AIM_MEASUREMENTS_PATH)
        self.send_response(204)
        self.end_headers()

    def _send_own_location(self) -> None:
        self._send_json(self._read_json_file(OWN_LOCATION_PATH, {}))

    def _save_own_location(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 4096:
            self.send_error(413, "Invalid location size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            latitude, longitude = float(source.get("latitude")), float(source.get("longitude"))
            node_id = int(source.get("nodeId", 0)) & 0xFFFFFFFF
            if not math.isfinite(latitude) or not -90 <= latitude <= 90 or not math.isfinite(longitude) or not -180 <= longitude <= 180:
                raise ValueError("coordinates out of range")
            payload = {"latitude": latitude, "longitude": longitude, "nodeId": node_id, "savedAt": int(time.time())}
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self.send_error(400, f"Invalid location: {error}")
            return
        with LOCAL_STATE_LOCK:
            self._write_json_file(OWN_LOCATION_PATH, payload)
        self._send_json(payload)

    def _send_ping_schedule(self) -> None:
        config = self._read_json_file(PING_SCHEDULE_PATH, {"enabled": False})
        progress = self._read_json_file(PING_PROGRESS_PATH, {})
        if not isinstance(config, dict):
            config = {"enabled": False}
        if not isinstance(progress, dict) or progress.get("scheduleId") != config.get("scheduleId"):
            progress = {}
        self._send_json({"config": config, "progress": progress})

    def _save_ping_schedule(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 4096:
            self.send_error(413, "Invalid schedule size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            action = str(source.get("action", ""))
            if action == "cancel":
                current = self._read_json_file(PING_SCHEDULE_PATH, {})
                current = current if isinstance(current, dict) else {}
                payload = {**current, "enabled": False, "cancelledAt": int(time.time())}
            elif action == "start":
                interval = int(source.get("intervalMinutes"))
                count = int(source.get("count"))
                heading = float(source.get("heading", 0)) % 360
                if not 15 <= interval <= 1440:
                    raise ValueError("interval must be 15–1440 minutes")
                if not 1 <= count <= 24:
                    raise ValueError("count must be 1–24")
                now = int(time.time())
                payload = {
                    "scheduleId": str(time.time_ns()), "enabled": True,
                    "intervalMinutes": interval, "count": count,
                    "heading": round(heading, 1), "createdAt": now,
                }
            else:
                raise ValueError("action must be start or cancel")
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self.send_error(400, f"Invalid Ping schedule: {error}")
            return
        with LOCAL_STATE_LOCK:
            self._write_json_file(PING_SCHEDULE_PATH, payload)
        self._send_json({"config": payload, "progress": {}})

    def _send_lora_message(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 4096:
            self.send_error(413, "Invalid message size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            action = str(source.get("action", "text"))
            text = str(source.get("text", "")).strip()
            channel = int(source.get("channel"))
            destination = str(source.get("destination", "^all"))
            public_key = str(source.get("publicKey", "")).strip()
            if action not in {"text", "position", "trace"}:
                raise ValueError("invalid LoRa action")
            if action == "text" and (not text or len(text.encode("utf-8")) > 228):
                raise ValueError("message must contain 1–228 UTF-8 bytes")
            if not 0 <= channel <= 7:
                raise ValueError("channel must be 0–7")
            if destination != "^all" and not (len(destination) == 9 and destination.startswith("!") and all(c in "0123456789abcdefABCDEF" for c in destination[1:])):
                raise ValueError("invalid destination")
            if action != "text" and destination == "^all":
                raise ValueError("position and trace requests require a node destination")
            body = json.dumps({"action": action, "text": text, "channel": channel, "destination": destination, "publicKey": public_key}, ensure_ascii=False, separators=(",", ":")).encode()
            request = Request(LORA_SEND_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=25) as response:
                result = json.loads(response.read())
            if not isinstance(result, dict) or not result.get("ok"):
                raise ValueError(str(result.get("error", "LoRa sender rejected the message")) if isinstance(result, dict) else "invalid LoRa sender response")
            if action == "text":
                row = {
                    "ts": int(time.time()),
                    "event": "tx",
                    "from": "self",
                    "to": destination,
                    "channel": channel,
                    "text": text,
                    "id": uint32(result.get("packetId", result.get("id"))),
                }
                try:
                    SERVER_SENT_MESSAGES_PATH.parent.mkdir(parents=True, exist_ok=True)
                    encoded = (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                    with SERVER_SENT_MESSAGES_LOCK:
                        new_file = not SERVER_SENT_MESSAGES_PATH.exists()
                        with SERVER_SENT_MESSAGES_PATH.open("ab") as stream:
                            stream.write(encoded)
                        if new_file:
                            os.chmod(SERVER_SENT_MESSAGES_PATH, 0o600)
                        if SERVER_SENT_MESSAGES_PATH.stat().st_size > MAX_SERVER_SENT_MESSAGES_BYTES:
                            lines = SERVER_SENT_MESSAGES_PATH.read_bytes().splitlines()[-1000:]
                            temporary = SERVER_SENT_MESSAGES_PATH.with_suffix(".tmp")
                            temporary.write_bytes(b"\n".join(lines) + b"\n")
                            os.chmod(temporary, 0o600)
                            os.replace(temporary, SERVER_SENT_MESSAGES_PATH)
                except OSError:
                    # The packet has already been accepted by the radio sender.
                    # Never report a send failure merely because local history
                    # could not be written: that could provoke a duplicate TX.
                    result["archiveWarning"] = "outgoing message history was not written"
        except HTTPError as error:
            try:
                detail = json.loads(error.read())
            except (ValueError, json.JSONDecodeError):
                detail = {"ok": False, "stage": "sender", "error": str(error)}
            if not isinstance(detail, dict):
                detail = {"ok": False, "stage": "sender", "error": str(detail)}
            detail.setdefault("ok", False)
            self._send_json(detail, status=error.code if 400 <= error.code < 600 else 503)
            return
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, URLError, TimeoutError, OSError) as error:
            self._send_json({"ok": False, "error": f"LoRa send failed: {error}"}, status=503)
            return
        self._send_json(result)

    def _send_mqtt_state(self) -> None:
        onemesh = ONEMESH_CHAT.snapshot()
        self._send_json({
            "status": MQTT_CHAT.public_status(),
            "messages": MQTT_CHAT.messages(),
            "onemesh": onemesh["status"],
            "onemeshMessages": onemesh["messages"],
        })

    def _mqtt_config(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 8192:
            self.send_error(413, "Invalid MQTT configuration size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            if not isinstance(source, dict):
                raise ValueError("configuration must be an object")
            if source.get("action") == "disconnect":
                MQTT_CHAT.disconnect()
            elif source.get("action") == "save":
                MQTT_CHAT.save(source, enabled=False)
            elif source.get("action") == "connect":
                MQTT_CHAT.configure(source)
            else:
                raise ValueError("action must be save, connect or disconnect")
            onemesh = ONEMESH_CHAT.snapshot()
            self._send_json({
                "status": MQTT_CHAT.public_status(),
                "messages": MQTT_CHAT.messages(),
                "onemesh": onemesh["status"],
                "onemeshMessages": onemesh["messages"],
            })
        except (TypeError, ValueError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._send_json({"error": str(error), "status": MQTT_CHAT.public_status()}, 400)

    def _send_radio_profile_memory(self) -> None:
        with LOCAL_STATE_LOCK:
            source = self._read_json_file(RADIO_PROFILE_MEMORY_PATH, {"profiles": {}})
        profiles = source.get("profiles", {}) if isinstance(source, dict) else {}
        public = {}
        for name in ("LONG_FAST", "MEDIUM_FAST"):
            row = profiles.get(name, {}) if isinstance(profiles, dict) else {}
            snapshot = row.get("snapshot", {}) if isinstance(row, dict) else {}
            radio = snapshot.get("radio", {}) if isinstance(snapshot, dict) else {}
            if isinstance(radio, dict) and isinstance(radio.get("lora"), dict):
                public[name] = {"savedAt": row.get("savedAt"), "lora": radio["lora"]}
        self._send_json({"profiles": public})

    def _save_radio_profile_memory(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 2 * 1024 * 1024:
            self.send_error(413, "Invalid radio profile snapshot size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            profile = str(source.get("profile", ""))
            snapshot = source.get("snapshot")
            if profile not in {"LONG_FAST", "MEDIUM_FAST"} or not isinstance(snapshot, dict):
                raise ValueError("profile must be LONG_FAST or MEDIUM_FAST")
            radio = snapshot.get("radio")
            if not isinstance(radio, dict) or not isinstance(radio.get("lora"), dict):
                raise ValueError("snapshot must contain radio.lora")
            with LOCAL_STATE_LOCK:
                memory = self._read_json_file(RADIO_PROFILE_MEMORY_PATH, {"profiles": {}})
                if not isinstance(memory, dict):
                    memory = {"profiles": {}}
                profiles = memory.setdefault("profiles", {})
                profiles[profile] = {"savedAt": int(time.time()), "snapshot": snapshot}
                RADIO_PROFILE_MEMORY_PATH.parent.mkdir(parents=True, exist_ok=True)
                temporary = RADIO_PROFILE_MEMORY_PATH.with_suffix(".tmp")
                temporary.write_text(json.dumps(memory, ensure_ascii=False, separators=(",", ":")) + "\n")
                os.chmod(temporary, 0o600)
                os.replace(temporary, RADIO_PROFILE_MEMORY_PATH)
            self._send_json({"ok": True, "profile": profile})
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._send_json({"error": str(error)}, 400)

    def _mqtt_publish(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 8192:
            self.send_error(413, "Invalid MQTT message size")
            return
        try:
            source = json.loads(self.rfile.read(length))
            if not isinstance(source, dict):
                raise ValueError("message must be an object")
            row = MQTT_CHAT.publish(str(source.get("text", "")), str(source.get("sender", "")), str(source.get("senderId", "")))
            self._send_json({"message": row})
        except (TypeError, ValueError, RuntimeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._send_json({"error": str(error)}, 400)

    def _send_link_quality(self) -> None:
        now = int(time.time())
        cutoff = now - LINK_QUALITY_WINDOW_SECONDS
        first_bin = cutoff - cutoff % LINK_QUALITY_BIN_SECONDS
        bins: dict[int, dict[str, object]] = {}
        try:
            lines = LINK_QUALITY_PATH.read_text().splitlines()
        except (FileNotFoundError, PermissionError, OSError):
            lines = []
        for line in lines:
            try:
                row = json.loads(line)
                timestamp = int(row.get("ts", 0))
                if timestamp < cutoff or timestamp > now + 60:
                    continue
                start = timestamp - timestamp % LINK_QUALITY_BIN_SECONDS
                bucket = bins.setdefault(start, {
                    "rssis": [], "snrs": [], "direct_rssis": [], "direct_snrs": [],
                    "nodes": set(), "packets": 0, "direct": 0,
                })
                bucket["packets"] = int(bucket["packets"]) + 1
                sender = int(row.get("from", 0) or 0)
                if sender:
                    bucket["nodes"].add(sender)
                rssi, snr = row.get("rssi"), row.get("snr")
                if isinstance(rssi, (int, float)) and math.isfinite(rssi):
                    bucket["rssis"].append(float(rssi))
                if isinstance(snr, (int, float)) and math.isfinite(snr):
                    bucket["snrs"].append(float(snr))
                if row.get("hops") == 0:
                    bucket["direct"] = int(bucket["direct"]) + 1
                    if isinstance(rssi, (int, float)) and math.isfinite(rssi):
                        bucket["direct_rssis"].append(float(rssi))
                    if isinstance(snr, (int, float)) and math.isfinite(snr):
                        bucket["direct_snrs"].append(float(snr))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

        def midpoint(values: object) -> float | None:
            return round(float(statistics.median(values)), 2) if isinstance(values, list) and values else None

        output = []
        for start in range(first_bin, now + LINK_QUALITY_BIN_SECONDS, LINK_QUALITY_BIN_SECONDS):
            bucket = bins.get(start, {})
            output.append({
                "ts": start,
                "packets": int(bucket.get("packets", 0)),
                "direct": int(bucket.get("direct", 0)),
                "nodes": len(bucket.get("nodes", set())),
                "rssi": midpoint(bucket.get("rssis")),
                "snr": midpoint(bucket.get("snrs")),
                "directRssi": midpoint(bucket.get("direct_rssis")),
                "directSnr": midpoint(bucket.get("direct_snrs")),
            })
        body = json.dumps({
            "generatedAt": now,
            "windowSeconds": LINK_QUALITY_WINDOW_SECONDS,
            "binSeconds": LINK_QUALITY_BIN_SECONDS,
            "bins": output,
        }, separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_device_request(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else None
        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower()
            in {
                "accept",
                "content-type",
            }
        }
        request = Request(
            f"{DEVICE_URL}{self.path}",
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            response = urlopen(request, timeout=65)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError) as error:
            self.send_error(502, f"Meshtastic device unavailable: {error}")
            return

        response_body = response.read()
        if self.command == "GET" and urlsplit(self.path).path == "/nightbot.sent.jsonl" and response.status == 200:
            progress = self._read_json_file(PING_PROGRESS_PATH, {})
            sent_pings = progress.get("sentPings", []) if isinstance(progress, dict) else []
            scheduled_rows = []
            if isinstance(sent_pings, list):
                for item in sent_pings:
                    if not isinstance(item, dict):
                        continue
                    sent_at = int(item.get("sentAt", 0) or 0)
                    if sent_at <= 0:
                        continue
                    scheduled_rows.append(json.dumps({
                        "ts": sent_at, "event": "tx", "from": "self", "to": "^all",
                        "channel": int(item.get("channel", 3) or 3), "text": "Ping",
                        "id": int(item.get("packetId", 0) or 0), "automatic": True,
                    }, ensure_ascii=False, separators=(",", ":")))
            if scheduled_rows:
                response_body = response_body.rstrip(b"\n") + b"\n" + ("\n".join(scheduled_rows) + "\n").encode()
        self.send_response(response.status)
        for name in ("Content-Type",):
            value = response.headers.get(name)
            if value:
                self.send_header(name, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(response_body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if urlsplit(self.path).path == "/delivery-status.json":
            try:
                lines = DELIVERY_STATUS_PATH.read_text().splitlines()[-1000:]
            except (FileNotFoundError, PermissionError, OSError):
                lines = []
            events = []
            for line in lines:
                try:
                    row = json.loads(line)
                    if isinstance(row, dict):
                        events.append(row)
                except (ValueError, json.JSONDecodeError):
                    continue
            self._send_json({"events": events})
            return
        if urlsplit(self.path).path == "/server-sent.jsonl":
            try:
                body = SERVER_SENT_MESSAGES_PATH.read_bytes()
            except (FileNotFoundError, PermissionError, OSError):
                body = b""
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            return
        if urlsplit(self.path).path == "/rnode/announces":
            self._proxy_rnode_announces()
            return
        if urlsplit(self.path).path.startswith("/reticulum/api"):
            self._proxy_reticulum_bridge()
            return
        if urlsplit(self.path).path.startswith("/meshcore/api"):
            self._proxy_meshcore_bridge()
            return
        if urlsplit(self.path).path in {"/", "/index.html"}:
            if self._device_is_meshcore():
                self._send_meshcore_return_page()
                return
            if self._device_is_rnode():
                self._send_rnode_page()
                return
        if urlsplit(self.path).path == "/runtime-capabilities.json":
            self._send_json({
                "backend": "orangepi",
                "serverStorage": True,
                "mqtt": True,
                "onemesh": True,
                "pingScheduler": True,
                "linkQuality": True,
            })
            return
        if urlsplit(self.path).path == "/mqtt-chat.json":
            self._send_mqtt_state()
            return
        if urlsplit(self.path).path == "/radio-profile-memory.json":
            self._send_radio_profile_memory()
            return
        if urlsplit(self.path).path == "/own-location.json":
            self._send_own_location()
            return
        if urlsplit(self.path).path == "/ping-schedule.json":
            self._send_ping_schedule()
            return
        if urlsplit(self.path).path == "/link-quality.json":
            self._send_link_quality()
            return
        if urlsplit(self.path).path == "/aim-measurements.json":
            self._send_aim_measurements()
            return
        if urlsplit(self.path).path == "/node-cache.json":
            self._send_node_cache()
            return
        if urlsplit(self.path).path == "/reset-ui":
            body = b"""<!doctype html><meta charset=utf-8><title>Reset BarbieNode UI</title>
<p>Resetting the obsolete browser cache...</p><script>
async function reset() {
  if ('serviceWorker' in navigator) {
    const registrations = await navigator.serviceWorker.getRegistrations();
    await Promise.all(registrations.map((item) => item.unregister()));
  }
  if ('caches' in window) {
    const names = await caches.keys();
    await Promise.all(names.map((name) => caches.delete(name)));
  }
  localStorage.clear();
  sessionStorage.clear();
  location.replace('/?ui=barbienode-20260926');
}
reset();
</script>"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        if self._is_device_request():
            self._proxy_device_request()
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if urlsplit(self.path).path == "/rnode/prepare-boot":
            self._prepare_rnode_boot()
            return
        if urlsplit(self.path).path.startswith("/reticulum/api"):
            self._proxy_reticulum_bridge()
            return
        if urlsplit(self.path).path.startswith("/meshcore/api"):
            self._proxy_meshcore_bridge()
            return
        if urlsplit(self.path).path == "/meshcore/boot/meshtastic":
            self._boot_meshcore_to_meshtastic()
            return
        if urlsplit(self.path).path == "/meshcore/restart":
            self._restart_meshcore()
            return
        if urlsplit(self.path).path == "/lora-send.json":
            self._send_lora_message()
            return
        if urlsplit(self.path).path == "/mqtt-chat/config":
            self._mqtt_config()
            return
        if urlsplit(self.path).path == "/mqtt-chat/publish":
            self._mqtt_publish()
            return
        if urlsplit(self.path).path == "/radio-profile-memory.json":
            self._save_radio_profile_memory()
            return
        if urlsplit(self.path).path == "/own-location.json":
            self._save_own_location()
            return
        if urlsplit(self.path).path == "/ping-schedule.json":
            self._save_ping_schedule()
            return
        if urlsplit(self.path).path == "/aim-measurements.json":
            self._save_aim_measurements()
            return
        if urlsplit(self.path).path == "/node-cache.json":
            self._save_node_cache()
            return
        if self._is_device_request() or urlsplit(self.path).path in {"/node-cache.json", "/aim-measurements.json"}:
            self._proxy_device_request()
            return
        self.send_error(405)

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self._is_device_request() or urlsplit(self.path).path == "/node-cache.json":
            self._proxy_device_request()
            return
        self.send_error(405)

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        if self._is_device_request():
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Pragma", "no-cache")
        else:
            # Asset names are stable across deployments, so every browser must
            # revalidate them instead of keeping an obsolete UI for 90 days.
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()


def main() -> None:
    def clock_sync_loop() -> None:
        delay = 5
        last_applied = 0.0
        while True:
            time.sleep(delay)
            epoch = int(time.time())
            if epoch < 1_700_000_000:
                delay = CLOCK_SYNC_RETRY_SECONDS
                continue
            should_apply = time.monotonic() - last_applied >= CLOCK_SYNC_INTERVAL_SECONDS
            try:
                with urlopen(f"{DEVICE_URL}/clock/status", timeout=10) as response:
                    status = json.loads(response.read())
                board_epoch = int(status.get("time", 0))
                should_apply = should_apply or board_epoch < 1_700_000_000 or abs(board_epoch - epoch) > 5
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError):
                delay = CLOCK_SYNC_RETRY_SECONDS
                continue
            if not should_apply:
                delay = CLOCK_SYNC_RETRY_SECONDS
                continue
            request = Request(
                f"{DEVICE_URL}/clock/sync",
                data=str(epoch).encode("ascii"),
                headers={"Content-Type": "text/plain"},
                method="POST",
            )
            try:
                with urlopen(request, timeout=65) as response:
                    payload = json.loads(response.read())
                if not payload.get("ok"):
                    raise ValueError("device did not confirm clock update")
                print(
                    "Board clock synchronized from Orange Pi; "
                    f"corrected archive records: {payload.get('corrected_archive_records', 0)}",
                    flush=True,
                )
                last_applied = time.monotonic()
                delay = CLOCK_SYNC_RETRY_SECONDS
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as error:
                print(f"Board clock synchronization pending: {error}", flush=True)
                delay = CLOCK_SYNC_RETRY_SECONDS

    threading.Thread(target=clock_sync_loop, name="board-clock-sync", daemon=True).start()
    server = ThreadingHTTPServer((BIND_HOST, PORT), SPAHandler)
    print(
        f"Meshtastic Web listening on http://{BIND_HOST}:{PORT}; "
        f"proxying Meshtastic device endpoints to {DEVICE_URL}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
