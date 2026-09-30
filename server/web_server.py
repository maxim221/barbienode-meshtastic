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
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
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
AIM_MEASUREMENTS_PATH = Path(os.environ.get("AIM_MEASUREMENTS_PATH", "/var/lib/barbienode-web/aim-measurements.json"))
AIM_MEASUREMENTS_LOCK = threading.Lock()
MAX_AIM_MEASUREMENTS_BYTES = 1024 * 1024
LINK_QUALITY_PATH = Path(os.environ.get("LINK_QUALITY_PATH", "/var/lib/barbienode-link-quality/events.jsonl"))
LINK_QUALITY_WINDOW_SECONDS = 24 * 60 * 60
LINK_QUALITY_BIN_SECONDS = 15 * 60
OWN_LOCATION_PATH = Path(os.environ.get("OWN_LOCATION_PATH", "/var/lib/barbienode-web/own-location.json"))
PING_SCHEDULE_PATH = Path(os.environ.get("PING_SCHEDULE_PATH", "/var/lib/barbienode-web/ping-schedule.json"))
PING_PROGRESS_PATH = Path(os.environ.get("PING_PROGRESS_PATH", "/var/lib/barbienode-link-quality/ping-progress.json"))
LOCAL_STATE_LOCK = threading.Lock()
MQTT_CONFIG_PATH = Path(os.environ.get("MQTT_CONFIG_PATH", "/var/lib/barbienode-web/mqtt-config.json"))
MQTT_MESSAGES_PATH = Path(os.environ.get("MQTT_MESSAGES_PATH", "/var/lib/barbienode-web/mqtt-messages.jsonl"))
RADIO_PROFILE_MEMORY_PATH = Path(os.environ.get("RADIO_PROFILE_MEMORY_PATH", "/var/lib/barbienode-web/radio-profile-memory.json"))
LORA_SEND_URL = os.environ.get("LORA_SEND_URL", "http://127.0.0.1:8765/send")
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
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        NODE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = NODE_CACHE_PATH.with_suffix(".tmp")
        with NODE_CACHE_LOCK:
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
            body = json.dumps({"action": action, "text": text, "channel": channel, "destination": destination}, ensure_ascii=False, separators=(",", ":")).encode()
            request = Request(LORA_SEND_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=25) as response:
                result = json.loads(response.read())
            if not isinstance(result, dict) or not result.get("ok"):
                raise ValueError(str(result.get("error", "LoRa sender rejected the message")) if isinstance(result, dict) else "invalid LoRa sender response")
        except HTTPError as error:
            try:
                detail = json.loads(error.read()).get("error", str(error))
            except (ValueError, json.JSONDecodeError):
                detail = str(error)
            self._send_json({"ok": False, "error": f"LoRa sender unavailable: {detail}"}, status=503)
            return
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, URLError, TimeoutError, OSError) as error:
            self._send_json({"ok": False, "error": f"LoRa send failed: {error}"}, status=503)
            return
        self._send_json(result)

    def _send_mqtt_state(self) -> None:
        self._send_json({"status": MQTT_CHAT.public_status(), "messages": MQTT_CHAT.messages()})

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
            self._send_json({"status": MQTT_CHAT.public_status(), "messages": MQTT_CHAT.messages()})
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
