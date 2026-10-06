#!/usr/bin/env python3
"""Persist passive Meshtastic RF quality observations for the local dashboard."""

from __future__ import annotations

import json
import base64
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

from meshtastic.tcp_interface import TCPInterface
from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pubsub import pub


DEVICE_HOST = os.environ.get("MESHTASTIC_HOST", "192.168.1.31")
EVENTS_PATH = Path(os.environ.get("LINK_QUALITY_PATH", "/var/lib/barbienode-link-quality/events.jsonl"))
DELIVERIES_PATH = Path(os.environ.get("DELIVERY_STATUS_PATH", "/var/lib/barbienode-link-quality/deliveries.jsonl"))
PING_SCHEDULE_PATH = Path(os.environ.get("PING_SCHEDULE_PATH", "/var/lib/barbienode-web/ping-schedule.json"))
PING_SCHEDULE_URL = os.environ.get("PING_SCHEDULE_URL", "").strip()
PING_PROGRESS_PATH = Path(os.environ.get("PING_PROGRESS_PATH", "/var/lib/barbienode-link-quality/ping-progress.json"))
PING_GUARD_PATH = Path(os.environ.get("PING_GUARD_PATH", "/var/lib/barbienode-link-quality/ping-guard.json"))
RETENTION_SECONDS = 48 * 60 * 60
WARMUP_SECONDS = 20
MIN_PING_INTERVAL_SECONDS = 15 * 60
SEND_GATEWAY_HOST = os.environ.get("SEND_GATEWAY_HOST", "127.0.0.1")
SEND_GATEWAY_PORT = int(os.environ.get("SEND_GATEWAY_PORT", "8765"))

lock = threading.Lock()
delivery_lock = threading.Lock()
pending_delivery_lock = threading.Lock()
pending_destinations: dict[int, str] = {}
send_queue: queue.Queue[dict] = queue.Queue()
ready_at = float("inf")
own_node = 0


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
        return {}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(temporary, path)


def read_ping_schedule() -> dict:
    """Read the web-owned schedule without crossing its DynamicUser state boundary."""
    if PING_SCHEDULE_URL:
        try:
            with urlopen(PING_SCHEDULE_URL, timeout=5) as response:
                value = json.load(response)
            if isinstance(value, dict):
                config = value.get("config", value)
                return config if isinstance(config, dict) else {}
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    return read_json(PING_SCHEDULE_PATH)


def ping_channel_index(interface: TCPInterface) -> int | None:
    for index, channel in enumerate(getattr(interface.localNode, "channels", ())):
        settings = getattr(channel, "settings", None)
        if getattr(settings, "name", "") == "Ping" and int(getattr(channel, "role", 0)) != 0:
            return index
    return None


class LocalSendHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/deliveries":
            self.send_error(404)
            return
        try:
            lines = DELIVERIES_PATH.read_text().splitlines()[-1000:]
        except (FileNotFoundError, PermissionError, OSError):
            lines = []
        rows = []
        for line in lines:
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except (ValueError, json.JSONDecodeError):
                continue
        self._reply(200, {"ok": True, "events": rows})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/send":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 4096:
                raise ValueError("invalid request size")
            source = json.loads(self.rfile.read(length))
            action = str(source.get("action", "text"))
            text = str(source.get("text", "")).strip()
            channel = int(source.get("channel"))
            destination = str(source.get("destination", "^all"))
            public_key_b64 = str(source.get("publicKey", "")).strip()
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
            public_key = b""
            if public_key_b64:
                public_key = base64.b64decode(public_key_b64, validate=True)
                if len(public_key) != 32 or not any(public_key):
                    raise ValueError("publicKey must contain exactly 32 non-zero bytes")
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._reply(400, {"ok": False, "stage": "validation", "transmitted": False, "error": str(error)})
            return
        request = {"action": action, "text": text, "channel": channel, "destination": destination, "publicKey": public_key, "done": threading.Event()}
        send_queue.put(request)
        if not request["done"].wait(20):
            self._reply(504, {"ok": False, "error": "LoRa sender did not respond in time"})
            return
        result = request.get("result", {"ok": False, "error": "unknown send error"})
        self._reply(200 if result.get("ok") else 409 if result.get("stage") == "preflight" else 503, result)

    def _reply(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def process_send_queue(interface: TCPInterface) -> None:
    try:
        request = send_queue.get_nowait()
    except queue.Empty:
        return
    try:
        channel_index = int(request["channel"])
        channels = tuple(getattr(interface.localNode, "channels", ()))
        if channel_index >= len(channels) or int(getattr(channels[channel_index], "role", 0)) == 0:
            raise ValueError("Выбранный канал выключен на плате.")
        channel_name = str(getattr(getattr(channels[channel_index], "settings", None), "name", ""))
        if request["action"] == "text" and request["text"].casefold() == "ping" and (request["destination"] != "^all" or channel_name != "Ping"):
            raise ValueError("Ping разрешён только широковещательно в точном канале Ping.")
        if request["action"] == "position":
            packet = interface.sendData(mesh_pb2.Position(), destinationId=request["destination"], portNum=portnums_pb2.PortNum.POSITION_APP, wantAck=True, wantResponse=True, channelIndex=channel_index)
        elif request["action"] == "trace":
            local_config = getattr(getattr(interface, "localNode", None), "localConfig", None)
            lora_config = getattr(local_config, "lora", None)
            configured_hops = int(getattr(lora_config, "hop_limit", 0) or 0)
            packet = interface.sendData(mesh_pb2.RouteDiscovery(), destinationId=request["destination"], portNum=portnums_pb2.PortNum.TRACEROUTE_APP, wantAck=True, wantResponse=True, channelIndex=channel_index, hopLimit=configured_hops or None)
        else:
            send_options = {}
            if request["destination"] != "^all":
                destination_id = request["destination"].lower()
                public_key = request.get("publicKey", b"")
                if not public_key:
                    node = getattr(interface, "nodes", {}).get(destination_id, {})
                    user = node.get("user", {}) if isinstance(node, dict) else {}
                    public_key = user.get("publicKey", user.get("public_key", b"")) if isinstance(user, dict) else b""
                if isinstance(public_key, list):
                    public_key = bytes(public_key)
                elif isinstance(public_key, str):
                    try:
                        public_key = base64.b64decode(public_key, validate=True)
                    except ValueError:
                        public_key = b""
                if not isinstance(public_key, (bytes, bytearray)) or len(public_key) != 32 or not any(public_key):
                    request["result"] = {"ok": False, "stage": "preflight", "transmitted": False, "errorCode": 39, "error": "Личная отправка остановлена до эфира: у отправителя нет 32-байтного публичного PKI-ключа получателя."}
                    return
                destination = request["destination"]
                def on_ack_nak(response: dict) -> None:
                    record_delivery_response(response, destination)
                send_options = {"pkiEncrypted": True, "publicKey": bytes(public_key), "onResponse": on_ack_nak, "onResponseAckPermitted": True}
            packet = interface.sendData(
                request["text"].encode("utf-8"), destinationId=request["destination"],
                portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                wantAck=request["destination"] != "^all", channelIndex=channel_index,
                **send_options,
            )
        packet_id = int(packet.get("id", 0) if isinstance(packet, dict) else getattr(packet, "id", 0) or 0)
        if request["action"] == "text" and request["destination"] != "^all":
            with pending_delivery_lock:
                pending_destinations[packet_id] = request["destination"].lower()
            append_delivery({"ts": int(time.time()), "packetId": packet_id, "destination": request["destination"], "status": "accepted", "evidence": "Пакет принят локальным отправителем и поставлен в очередь с запросом ACK."})
        request["result"] = {"ok": True, "packetId": packet_id, "action": request["action"], "channel": channel_index, "channelName": channel_name, "stage": "accepted", "transmitted": None}
    except Exception as error:  # Meshtastic transport errors vary by release.
        if not request.get("result"):
            request["result"] = {"ok": False, "stage": "sender", "transmitted": False, "error": str(error)}
    finally:
        request["done"].set()
        send_queue.task_done()


def ensure_transport_alive(interface: TCPInterface) -> None:
    """Let systemd recreate the client after the Meshtastic reader exits.

    TCPInterface's reader thread terminates on a connection reset.  Its
    heartbeat may then fail to reconnect while the board is still rebooting,
    leaving the HTTP gateway alive but with nobody able to service send_queue.
    Raising here makes the process fail closed; Restart=always establishes a
    fresh interface once the board's TCP API is available again.
    """
    reader = getattr(interface, "_rxThread", None)
    if reader is not None and not reader.is_alive():
        raise ConnectionError("Meshtastic TCP reader stopped")


def process_ping_schedule(interface: TCPInterface) -> None:
    schedule = read_ping_schedule()
    if not schedule.get("enabled"):
        return
    schedule_id = str(schedule.get("scheduleId", ""))
    interval = int(schedule.get("intervalMinutes", 0) or 0) * 60
    count = int(schedule.get("count", 0) or 0)
    if not schedule_id or interval < MIN_PING_INTERVAL_SECONDS or not 1 <= count <= 24:
        return
    progress = read_json(PING_PROGRESS_PATH)
    if progress.get("scheduleId") != schedule_id:
        progress = {"scheduleId": schedule_id, "sent": 0, "nextAt": int(schedule.get("createdAt", time.time()))}
    sent = int(progress.get("sent", 0) or 0)
    if sent >= count:
        if not progress.get("completedAt"):
            progress["completedAt"] = int(time.time())
            write_json(PING_PROGRESS_PATH, progress)
        return
    now = int(time.time())
    if now < int(progress.get("nextAt", 0) or 0):
        return
    guard = read_json(PING_GUARD_PATH)
    last_sent = int(guard.get("lastSentAt", 0) or 0)
    if now - last_sent < MIN_PING_INTERVAL_SECONDS:
        progress["nextAt"] = last_sent + MIN_PING_INTERVAL_SECONDS
        write_json(PING_PROGRESS_PATH, progress)
        return
    channel_index = ping_channel_index(interface)
    if channel_index is None:
        progress["error"] = "Точный активный канал Ping не найден; передача не выполнялась."
        write_json(PING_PROGRESS_PATH, progress)
        return
    try:
        packet = interface.sendText("Ping", destinationId="^all", wantAck=False, channelIndex=channel_index)
        packet_id = int(packet.get("id", 0) if isinstance(packet, dict) else getattr(packet, "id", 0) or 0)
        sent += 1
        sent_at = int(time.time())
        sent_pings = progress.get("sentPings", [])
        if not isinstance(sent_pings, list):
            sent_pings = []
        sent_pings.append({"sentAt": sent_at, "packetId": packet_id, "channel": channel_index})
        progress.update({"sent": sent, "lastSentAt": sent_at, "lastPacketId": packet_id, "nextAt": sent_at + interval, "sentPings": sent_pings[-24:]})
        progress.pop("error", None)
        if sent >= count:
            progress["completedAt"] = sent_at
        write_json(PING_PROGRESS_PATH, progress)
        write_json(PING_GUARD_PATH, {"lastSentAt": sent_at, "scheduleId": schedule_id, "packetId": packet_id})
    except Exception as error:  # Meshtastic transport errors vary by release.
        progress["error"] = f"Не удалось отправить Ping: {error}"
        write_json(PING_PROGRESS_PATH, progress)


def number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
        return result if result == result else None
    except (TypeError, ValueError):
        return None


def append_event(row: dict[str, object]) -> None:
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with lock, EVENTS_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, separators=(",", ":")) + "\n")


def append_delivery(row: dict[str, object]) -> None:
    DELIVERIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with delivery_lock, DELIVERIES_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def delivery_from_packet(packet: dict, destination: str = "") -> dict[str, object] | None:
    decoded = packet.get("decoded", {})
    if not isinstance(decoded, dict):
        return None
    request_id = int(decoded.get("requestId", 0) or 0)
    routing = decoded.get("routing", {})
    if not request_id or not isinstance(routing, dict):
        return None
    if not destination:
        with pending_delivery_lock:
            destination = pending_destinations.get(request_id, "")
    if not destination:
        return None
    destination = destination.lower()
    source = str(packet.get("fromId", "")).lower()
    reason = str(routing.get("errorReason", "NONE"))
    raw = routing.get("raw")
    code = int(getattr(raw, "error_reason", 0) or 0) if raw is not None else 0
    delivered = reason == "NONE" or not reason
    # The Python client also exposes a local/implicit ACK from our own board.
    # It confirms queue/radio handling, not receipt by the intended peer.
    if delivered and source != destination:
        return None
    with pending_delivery_lock:
        pending_destinations.pop(request_id, None)
    return {
        "ts": int(time.time()), "packetId": request_id,
        "destination": destination,
        "status": "delivered" if delivered else "failed",
        "errorCode": code if not delivered else 0,
        "errorReason": reason,
        "from": source,
        "evidence": "Получен адресный routing ACK от получателя." if delivered else f"Получен routing NAK: {reason}.",
    }


def record_delivery_response(packet: dict, destination: str = "") -> None:
    row = delivery_from_packet(packet, destination)
    if row:
        append_delivery(row)


def on_receive(packet: dict, interface: TCPInterface) -> None:
    if time.time() < ready_at:
        return
    delivery = delivery_from_packet(packet)
    if delivery:
        append_delivery(delivery)
    sender = int(packet.get("from", 0) or 0)
    if not sender or sender == own_node or bool(packet.get("viaMqtt", False)):
        return
    hop_start = number(packet.get("hopStart"))
    hop_limit = number(packet.get("hopLimit"))
    hops = int(hop_start - hop_limit) if hop_start is not None and hop_limit is not None else None
    append_event({
        "ts": int(time.time()),
        "from": sender,
        "hops": hops,
        "rssi": number(packet.get("rxRssi")),
        "snr": number(packet.get("rxSnr")),
    })


def compact() -> None:
    cutoff = int(time.time()) - RETENTION_SECONDS
    for path, path_lock in ((EVENTS_PATH, lock), (DELIVERIES_PATH, delivery_lock)):
        try:
            with path_lock:
                retained = []
                for line in path.read_text().splitlines():
                    try:
                        row = json.loads(line)
                        if int(row.get("ts", 0)) >= cutoff:
                            retained.append(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                temporary = path.with_suffix(".tmp")
                temporary.write_text("\n".join(retained) + ("\n" if retained else ""))
                os.replace(temporary, path)
        except FileNotFoundError:
            continue


def main() -> int:
    global ready_at, own_node
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    pub.subscribe(on_receive, "meshtastic.receive")
    interface = TCPInterface(hostname=DEVICE_HOST, timeout=30)
    gateway = ThreadingHTTPServer((SEND_GATEWAY_HOST, SEND_GATEWAY_PORT), LocalSendHandler)
    gateway_thread = threading.Thread(target=gateway.serve_forever, name="lora-send-gateway", daemon=True)
    gateway_thread.start()
    try:
        own_node = int(getattr(getattr(interface, "myInfo", None), "my_node_num", 0) or 0)
        ready_at = time.time() + WARMUP_SECONDS
        next_compaction = time.monotonic() + 3600
        next_schedule_check = 0.0
        while True:
            ensure_transport_alive(interface)
            process_send_queue(interface)
            if time.monotonic() >= next_schedule_check:
                process_ping_schedule(interface)
                next_schedule_check = time.monotonic() + 5
            if time.monotonic() >= next_compaction:
                compact()
                next_compaction = time.monotonic() + 3600
            time.sleep(0.25)
    finally:
        gateway.shutdown()
        gateway.server_close()
        interface.close()


if __name__ == "__main__":
    raise SystemExit(main())
