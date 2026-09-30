#!/usr/bin/env python3
"""Persist passive Meshtastic RF quality observations for the local dashboard."""

from __future__ import annotations

import json
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
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            self._reply(400, {"ok": False, "error": str(error)})
            return
        request = {"action": action, "text": text, "channel": channel, "destination": destination, "done": threading.Event()}
        send_queue.put(request)
        if not request["done"].wait(20):
            self._reply(504, {"ok": False, "error": "LoRa sender did not respond in time"})
            return
        result = request.get("result", {"ok": False, "error": "unknown send error"})
        self._reply(200 if result.get("ok") else 503, result)

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
            packet = interface.sendText(
                request["text"], destinationId=request["destination"],
                wantAck=request["destination"] != "^all", channelIndex=channel_index,
            )
        packet_id = int(packet.get("id", 0) if isinstance(packet, dict) else getattr(packet, "id", 0) or 0)
        request["result"] = {"ok": True, "packetId": packet_id, "action": request["action"], "channel": channel_index, "channelName": channel_name}
    except Exception as error:  # Meshtastic transport errors vary by release.
        request["result"] = {"ok": False, "error": str(error)}
    finally:
        request["done"].set()
        send_queue.task_done()


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
        sent_pings.append({"sentAt": sent_at, "packetId": packet_id})
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


def on_receive(packet: dict, interface: TCPInterface) -> None:
    if time.time() < ready_at:
        return
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
    try:
        with lock:
            retained = []
            for line in EVENTS_PATH.read_text().splitlines():
                try:
                    row = json.loads(line)
                    if int(row.get("ts", 0)) >= cutoff:
                        retained.append(json.dumps(row, separators=(",", ":")))
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
            temporary = EVENTS_PATH.with_suffix(".tmp")
            temporary.write_text("\n".join(retained) + ("\n" if retained else ""))
            os.replace(temporary, EVENTS_PATH)
    except FileNotFoundError:
        return


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
