#!/usr/bin/env python3
"""Local MeshCore companion bridge between BarbieNode and the Orange Pi UI."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from meshcore import EventType, MeshCore
from meshcore.packets import CommandType


LOG = logging.getLogger("barbienode.meshcore")
BIND_HOST = os.environ.get("MESHCORE_BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("MESHCORE_BRIDGE_PORT", "8766"))
BLE_ADDRESS = os.environ.get("MESHCORE_BLE_ADDRESS", "").strip() or None
TCP_HOST = os.environ.get("MESHCORE_TCP_HOST", "").strip() or None
TCP_PORT = int(os.environ.get("MESHCORE_TCP_PORT", "5000"))
WIFI_NAME = os.environ.get("MESHCORE_WIFI_NAME", "BarbieNode-MeshCore").strip() or "BarbieNode-MeshCore"
EXPECTED_NAME = os.environ.get("MESHCORE_EXPECTED_NAME", "BarbieNode").strip()
STATE_DIR = Path(os.environ.get("MESHCORE_STATE_DIR", "/var/lib/barbienode-meshcore"))
IDENTITY_PATH = STATE_DIR / "identity.json"
MESSAGES_PATH = STATE_DIR / "messages.jsonl"
READ_STATE_PATH = STATE_DIR / "read-state.json"
BOT_STATE_PATH = STATE_DIR / "bot-state.json"
CHANNEL_BACKUP_DIR = STATE_DIR / "channel-backups"
FIRMWARE_BACKUP_DIR = STATE_DIR / "firmware-backups"
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_MESSAGE_BYTES = 133
MAX_MESSAGES_RETURNED = 500
SENDER_PATTERN = re.compile(r"^([^:\n]{1,40}):\s(.*)$", re.DOTALL)
BOT_TRIGGER = "Ping"
BOT_LOCATION = "Бутырский"
BOT_REPLY = "🛜 Pong @[отправитель] N🐰 · Бутырский · RSSI · SNR (BarbieNode💅)"
BOT_CHANNEL = "#connections"
BOT_SENDER_COOLDOWN_SECONDS = 5 * 60
BOT_GLOBAL_WINDOW_SECONDS = 60 * 60
BOT_GLOBAL_LIMIT = 10
KNOWN_SCOPES = ("msk", "mow", "ru")


def json_safe(value):
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


class Bridge:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.mc: MeshCore | None = None
        self.connected = False
        self.connecting = False
        self.last_error = ""
        self.last_connected = 0
        self.ble_address = BLE_ADDRESS or ""
        self.self_info: dict = {}
        self.device_info: dict = {}
        self.default_scope = ""
        self.channels: list[dict] = []
        self.contacts: list[dict] = []
        self._subscriptions = []
        self._archive_lock = threading.Lock()
        self._read_lock = threading.Lock()
        self._bot_lock = threading.Lock()
        self._known_ids = deque(maxlen=5000)
        self._known_id_set: set[str] = set()
        self._pending_acks: dict[str, str] = {}
        self._early_acks = deque(maxlen=100)
        self._early_ack_set: set[str] = set()
        self._bot_state = self._load_bot_state()
        self._send_queue: asyncio.PriorityQueue | None = None
        self._send_sequence = 0
        self._send_worker_task: asyncio.Task | None = None
        self._load_known_ids()

    @staticmethod
    def _load_bot_state() -> dict:
        try:
            value = json.loads(BOT_STATE_PATH.read_text(encoding="utf-8"))
        except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
            value = {}
        return {
            "enabled": bool(value.get("enabled", False)) if isinstance(value, dict) else False,
            "handled": list(value.get("handled", []))[-1000:] if isinstance(value, dict) else [],
            "lastBySender": dict(value.get("lastBySender", {})) if isinstance(value, dict) else {},
            "globalReplies": list(value.get("globalReplies", [])) if isinstance(value, dict) else [],
            "lastReplyAt": int(value.get("lastReplyAt", 0) or 0) if isinstance(value, dict) else 0,
            "replyCount": int(value.get("replyCount", 0) or 0) if isinstance(value, dict) else 0,
        }

    def _save_bot_state(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        temporary = BOT_STATE_PATH.with_suffix(".tmp")
        temporary.write_text(json.dumps(self._bot_state, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, BOT_STATE_PATH)

    def _load_known_ids(self) -> None:
        try:
            lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()[-5000:]
        except (FileNotFoundError, PermissionError, OSError):
            return
        for line in lines:
            try:
                message_id = str(json.loads(line).get("id", ""))
            except (ValueError, json.JSONDecodeError):
                continue
            if message_id:
                self._remember_id(message_id)

    def _remember_id(self, message_id: str) -> None:
        if message_id in self._known_id_set:
            return
        if len(self._known_ids) == self._known_ids.maxlen:
            self._known_id_set.discard(self._known_ids[0])
        self._known_ids.append(message_id)
        self._known_id_set.add(message_id)

    @staticmethod
    def _transport_code(scope: str) -> str:
        value = str(scope or "").strip().lstrip("#").casefold()
        return hashlib.sha256(f"#{value}".encode("utf-8")).hexdigest()[:8] if value else ""

    def _scope_from_code(self, code: str) -> str:
        value = str(code or "").casefold()[:8]
        candidates = dict.fromkeys((self.default_scope, *KNOWN_SCOPES))
        return next((scope for scope in candidates if scope and self._transport_code(scope) == value), "")

    async def _channel_packet_metadata(self, timestamp: int, raw_text: str, payload: dict) -> dict:
        txt_hash = payload.get("txt_hash")
        if txt_hash is None:
            digest = hashlib.sha256(timestamp.to_bytes(4, "little", signed=False) + raw_text.encode("utf-8")).digest()
            txt_hash = int.from_bytes(digest[:4], "little", signed=False)
        logged = None
        try:
            parser = self.mc._reader.packet_parser if self.mc else None
            if parser:
                logged = await parser.findLogChannelMsg(int(txt_hash))
        except (AttributeError, TypeError, ValueError):
            LOG.debug("MeshCore packet metadata lookup unavailable", exc_info=True)
        logged = json_safe(logged or {})
        transport_code = str(logged.get("transport_code", "") or "").casefold()
        return {
            "snr": payload.get("SNR", logged.get("snr")),
            "rssi": payload.get("RSSI", logged.get("rssi")),
            "path": payload.get("path", logged.get("path", "")),
            "pathLength": payload.get("path_len", logged.get("path_len")),
            "pathHashMode": payload.get("path_hash_mode"),
            "pathHashSize": logged.get("path_hash_size"),
            "routeType": logged.get("route_typename"),
            "attempt": payload.get("attempt", logged.get("attempt")),
            "packetHash": f"{int(logged['pkt_hash']):08X}" if logged.get("pkt_hash") is not None else "",
            "localTextHash": f"{int(txt_hash):08X}",
            "transportCode": transport_code,
            "scope": self._scope_from_code(transport_code),
        }

    @staticmethod
    def _message_id(row: dict) -> str:
        parts = (
            str(row.get("direction", "")), str(row.get("kind", "")),
            str(row.get("channel", "")), str(row.get("contact", "")),
            str(row.get("timestamp", "")), str(row.get("text", "")),
        )
        return hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:24]

    def _append_message(self, row: dict) -> str:
        row = json_safe(dict(row))
        row.setdefault("receivedAt", int(time.time()))
        row.setdefault("source", "lora")
        row["id"] = self._message_id(row)
        with self._archive_lock:
            if row["id"] in self._known_id_set:
                return row["id"]
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            if MESSAGES_PATH.exists() and MESSAGES_PATH.stat().st_size > MAX_ARCHIVE_BYTES:
                lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()[-5000:]
                temporary = MESSAGES_PATH.with_suffix(".tmp")
                temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, MESSAGES_PATH)
            with MESSAGES_PATH.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            os.chmod(MESSAGES_PATH, 0o600)
            self._remember_id(row["id"])
        return row["id"]

    def _update_message(self, message_id: str, **updates) -> bool:
        """Update one archived message without exposing archive internals to the UI."""
        with self._archive_lock:
            try:
                lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()
            except (FileNotFoundError, PermissionError, OSError):
                return False
            changed = False
            output = []
            for line in lines:
                try:
                    row = json.loads(line)
                except (ValueError, json.JSONDecodeError):
                    output.append(line)
                    continue
                if isinstance(row, dict) and row.get("id") == message_id:
                    row.update(json_safe(updates))
                    line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                    changed = True
                output.append(line)
            if changed:
                temporary = MESSAGES_PATH.with_suffix(".tmp")
                temporary.write_text("\n".join(output) + "\n", encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, MESSAGES_PATH)
            return changed

    async def _on_ack(self, event) -> None:
        payload = json_safe(event.payload)
        code = str(payload.get("code", "")).lower()
        if not code:
            return
        message_id = self._pending_acks.pop(code, "")
        if message_id:
            self._update_message(
                message_id, delivery="delivered", deliveredAt=int(time.time()),
                roundTripMs=payload.get("trip_time"),
            )
            return
        if code not in self._early_ack_set:
            if len(self._early_acks) == self._early_acks.maxlen:
                self._early_ack_set.discard(self._early_acks[0])
            self._early_acks.append(code)
            self._early_ack_set.add(code)

    async def _on_channel_message(self, event) -> None:
        payload = json_safe(event.payload)
        raw_text = str(payload.get("text", "")).strip("\x00")
        match = SENDER_PATTERN.match(raw_text)
        sender, text = (match.group(1), match.group(2)) if match else ("", raw_text)
        timestamp = int(payload.get("sender_timestamp", 0) or time.time())
        channel_idx = int(payload.get("channel_idx", 0) or 0)
        channel_name = next(
            (item["name"] for item in self.channels if item["index"] == channel_idx),
            f"Канал {channel_idx}",
        )
        metadata = await self._channel_packet_metadata(timestamp, raw_text, payload)
        message_id = self._append_message({
            "direction": "rx", "kind": "channel", "timestamp": timestamp,
            "channel": channel_idx, "channelName": channel_name,
            "sender": sender, "text": text, "rawText": raw_text,
            **metadata,
        })
        asyncio.create_task(self._maybe_bot_reply(
            message_id=message_id, kind="channel", text=text, sender=sender,
            channel=channel_idx, channel_name=channel_name,
            rssi=payload.get("RSSI"), snr=payload.get("SNR"), path_length=payload.get("path_len"),
        ))

    async def _on_contact_message(self, event) -> None:
        payload = json_safe(event.payload)
        prefix = str(payload.get("pubkey_prefix", ""))
        contact = self.mc.get_contact_by_key_prefix(prefix) if self.mc else None
        sender = str((contact or {}).get("adv_name", prefix or "Неизвестный контакт"))
        contact_key = str((contact or {}).get("public_key", prefix))
        message_id = self._append_message({
            "direction": "rx", "kind": "contact",
            "timestamp": int(payload.get("sender_timestamp", 0) or time.time()),
            "contact": contact_key,
            "sender": sender, "text": str(payload.get("text", "")).strip("\x00"),
            "snr": payload.get("SNR"), "rssi": payload.get("RSSI"),
            "path": payload.get("path", ""), "pathLength": payload.get("path_len"),
        })
        asyncio.create_task(self._maybe_bot_reply(
            message_id=message_id, kind="contact", text=str(payload.get("text", "")).strip("\x00"),
            sender=sender, contact=contact_key,
            rssi=payload.get("RSSI"), snr=payload.get("SNR"), path_length=payload.get("path_len"),
        ))

    def bot_status(self) -> dict:
        now = int(time.time())
        with self._bot_lock:
            recent = [
                int(value) for value in self._bot_state["globalReplies"]
                if now - int(value) < BOT_GLOBAL_WINDOW_SECONDS
            ]
            return {
                "enabled": bool(self._bot_state["enabled"]),
                "trigger": BOT_TRIGGER, "reply": BOT_REPLY, "channel": BOT_CHANNEL,
                "directMessages": True, "exactMatch": True, "caseSensitive": False,
                "senderCooldownSeconds": BOT_SENDER_COOLDOWN_SECONDS,
                "globalLimit": BOT_GLOBAL_LIMIT, "globalWindowSeconds": BOT_GLOBAL_WINDOW_SECONDS,
                "repliesInWindow": len(recent),
                "lastReplyAt": int(self._bot_state.get("lastReplyAt", 0) or 0),
                "replyCount": int(self._bot_state.get("replyCount", 0) or 0),
            }

    def set_bot_enabled(self, enabled: bool) -> dict:
        with self._bot_lock:
            self._bot_state["enabled"] = bool(enabled)
            self._save_bot_state()
        return {"ok": True, **self.bot_status(), "transmitted": False}

    def _reserve_bot_reply(
        self, *, message_id: str, kind: str, text: str, sender: str,
        channel_name: str = "", contact: str = "",
    ) -> bool:
        # Exact means the complete message only; letter case is irrelevant in
        # community clients, where both "Ping" and "ping" are commonplace.
        if text.casefold() != BOT_TRIGGER.casefold():
            return False
        if kind == "channel" and channel_name.casefold() != BOT_CHANNEL.casefold():
            return False
        if kind not in {"channel", "contact"}:
            return False
        if sender and sender.casefold() == str(self.self_info.get("name", "")).casefold():
            return False
        now = int(time.time())
        sender_key = f"contact:{contact}" if kind == "contact" else f"channel:{channel_name}:{sender or 'unknown'}"
        with self._bot_lock:
            state = self._bot_state
            if not state["enabled"] or message_id in state["handled"]:
                return False
            state["handled"] = (state["handled"] + [message_id])[-1000:]
            recent = [int(value) for value in state["globalReplies"] if now - int(value) < BOT_GLOBAL_WINDOW_SECONDS]
            last = int(state["lastBySender"].get(sender_key, 0) or 0)
            allowed = now - last >= BOT_SENDER_COOLDOWN_SECONDS and len(recent) < BOT_GLOBAL_LIMIT
            if allowed:
                recent.append(now)
                state["lastBySender"][sender_key] = now
                state["lastReplyAt"] = now
                state["replyCount"] = int(state.get("replyCount", 0) or 0) + 1
            state["globalReplies"] = recent
            state["lastBySender"] = {
                key: int(value) for key, value in state["lastBySender"].items()
                if now - int(value) < BOT_GLOBAL_WINDOW_SECONDS
            }
            self._save_bot_state()
            return allowed

    @staticmethod
    def _clip_utf8(value: str, limit: int) -> str:
        output = ""
        for character in str(value):
            if len((output + character).encode("utf-8")) > limit:
                break
            output += character
        return output

    def _bot_reply_text(self, sender: str, path_length: object, rssi: object, snr: object) -> str:
        sender_text = self._clip_utf8(sender or "неизвестный", 32)
        node_name = self._clip_utf8(str(self.self_info.get("name", "BarbieNode💅")), 32)
        try:
            hops = str(max(0, int(path_length)))
        except (TypeError, ValueError, OverflowError):
            hops = "?"
        try:
            rssi_text = str(int(float(rssi)))
        except (TypeError, ValueError, OverflowError):
            rssi_text = "?"
        try:
            snr_text = f"{float(snr):.1f}"
        except (TypeError, ValueError, OverflowError):
            snr_text = "?"
        reply = f"🛜 Pong @[{sender_text}] {hops}🐰 · {BOT_LOCATION} · RSSI {rssi_text} · SNR {snr_text} ({node_name})"
        if len(reply.encode("utf-8")) <= MAX_MESSAGE_BYTES:
            return reply
        sender_text = self._clip_utf8(sender_text, 16)
        reply = f"🛜 Pong @[{sender_text}] {hops}🐰 · {BOT_LOCATION} · RSSI {rssi_text} · SNR {snr_text} ({node_name})"
        return self._clip_utf8(reply, MAX_MESSAGE_BYTES)

    async def _maybe_bot_reply(
        self, *, message_id: str, kind: str, text: str, sender: str,
        channel: int = 0, channel_name: str = "", contact: str = "",
        rssi: object = None, snr: object = None, path_length: object = None,
    ) -> None:
        try:
            if not self._reserve_bot_reply(
                message_id=message_id, kind=kind, text=text, sender=sender,
                channel_name=channel_name, contact=contact,
            ):
                return
            payload = {
                "kind": kind,
                "text": self._bot_reply_text(sender, path_length, rssi, snr),
                "automatic": True,
                "bot": True,
            }
            payload["channel" if kind == "channel" else "contact"] = channel if kind == "channel" else contact
            await self.send(payload)
            LOG.info("MeshCore bot replied to exact Ping in %s", channel_name if kind == "channel" else "direct message")
        except Exception as error:
            LOG.warning("MeshCore bot reply failed: %s", error)

    async def _refresh_channels(self, max_channels: int = 8) -> None:
        if not self.mc:
            return
        channels = []
        for index in range(max(1, min(64, max_channels))):
            result = await self.mc.commands.get_channel(index)
            if result and result.type == EventType.CHANNEL_INFO:
                payload = result.payload
                name = str(payload.get("channel_name", ""))
                if name:
                    channels.append({
                        "index": index, "name": name,
                        "hash": str(payload.get("channel_hash", "")).upper(),
                        "public": index == 0 or name.startswith("#"),
                    })
        self.channels = channels

    def _channel_limit(self) -> int:
        return max(1, min(64, int(self.device_info.get("max_channels", 8) or 8)))

    def _channel_values(self, payload: dict, max_channels: int) -> tuple[int, str, bytes | None]:
        index = int(payload.get("index", -1))
        if not 0 <= index < max_channels:
            raise ValueError(f"номер канала должен быть от 0 до {max_channels - 1}")
        name = str(payload.get("name", "")).strip()
        if not name or "\x00" in name or len(name.encode("utf-8")) > 32:
            raise ValueError("имя канала должно занимать 1–32 байта UTF-8")
        existing = next((item for item in self.channels if item["index"] == index), None)
        if existing and not bool(payload.get("replace", False)):
            raise ValueError(f"слот {index} уже занят каналом {existing['name']}")
        secret_text = str(payload.get("secret", "")).strip().replace(" ", "")
        secret = None
        if secret_text:
            if len(secret_text) != 32:
                raise ValueError("ключ канала должен содержать ровно 32 шестнадцатеричных символа")
            try:
                secret = bytes.fromhex(secret_text)
            except ValueError as error:
                raise ValueError("ключ канала должен быть шестнадцатеричным") from error
        elif not name.startswith("#"):
            raise ValueError("для приватного канала укажите 16-байтный ключ; без ключа разрешены только публичные имена с #")
        return index, name, secret

    async def _backup_channels(self, max_channels: int) -> Path:
        if not self.mc:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        backup = []
        for slot in range(max_channels):
            current = await self.mc.commands.get_channel(slot)
            if not current or current.type != EventType.CHANNEL_INFO:
                continue
            current_payload = current.payload
            current_name = str(current_payload.get("channel_name", ""))
            if not current_name:
                continue
            current_secret = current_payload.get("channel_secret", b"")
            backup.append({
                "index": slot,
                "name": current_name,
                "secret": current_secret.hex() if isinstance(current_secret, bytes) else str(current_secret),
            })
        CHANNEL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(CHANNEL_BACKUP_DIR, 0o700)
        backup_path = CHANNEL_BACKUP_DIR / f"channels-before-change-{time.time_ns()}.json"
        temporary = backup_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"createdAt": int(time.time()), "channels": backup}, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, backup_path)
        return backup_path

    async def backup_configuration(self) -> dict:
        """Create a private local identity/config backup without radio traffic."""
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        backup_path = await self._backup_channels(self._channel_limit())
        identity = await self.mc.commands.export_private_key()
        if not identity or identity.type == EventType.ERROR:
            raise RuntimeError("нода не разрешила резервную копию идентичности")
        tuning = await self.mc.commands.get_tuning()
        if not tuning or tuning.type == EventType.ERROR:
            raise RuntimeError("нода не вернула тонкие настройки для резервной копии")
        FIRMWARE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(FIRMWARE_BACKUP_DIR, 0o700)
        firmware_path = FIRMWARE_BACKUP_DIR / f"meshcore-before-firmware-{time.time_ns()}.json"
        temporary = firmware_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({
            "createdAt": int(time.time()),
            "identity": json_safe(identity.payload),
            "self": self.self_info,
            "device": self.device_info,
            "tuning": json_safe(tuning.payload),
            "channelBackup": backup_path.name,
        }, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, firmware_path)
        return {
            "ok": True,
            "backup": firmware_path.name,
            "channelBackup": backup_path.name,
            "channelCount": len(self.channels),
            "transmitted": False,
        }

    async def save_channel(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        max_channels = self._channel_limit()
        index, name, secret = self._channel_values(payload, max_channels)
        await self._backup_channels(max_channels)
        result = await self.mc.commands.set_channel(index, name, secret)
        if not result or result.type == EventType.ERROR:
            raise RuntimeError(f"нода отклонила канал: {json_safe(getattr(result, 'payload', {}))}")
        await self._refresh_channels(max_channels)
        saved = next((item for item in self.channels if item["index"] == index), None)
        if not saved or saved["name"] != name:
            raise RuntimeError("канал не прошёл проверку после записи")
        return {"ok": True, "channel": saved, "backupCreated": True}

    async def remove_channel(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        max_channels = self._channel_limit()
        index = int(payload.get("index", -1))
        if not 0 <= index < max_channels:
            raise ValueError(f"номер канала должен быть от 0 до {max_channels - 1}")
        existing = next((item for item in self.channels if item["index"] == index), None)
        if not existing:
            raise ValueError(f"слот {index} уже свободен")
        await self._backup_channels(max_channels)
        result = await self.mc.commands.set_channel(index, "", bytes(16))
        if not result or result.type == EventType.ERROR:
            raise RuntimeError(f"нода не удалила канал: {json_safe(getattr(result, 'payload', {}))}")
        await self._refresh_channels(max_channels)
        if any(item["index"] == index for item in self.channels):
            raise RuntimeError("удаление канала не прошло проверку")
        return {"ok": True, "index": index, "name": existing["name"], "backupCreated": True, "transmitted": False}

    async def save_channels(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        max_channels = self._channel_limit()
        requested = payload.get("channels")
        if not isinstance(requested, list) or not requested or len(requested) > max_channels:
            raise ValueError(f"укажите от 1 до {max_channels} каналов")
        if not all(isinstance(item, dict) for item in requested):
            raise ValueError("каждый канал должен быть JSON-объектом")
        parsed = [self._channel_values(item, max_channels) for item in requested]
        indexes = [item[0] for item in parsed]
        if len(set(indexes)) != len(indexes):
            raise ValueError("номера слотов не должны повторяться")
        await self._backup_channels(max_channels)
        for index, name, secret in parsed:
            result = await self.mc.commands.set_channel(index, name, secret)
            if not result or result.type == EventType.ERROR:
                raise RuntimeError(f"нода отклонила канал в слоте {index}: {json_safe(getattr(result, 'payload', {}))}")
        await self._refresh_channels(max_channels)
        saved = {item["index"]: item for item in self.channels}
        if any(index not in saved or saved[index]["name"] != name for index, name, _secret in parsed):
            raise RuntimeError("не все каналы прошли проверку после записи")
        return {"ok": True, "channels": [saved[index] for index in indexes], "backupCreated": True}

    def _refresh_contacts_snapshot(self) -> None:
        if not self.mc:
            return
        contacts = []
        for value in self.mc.contacts.values():
            item = json_safe(value)
            contacts.append({
                "publicKey": item.get("public_key", ""),
                "name": item.get("adv_name", "Без имени"),
                "type": item.get("type"), "lastAdvert": item.get("last_advert", 0),
                "lat": item.get("adv_lat"), "lon": item.get("adv_lon"),
                "pathLength": item.get("out_path_len"),
            })
        self.contacts = sorted(contacts, key=lambda item: str(item["name"]).casefold())

    def _verify_identity(self) -> None:
        name = str(self.self_info.get("name", ""))
        public_key = str(self.self_info.get("public_key", ""))
        if EXPECTED_NAME and not name.startswith(EXPECTED_NAME):
            raise RuntimeError(f"отказано: BLE-нода имеет имя {name!r}, ожидалось {EXPECTED_NAME!r}")
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            saved = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            temporary = IDENTITY_PATH.with_suffix(".tmp")
            temporary.write_text(json.dumps({"name": name, "publicKey": public_key}) + "\n", encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, IDENTITY_PATH)
            return
        if saved.get("publicKey") != public_key:
            raise RuntimeError("отказано: публичный ключ BLE-ноды отличается от сохранённой BarbieNode")

    async def _connect(self) -> None:
        self.connecting = True
        self.last_error = ""
        try:
            if TCP_HOST:
                mc = await MeshCore.create_tcp(TCP_HOST, port=TCP_PORT, default_timeout=8)
            else:
                mc = await MeshCore.create_ble(BLE_ADDRESS, default_timeout=8)
            if mc is None:
                transport = "TCP" if TCP_HOST else "BLE"
                raise RuntimeError(f"MeshCore {transport} не найден")
            self.mc = mc
            if not TCP_HOST:
                self.ble_address = str(getattr(mc.connection_manager.connection, "address", "") or BLE_ADDRESS or "")
            self.self_info = json_safe(mc.self_info)
            self._verify_identity()
            self._subscriptions = [
                mc.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_message),
                mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_contact_message),
                mc.subscribe(EventType.ACK, self._on_ack),
            ]
            device_result = await mc.commands.send_device_query()
            self.device_info = json_safe(device_result.payload) if device_result and not device_result.is_error() else {}
            await self._refresh_default_scope()
            contacts_result = await mc.commands.get_contacts(timeout=12)
            if contacts_result and not contacts_result.is_error():
                self._refresh_contacts_snapshot()
            await self._refresh_channels(int(self.device_info.get("max_channels", 8) or 8))
            mc.set_decrypt_channel_logs(True)
            await mc.start_auto_message_fetching()
            self.connected = True
            self.last_connected = int(time.time())
            endpoint = f"{TCP_HOST}:{TCP_PORT}" if TCP_HOST else self.ble_address
            LOG.info("Connected to %s at %s", self.self_info.get("name"), endpoint)
        except Exception as error:
            self.connected = False
            self.last_error = str(error)[:400]
            LOG.warning("MeshCore connection failed: %s", self.last_error)
            if self.mc:
                try:
                    await self.mc.disconnect()
                except Exception:
                    pass
            self.mc = None
        finally:
            self.connecting = False

    async def run(self) -> None:
        self._send_queue = asyncio.PriorityQueue()
        self._send_worker_task = asyncio.create_task(self._send_worker())
        while True:
            if not self.mc or not self.mc.is_connected:
                self.connected = False
                await self._connect()
            else:
                try:
                    # The TCP library can retain a stale "connected" flag
                    # after the node loses power. This is local management
                    # traffic only and does not create a LoRa packet.
                    result = await self.mc.commands.send_device_query()
                    if not result or result.is_error():
                        raise RuntimeError("MeshCore TCP health check failed")
                    self.device_info = json_safe(result.payload)
                    self.connected = True
                    self.last_error = ""
                    self._refresh_contacts_snapshot()
                except Exception as error:
                    self.connected = False
                    self.last_error = str(error)[:400]
                    LOG.warning("MeshCore connection lost: %s", self.last_error)
                    try:
                        await self.mc.disconnect()
                    except Exception:
                        pass
                    self.mc = None
            await asyncio.sleep(10 if self.connected else 15)

    async def _send_worker(self) -> None:
        """Serialize LoRa sends, always selecting user traffic before bot traffic."""
        assert self._send_queue is not None
        while True:
            _priority, _sequence, payload, future = await self._send_queue.get()
            try:
                if not future.cancelled():
                    future.set_result(await self._send_now(payload))
            except Exception as error:
                if not future.cancelled():
                    future.set_exception(error)
            finally:
                self._send_queue.task_done()

    def call(self, coroutine, timeout: float = 20):
        if not self.loop:
            raise RuntimeError("MeshCore bridge event loop is not ready")
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=timeout)

    def status(self) -> dict:
        info = dict(self.self_info)
        info.pop("public_key", None)
        device = dict(self.device_info)
        device.pop("ble_pin", None)
        return {
            "connected": self.connected, "connecting": self.connecting,
            "error": self.last_error, "lastConnected": self.last_connected,
            "bleAddress": self.ble_address, "self": info, "device": device,
            "channelCount": len(self.channels), "contactCount": len(self.contacts),
            "transport": "tcp" if TCP_HOST else "ble", "messageSource": "lora",
            "defaultScope": self.default_scope,
            "wifi": {
                "name": WIFI_NAME,
                "ip": TCP_HOST or "",
                "mac": self._neighbor_mac(TCP_HOST),
            },
        }

    @staticmethod
    def _neighbor_mac(address: str | None) -> str:
        if not address:
            return ""
        try:
            rows = Path("/proc/net/arp").read_text(encoding="ascii").splitlines()[1:]
        except (FileNotFoundError, PermissionError, OSError, UnicodeDecodeError):
            return ""
        for row in rows:
            fields = row.split()
            if len(fields) >= 4 and fields[0] == address and re.fullmatch(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", fields[3]):
                return fields[3].upper()
        return ""

    async def _refresh_default_scope(self) -> str:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        result = await self.mc.commands.get_default_flood_scope()
        if not result or result.is_error():
            raise RuntimeError("не удалось прочитать регион сообщений")
        payload = json_safe(result.payload)
        self.default_scope = str(payload.get("scope_name", "") or "").lstrip("#").casefold()
        return self.default_scope

    async def set_default_scope(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        scope = str(payload.get("scope", "")).strip().lstrip("#").casefold()
        if scope and not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,29}", scope):
            raise ValueError("регион должен содержать 1–30 латинских букв, цифр или дефисов")
        if scope:
            result = await self.mc.commands.set_default_flood_scope(scope)
        else:
            # Companion firmware clears the default scope only when
            # SET_DEFAULT_FLOOD_SCOPE has no payload. meshcore-python 1.17
            # incorrectly builds a 48-byte empty-name record, which firmware
            # rejects as ILLEGAL_ARG, so send the documented one-byte command.
            command = bytearray([CommandType.SET_DEFAULT_FLOOD_SCOPE.value])
            result = await self.mc.commands.send(command, [EventType.OK, EventType.ERROR])
        if not result or result.is_error():
            raise RuntimeError("нода отклонила регион сообщений")
        saved = await self._refresh_default_scope()
        if saved != scope:
            raise RuntimeError(f"проверка региона не прошла: сохранено {saved or 'пусто'}")
        return {"ok": True, "scope": saved, "transmitted": False}

    def messages(self, limit: int) -> list[dict]:
        try:
            lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()[-limit:]
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

    def read_state(self) -> dict[str, int]:
        with self._read_lock:
            try:
                payload = json.loads(READ_STATE_PATH.read_text(encoding="utf-8"))
            except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
                payload = {"__baseline": int(time.time())}
                STATE_DIR.mkdir(parents=True, exist_ok=True)
                temporary = READ_STATE_PATH.with_suffix(".tmp")
                temporary.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, READ_STATE_PATH)
        if not isinstance(payload, dict):
            return {}
        output = {}
        for key, value in payload.items():
            try:
                output[str(key)] = max(0, int(value))
            except (TypeError, ValueError, OverflowError):
                continue
        return output

    def mark_read(self, payload: dict) -> dict:
        kind = str(payload.get("kind", ""))
        value = str(payload.get("value", ""))
        if kind == "channel":
            channel = int(value)
            if not any(item["index"] == channel for item in self.channels):
                raise ValueError("неизвестный или отключённый канал")
            key = f"channel:{channel}"
        elif kind == "contact":
            contact = next(
                (item for item in self.contacts if str(item["publicKey"]).startswith(value) or value.startswith(str(item["publicKey"]))),
                None,
            )
            if not contact:
                raise ValueError("контакт не найден")
            key = f"contact:{contact['publicKey']}"
        else:
            raise ValueError("неизвестный тип чата")
        seen_at = min(int(time.time()) + 60, max(0, int(payload.get("seenAt", time.time()))))
        with self._read_lock:
            try:
                state = json.loads(READ_STATE_PATH.read_text(encoding="utf-8"))
            except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
                state = {}
            if not isinstance(state, dict):
                state = {}
            state[key] = max(int(state.get(key, 0) or 0), seen_at)
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            temporary = READ_STATE_PATH.with_suffix(".tmp")
            temporary.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, READ_STATE_PATH)
        return {"ok": True, "key": key, "seenAt": state[key]}

    def mark_all_read(self) -> dict:
        seen_at = int(time.time())
        with self._read_lock:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            temporary = READ_STATE_PATH.with_suffix(".tmp")
            temporary.write_text(
                json.dumps({"__baseline": seen_at}, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            os.replace(temporary, READ_STATE_PATH)
        return {"ok": True, "seenAt": seen_at, "transmitted": False}

    async def send(self, payload: dict) -> dict:
        if self._send_queue is None:
            raise RuntimeError("очередь отправки MeshCore ещё не готова")
        automatic = bool(payload.get("automatic", False))
        if automatic:
            # Give an almost-simultaneous user action time to enter the queue.
            # Once a radio transmission has begun it cannot safely be preempted.
            await asyncio.sleep(1.0)
        self._send_sequence += 1
        future = asyncio.get_running_loop().create_future()
        await self._send_queue.put((10 if automatic else 0, self._send_sequence, dict(payload), future))
        return await future

    async def _send_now(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        text = str(payload.get("text", "")).strip()
        if not text:
            raise ValueError("пустое сообщение")
        if len(text.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ValueError(f"сообщение длиннее {MAX_MESSAGE_BYTES} байт")
        kind = str(payload.get("kind", "channel"))
        automatic = bool(payload.get("automatic", False))
        bot = bool(payload.get("bot", False))
        timestamp = int(time.time())
        delivered = False
        if kind == "channel":
            channel = int(payload.get("channel", 0))
            known = next((item for item in self.channels if item["index"] == channel), None)
            if not known:
                raise ValueError("неизвестный или отключённый канал")
            result = await self.mc.commands.send_chan_msg(channel, text, timestamp)
            if not result or result.type == EventType.ERROR:
                raise RuntimeError(f"нода отклонила сообщение: {json_safe(getattr(result, 'payload', {}))}")
            message_id = self._append_message({
                "direction": "tx", "kind": "channel", "timestamp": timestamp,
                "channel": channel, "channelName": known["name"],
                "sender": self.self_info.get("name", "BarbieNode"), "text": text,
                "acceptedByRadio": True, "delivery": "broadcast", "source": "local-radio",
                "automatic": automatic, "bot": bot,
                "scope": self.default_scope, "transportCode": self._transport_code(self.default_scope),
            })
        elif kind == "contact":
            public_key = str(payload.get("contact", ""))
            contact = self.mc.get_contact_by_key_prefix(public_key)
            if not contact:
                raise ValueError("контакт не найден")
            result = await self.mc.commands.send_msg(contact, text)
            if not result or result.type == EventType.ERROR:
                raise RuntimeError(f"нода отклонила сообщение: {json_safe(getattr(result, 'payload', {}))}")
            expected_ack = ""
            if result.payload.get("expected_ack") is not None:
                value = result.payload["expected_ack"]
                expected_ack = value.hex() if isinstance(value, bytes) else str(value)
            message_id = self._append_message({
                "direction": "tx", "kind": "contact", "timestamp": timestamp,
                "contact": contact.get("public_key", public_key), "recipient": contact.get("adv_name", ""),
                "sender": self.self_info.get("name", "BarbieNode"), "text": text,
                "acceptedByRadio": True, "delivery": "pending", "source": "local-radio",
                "expectedAck": expected_ack,
                "automatic": automatic, "bot": bot,
                "scope": self.default_scope, "transportCode": self._transport_code(self.default_scope),
            })
            expected_ack = expected_ack.lower()
            if expected_ack:
                if expected_ack in self._early_ack_set:
                    self._early_ack_set.discard(expected_ack)
                    try:
                        self._early_acks.remove(expected_ack)
                    except ValueError:
                        pass
                    self._update_message(message_id, delivery="delivered", deliveredAt=int(time.time()))
                    delivered = True
                else:
                    self._pending_acks[expected_ack] = message_id
        else:
            raise ValueError("неизвестный тип адресата")
        return {
            "ok": True, "id": message_id, "acceptedByRadio": True,
            "receivedByPeer": delivered,
            "timestamp": timestamp,
        }

    async def advert(self, flood: bool) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        result = await self.mc.commands.send_advert(flood=flood)
        if not result or result.type == EventType.ERROR:
            raise RuntimeError("нода отклонила advert")
        return {"ok": True, "flood": flood, "acceptedByRadio": True}

    async def save_position(self, payload: dict) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        lat, lon = float(payload["lat"]), float(payload["lon"])
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise ValueError("координаты вне допустимого диапазона")
        result = await self.mc.commands.set_coords(lat, lon)
        if not result or result.type == EventType.ERROR:
            raise RuntimeError("нода отклонила координаты")
        share = bool(payload.get("share", False))
        result = await self.mc.commands.set_advert_loc_policy(1 if share else 0)
        if not result or result.type == EventType.ERROR:
            raise RuntimeError("не удалось сохранить политику публикации")
        self.self_info.update({"adv_lat": lat, "adv_lon": lon, "adv_loc_policy": 1 if share else 0})
        return {"ok": True, "lat": lat, "lon": lon, "share": share, "transmitted": False}

    async def set_power(self, value: int) -> dict:
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        maximum = min(22, int(self.self_info.get("max_tx_power", 22) or 22))
        if not -9 <= value <= maximum:
            raise ValueError(f"мощность должна быть от -9 до {maximum} dBm")
        result = await self.mc.commands.set_tx_power(value)
        if not result or result.type == EventType.ERROR:
            raise RuntimeError("нода отклонила мощность")
        self.self_info["tx_power"] = value
        return {"ok": True, "txPower": value, "maxTxPower": maximum, "transmitted": False}

    async def radio_diagnostics(self) -> dict:
        """Read tuning values over the local companion link; never transmit LoRa."""
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        result = await self.mc.commands.get_tuning()
        if not result or result.type == EventType.ERROR:
            raise RuntimeError(f"нода не вернула тонкие настройки: {json_safe(getattr(result, 'payload', {}))}")
        tuning = json_safe(result.payload)
        return {
            "rxDelay": int(tuning.get("rx_delay", 0) or 0) / 1000.0,
            "airtimeFactor": int(tuning.get("airtime_factor", 0) or 0) / 1000.0,
            "transmitted": False,
        }

    async def reboot(self) -> dict:
        """Restart the connected companion without transmitting over LoRa."""
        if not self.mc or not self.mc.is_connected:
            raise RuntimeError("BarbieNode не подключена к MeshCore-мосту")
        result = await self.mc.commands.reboot()
        if result and result.type == EventType.ERROR:
            raise RuntimeError("нода отклонила команду перезагрузки")
        # The BLE link normally disappears immediately after the command.  Let
        # the background loop establish a fresh connection once the node boots.
        self.connected = False
        return {
            "ok": True, "restarting": True,
            "transport": "tcp" if TCP_HOST else "ble", "transmitted": False,
        }


BRIDGE = Bridge()


class Handler(BaseHTTPRequestHandler):
    server_version = "BarbieNodeMeshCoreBridge/1"

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(json_safe(payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 16 * 1024:
            raise ValueError("неверный размер запроса")
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("ожидался JSON-объект")
        return value

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path == "/status":
            self._json(200, BRIDGE.status())
        elif parsed.path == "/channels":
            self._json(200, {"channels": BRIDGE.channels})
        elif parsed.path == "/contacts":
            self._json(200, {"contacts": BRIDGE.contacts})
        elif parsed.path == "/messages":
            try:
                limit = min(MAX_MESSAGES_RETURNED, max(1, int(parse_qs(parsed.query).get("limit", [200])[0])))
            except ValueError:
                limit = 200
            self._json(200, {"messages": BRIDGE.messages(limit)})
        elif parsed.path == "/reads":
            self._json(200, {"reads": BRIDGE.read_state()})
        elif parsed.path == "/scope":
            self._json(200, {"scope": BRIDGE.default_scope})
        elif parsed.path == "/bot":
            self._json(200, BRIDGE.bot_status())
        elif parsed.path == "/radio-diagnostics":
            try:
                self._json(200, BRIDGE.call(BRIDGE.radio_diagnostics(), timeout=20))
            except Exception as error:
                self._json(503, {"error": str(error)[:500]})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._body()
            path = urlsplit(self.path).path
            if path == "/send":
                result = BRIDGE.call(BRIDGE.send(payload), timeout=25)
            elif path == "/read":
                result = BRIDGE.mark_read(payload)
            elif path == "/read-all":
                result = BRIDGE.mark_all_read()
            elif path == "/scope":
                result = BRIDGE.call(BRIDGE.set_default_scope(payload), timeout=20)
            elif path == "/advert":
                result = BRIDGE.call(BRIDGE.advert(bool(payload.get("flood", False))), timeout=20)
            elif path == "/position":
                result = BRIDGE.call(BRIDGE.save_position(payload), timeout=25)
            elif path == "/power":
                result = BRIDGE.call(BRIDGE.set_power(int(payload["value"])), timeout=20)
            elif path == "/channel":
                result = BRIDGE.call(BRIDGE.save_channel(payload), timeout=25)
            elif path == "/channel/remove":
                result = BRIDGE.call(BRIDGE.remove_channel(payload), timeout=240)
            elif path == "/channels":
                result = BRIDGE.call(BRIDGE.save_channels(payload), timeout=240)
            elif path == "/reboot":
                result = BRIDGE.call(BRIDGE.reboot(), timeout=10)
            elif path == "/backup":
                result = BRIDGE.call(BRIDGE.backup_configuration(), timeout=240)
            elif path == "/bot":
                if "enabled" not in payload:
                    raise ValueError("укажите enabled")
                result = BRIDGE.set_bot_enabled(bool(payload["enabled"]))
            else:
                self._json(404, {"error": "not found"})
                return
            self._json(200, result)
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            self._json(400, {"error": str(error)})
        except Exception as error:
            LOG.exception("Bridge request failed")
            self._json(503, {"error": str(error)[:500]})

    def log_message(self, format: str, *args) -> None:
        LOG.debug(format, *args)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(STATE_DIR, 0o700)
    loop = asyncio.new_event_loop()
    BRIDGE.loop = loop

    def run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.create_task(BRIDGE.run())
        loop.run_forever()

    threading.Thread(target=run_loop, name="meshcore-ble", daemon=True).start()
    server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    LOG.info("MeshCore bridge listening on http://%s:%d", BIND_HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
