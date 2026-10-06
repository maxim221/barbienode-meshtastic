#!/usr/bin/env python3
"""Local-only Reticulum/LXMF bridge for the BarbieNode RNode mode."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import LXMF
import RNS


LOG = logging.getLogger("barbienode.reticulum")
BIND_HOST = os.environ.get("RETICULUM_BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("RETICULUM_BRIDGE_PORT", "8767"))
CONFIG_DIR = Path(os.environ.get("RETICULUM_CONFIG_DIR", "/etc/barbienode/reticulum"))
STATE_DIR = Path(os.environ.get("RETICULUM_STATE_DIR", "/var/lib/barbienode-reticulum"))
DISPLAY_NAME = os.environ.get("RETICULUM_DISPLAY_NAME", "BarbieNode RNode").strip() or "BarbieNode RNode"
IDENTITY_PATH = STATE_DIR / "identity"
CONTACTS_PATH = STATE_DIR / "contacts.json"
MESSAGES_PATH = STATE_DIR / "messages.jsonl"
PROPAGATION_ANNOUNCED_PATH = STATE_DIR / "propagation-announced"
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_MESSAGES_RETURNED = 500
MAX_MESSAGE_BYTES = 240
PATH_REQUEST_COOLDOWN = 5 * 60
PROPAGATION_STORAGE_LIMIT = 64 * 1024 * 1024


def hex_hash(value: bytes | None) -> str:
    return RNS.hexrep(value, delimit=False) if value else ""


class LXMFAnnounceHandler:
    aspect_filter = "lxmf.delivery"
    receive_path_responses = True

    def __init__(self, bridge: "ReticulumBridge") -> None:
        self.bridge = bridge

    def received_announce(
        self, destination_hash, announced_identity, app_data,
        announce_packet_hash=None, is_path_response=False,
    ) -> None:
        self.bridge.remember_contact(
            destination_hash,
            announced_identity,
            app_data,
            announce_packet_hash,
            is_path_response,
        )


class ReticulumBridge:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.archive_lock = threading.Lock()
        self.contacts: dict[str, dict] = {}
        self.known_ids = deque(maxlen=5000)
        self.known_id_set: set[str] = set()
        self.last_error = ""
        self.started_at = int(time.time())
        self.reticulum = None
        self.router = None
        self.destination = None
        self.identity = None
        self.path_requests: dict[str, int] = {}
        self._load_contacts()
        self._load_message_ids()

    def start(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(STATE_DIR, 0o700)
        if IDENTITY_PATH.exists():
            identity = RNS.Identity.from_file(str(IDENTITY_PATH))
            if identity is None:
                raise RuntimeError("не удалось прочитать сохранённую Reticulum identity")
        else:
            identity = RNS.Identity()
            identity.to_file(str(IDENTITY_PATH))
            os.chmod(IDENTITY_PATH, 0o600)

        self.reticulum = RNS.Reticulum(configdir=str(CONFIG_DIR), loglevel=RNS.LOG_NOTICE)
        self._restore_contact_identities()
        self.identity = identity
        self.router = LXMF.LXMRouter(identity=identity, storagepath=str(STATE_DIR), name=DISPLAY_NAME)
        self.destination = self.router.register_delivery_identity(identity, display_name=DISPLAY_NAME)
        if self.destination is None:
            raise RuntimeError("не удалось создать LXMF destination")
        self._enable_propagation_node()
        self.router.register_delivery_callback(self._on_message)
        RNS.Transport.register_announce_handler(LXMFAnnounceHandler(self))
        LOG.info("LXMF bridge ready at %s (%s)", DISPLAY_NAME, hex_hash(self.destination.hash))

    def _enable_propagation_node(self) -> None:
        """Enable a bounded store, announcing only on its first activation."""
        self.router.message_storage_limit = PROPAGATION_STORAGE_LIMIT
        suppress_startup_announce = PROPAGATION_ANNOUNCED_PATH.exists()
        announce_method = self.router.announce_propagation_node
        if suppress_startup_announce:
            self.router.announce_propagation_node = lambda *args, **kwargs: None
        try:
            self.router.enable_propagation()
        finally:
            self.router.announce_propagation_node = announce_method
        if not suppress_startup_announce:
            PROPAGATION_ANNOUNCED_PATH.touch(mode=0o600, exist_ok=True)
            os.chmod(PROPAGATION_ANNOUNCED_PATH, 0o600)

    def _load_contacts(self) -> None:
        try:
            payload = json.loads(CONTACTS_PATH.read_text(encoding="utf-8"))
        except (FileNotFoundError, PermissionError, OSError, ValueError, json.JSONDecodeError):
            return
        if isinstance(payload, dict):
            self.contacts = {str(key): value for key, value in payload.items() if isinstance(value, dict)}

    def _save_contacts(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        temporary = CONTACTS_PATH.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.contacts, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, CONTACTS_PATH)

    def _restore_contact_identities(self) -> None:
        restored = 0
        for address, contact in self.contacts.items():
            try:
                destination_hash = bytes.fromhex(address)
                public_key = bytes.fromhex(str(contact.get("publicKey", "")))
                packet_hash = bytes.fromhex(str(contact.get("announcePacketHash", "")))
                if len(destination_hash) != RNS.Reticulum.TRUNCATED_HASHLENGTH // 8:
                    continue
                if len(public_key) != RNS.Identity.KEYSIZE // 8 or not packet_hash:
                    continue
                RNS.Identity.remember(packet_hash, destination_hash, public_key)
                restored += 1
            except (TypeError, ValueError):
                continue
        if restored:
            LOG.info("Restored %d contact identities", restored)

    def remember_contact(
        self,
        destination_hash,
        announced_identity,
        app_data,
        announce_packet_hash,
        is_path_response: bool,
    ) -> None:
        address = hex_hash(destination_hash)
        if not address:
            return
        try:
            name = LXMF.display_name_from_app_data(app_data) or address[:12]
        except Exception:
            name = address[:12]
        public_key = ""
        try:
            public_key = hex_hash(announced_identity.get_public_key())
        except Exception:
            LOG.warning("Announce for %s did not contain a usable public identity", address[:12])
        packet_hash = hex_hash(announce_packet_hash)
        with self.lock:
            previous = self.contacts.get(address, {})
            self.contacts[address] = {
                **previous,
                "address": address,
                "name": str(name)[:80],
                "lastAnnounce": int(time.time()),
                "pathResponse": bool(is_path_response),
                "lastMessage": int(previous.get("lastMessage", 0) or 0),
                "publicKey": public_key or str(previous.get("publicKey", "")),
                "announcePacketHash": packet_hash or str(previous.get("announcePacketHash", "")),
            }
            self._save_contacts()

    def _load_message_ids(self) -> None:
        try:
            lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()[-5000:]
        except (FileNotFoundError, PermissionError, OSError):
            return
        for line in lines:
            try:
                identifier = str(json.loads(line).get("id", ""))
            except (ValueError, json.JSONDecodeError):
                continue
            if identifier:
                self._remember_id(identifier)

    def _remember_id(self, identifier: str) -> None:
        if identifier in self.known_id_set:
            return
        if len(self.known_ids) == self.known_ids.maxlen:
            self.known_id_set.discard(self.known_ids[0])
        self.known_ids.append(identifier)
        self.known_id_set.add(identifier)

    @staticmethod
    def _row_id(row: dict) -> str:
        source = "\0".join(str(row.get(key, "")) for key in ("direction", "contact", "timestamp", "text"))
        return hashlib.sha256(source.encode("utf-8")).hexdigest()[:24]

    def _append_message(self, row: dict) -> str:
        row = dict(row)
        row.setdefault("receivedAt", int(time.time()))
        row.setdefault("source", "lora")
        row["id"] = str(row.get("id") or self._row_id(row))
        with self.archive_lock:
            if row["id"] in self.known_id_set:
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

    def _update_message(self, identifier: str, **updates) -> None:
        with self.archive_lock:
            try:
                lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()
            except (FileNotFoundError, PermissionError, OSError):
                return
            output = []
            changed = False
            for line in lines:
                try:
                    row = json.loads(line)
                except (ValueError, json.JSONDecodeError):
                    output.append(line)
                    continue
                if isinstance(row, dict) and row.get("id") == identifier:
                    row.update(updates)
                    line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                    changed = True
                output.append(line)
            if changed:
                temporary = MESSAGES_PATH.with_suffix(".tmp")
                temporary.write_text("\n".join(output) + "\n", encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, MESSAGES_PATH)

    def _on_message(self, message) -> None:
        source = hex_hash(message.source_hash)
        timestamp = int(getattr(message, "timestamp", 0) or time.time())
        text = message.content_as_string()
        title = message.title_as_string()
        with self.lock:
            contact = self.contacts.get(source, {})
            if source:
                self.contacts[source] = {
                    **contact,
                    "address": source,
                    "name": contact.get("name", source[:12]),
                    "lastAnnounce": int(contact.get("lastAnnounce", 0) or 0),
                    "pathResponse": bool(contact.get("pathResponse", False)),
                    "lastMessage": int(time.time()),
                }
                self._save_contacts()
        self._append_message({
            "id": hex_hash(getattr(message, "hash", None)),
            "direction": "rx", "contact": source,
            "sender": contact.get("name", source[:12] if source else "Неизвестный"),
            "timestamp": timestamp, "title": title, "text": text,
            "signatureValid": bool(getattr(message, "signature_validated", False)),
            "source": "lora",
        })

    @staticmethod
    def _interfaces() -> list[dict]:
        output = []
        for interface in list(getattr(RNS.Transport, "interfaces", [])):
            output.append({
                "name": str(interface),
                "type": interface.__class__.__name__,
                "online": bool(getattr(interface, "online", False)),
                "rxBytes": int(getattr(interface, "rxb", 0) or 0),
                "txBytes": int(getattr(interface, "txb", 0) or 0),
                "bitrate": int(getattr(interface, "bitrate", 0) or 0),
                "rssi": getattr(interface, "r_stat_rssi", None),
                "snr": getattr(interface, "r_stat_snr", None),
            })
        return output

    def status(self) -> dict:
        interfaces = self._interfaces()
        rnodes = [item for item in interfaces if item["type"] == "RNodeInterface"]
        return {
            "ready": self.destination is not None,
            "address": hex_hash(self.destination.hash) if self.destination else "",
            "displayName": DISPLAY_NAME,
            "interfaces": interfaces,
            "rnodeOnline": any(item["online"] for item in rnodes),
            "contactCount": len(self.contacts),
            "messageSource": "lora",
            "internetGateways": False,
            "automaticAnnounce": False,
            "propagationNode": bool(self.router and self.router.propagation_node),
            "propagationAddress": hex_hash(self.router.propagation_destination.hash) if self.router and self.router.propagation_node else "",
            "propagationStorageBytes": int(self.router.message_storage_size()) if self.router and self.router.propagation_node else 0,
            "propagationStorageLimitBytes": PROPAGATION_STORAGE_LIMIT,
            "propagationStartupAnnounce": False,
            "lastError": self.last_error,
            "startedAt": self.started_at,
        }

    def contact_list(self) -> list[dict]:
        with self.lock:
            contacts = [
                {
                    key: value for key, value in item.items()
                    if key not in {"publicKey", "announcePacketHash"}
                }
                for item in self.contacts.values()
            ]
            return sorted(contacts, key=lambda item: (-int(item.get("lastMessage", 0) or 0), str(item.get("name", "")).casefold()))

    def messages(self, limit: int) -> list[dict]:
        try:
            lines = MESSAGES_PATH.read_text(encoding="utf-8").splitlines()[-limit:]
        except (FileNotFoundError, PermissionError, OSError):
            return []
        output = []
        for line in lines:
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    output.append(item)
            except (ValueError, json.JSONDecodeError):
                continue
        return output

    def announce(self) -> dict:
        if not self.destination or not self.router:
            raise RuntimeError("LXMF bridge ещё не готов")
        if not self.status()["rnodeOnline"]:
            raise RuntimeError("RNode-интерфейс не подключён")
        self.router.announce(self.destination.hash)
        return {"ok": True, "accepted": True, "transport": "lora", "automatic": False}

    def prepare_boot(self) -> dict:
        """Release the RNode TCP session without transmitting an LXMF packet."""
        detached = False
        for interface in list(getattr(RNS.Transport, "interfaces", [])):
            if interface.__class__.__name__ != "RNodeInterface" or getattr(interface, "detached", False):
                continue
            interface.detach()
            detached = True

        # RNS does not support rebuilding a detached interface in place. Exit
        # after the HTTP response; systemd restarts us and waits for RNode to
        # become available after the firmware transition.
        def restart_bridge() -> None:
            time.sleep(5)
            os._exit(1)

        threading.Thread(target=restart_bridge, name="reticulum-restart-after-boot", daemon=True).start()
        return {"ok": True, "detached": detached, "transmitted": False}

    def send(self, payload: dict) -> dict:
        if not self.destination or not self.router:
            raise RuntimeError("LXMF bridge ещё не готов")
        if not self.status()["rnodeOnline"]:
            raise RuntimeError("RNode-интерфейс не подключён")
        address = str(payload.get("contact", "")).lower().strip()
        text = str(payload.get("text", "")).strip()
        if not text:
            raise ValueError("пустое сообщение")
        if len(text.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ValueError(f"сообщение длиннее {MAX_MESSAGE_BYTES} байт")
        try:
            destination_hash = bytes.fromhex(address)
        except ValueError as error:
            raise ValueError("неверный адрес LXMF") from error
        if len(destination_hash) != RNS.Reticulum.TRUNCATED_HASHLENGTH // 8:
            raise ValueError("неверная длина адреса LXMF")
        identity = RNS.Identity.recall(destination_hash)
        if identity is None:
            now = int(time.time())
            last_request = int(self.path_requests.get(address, 0) or 0)
            requested = False
            if now - last_request >= PATH_REQUEST_COOLDOWN:
                RNS.Transport.request_path(destination_hash)
                self.path_requests[address] = now
                requested = True
            return {
                "ok": True,
                "queued": False,
                "transport": "lora",
                "delivered": False,
                "identityPending": True,
                "pathRequested": requested,
                "retryAfter": max(0, PATH_REQUEST_COOLDOWN - (now - last_request)) if not requested else 0,
            }
        destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "lxmf", "delivery")
        message = LXMF.LXMessage(destination, self.destination, text, title="", desired_method=LXMF.LXMessage.DIRECT)
        message.pack()
        identifier = hex_hash(message.hash)
        contact = self.contacts.get(address, {})
        self._append_message({
            "id": identifier, "direction": "tx", "contact": address,
            "recipient": contact.get("name", address[:12]), "timestamp": int(time.time()),
            "text": text, "delivery": "pending", "source": "local-radio",
        })
        message.register_delivery_callback(lambda _message: self._update_message(identifier, delivery="delivered", deliveredAt=int(time.time())))
        message.register_failed_callback(lambda _message: self._update_message(identifier, delivery="failed", failedAt=int(time.time())))
        self.router.handle_outbound(message)
        return {"ok": True, "id": identifier, "queued": True, "transport": "lora", "delivered": False}


BRIDGE = ReticulumBridge()


class Handler(BaseHTTPRequestHandler):
    server_version = "BarbieNodeReticulumBridge/1"

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
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
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ValueError("ожидался JSON-объект")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path == "/status":
            self._json(200, BRIDGE.status())
        elif parsed.path == "/contacts":
            self._json(200, {"contacts": BRIDGE.contact_list()})
        elif parsed.path == "/messages":
            try:
                limit = min(MAX_MESSAGES_RETURNED, max(1, int(parse_qs(parsed.query).get("limit", [200])[0])))
            except ValueError:
                limit = 200
            self._json(200, {"messages": BRIDGE.messages(limit)})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._body()
            path = urlsplit(self.path).path
            if path == "/send":
                result = BRIDGE.send(payload)
            elif path == "/announce":
                result = BRIDGE.announce()
            elif path == "/prepare-boot":
                result = BRIDGE.prepare_boot()
            else:
                self._json(404, {"error": "not found"})
                return
            self._json(200, result)
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            self._json(400, {"error": str(error)})
        except Exception as error:
            LOG.exception("Reticulum bridge request failed")
            self._json(503, {"error": str(error)[:500]})

    def log_message(self, format: str, *args) -> None:
        LOG.debug(format, *args)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    BRIDGE.start()
    server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    LOG.info("Reticulum bridge listening on http://%s:%d", BIND_HOST, PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
