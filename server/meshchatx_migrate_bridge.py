#!/usr/bin/env python3
"""One-shot import of the retired BarbieNode LXMF bridge into MeshChatX."""

from __future__ import annotations

import hashlib
import http.cookiejar
import json
from pathlib import Path
from urllib.request import HTTPCookieProcessor, Request, build_opener


BASE = "http://127.0.0.1:9338/api/v1"
STATE = Path("/var/lib/barbienode-reticulum")
OWN_ADDRESS = "9eae7099d07cbdde6890de954aa30730"


def load_contacts() -> list[dict]:
    path = STATE / "contacts.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = list(raw.values()) if isinstance(raw, dict) else raw
    return [row for row in rows if isinstance(row, dict)]


def contact_bundle(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    contacts: list[dict] = []
    names: list[dict] = []
    for row in rows:
        address = str(row.get("address", "")).lower()
        public_key = str(row.get("publicKey", ""))
        name = str(row.get("name", "")).strip() or address[:12]
        if len(address) != 32:
            continue
        names.append({"destination_hash": address, "display_name": name})
        try:
            identity_hash = hashlib.sha256(bytes.fromhex(public_key)).digest()[:16].hex()
        except (TypeError, ValueError):
            continue
        contacts.append(
            {
                "name": name,
                "remote_identity_hash": identity_hash,
                "lxmf_address": address,
                "is_telemetry_trusted": 0,
            }
        )
    return contacts, names


def message_bundle() -> list[dict]:
    path = STATE / "messages.jsonl"
    if not path.exists():
        return []
    output: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        peer = str(row.get("contact", "")).lower()
        content = str(row.get("text", ""))
        incoming = row.get("direction") == "rx"
        if len(peer) != 32 or not content:
            continue
        stable = str(row.get("id") or line)
        message_hash = hashlib.sha256(("barbienode-bridge:" + stable).encode()).hexdigest()
        delivery = str(row.get("delivery", ""))
        state = "delivered" if incoming or delivery == "delivered" else "failed" if delivery == "failed" else "sent"
        timestamp = float(row.get("timestamp") or row.get("receivedAt") or 0)
        output.append(
            {
                "hash": message_hash,
                "source_hash": peer if incoming else OWN_ADDRESS,
                "destination_hash": OWN_ADDRESS if incoming else peer,
                "peer_hash": peer,
                "state": state,
                "progress": 1.0 if state == "delivered" else 0.0,
                "is_incoming": 1 if incoming else 0,
                "method": "direct",
                "delivery_attempts": int(row.get("attempt") or 0),
                "title": "",
                "content": content,
                "fields": {},
                "timestamp": timestamp,
                "rssi": row.get("rssi"),
                "snr": row.get("snr"),
                "is_spam": 0,
            }
        )
    return output


def main() -> None:
    jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    csrf = json.loads(opener.open(BASE + "/auth/csrf", timeout=10).read())["csrf_token"]
    contacts, names = contact_bundle(load_contacts())
    payload = {
        "format": "meshchatx/messages/v2",
        "messages": message_bundle(),
        "contacts": contacts,
        "display_names": names,
        "conversation_read_state": [],
        "notification_viewed_state": [],
    }
    request = Request(
        BASE + "/maintenance/messages/import",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "X-CSRF-Token": csrf},
    )
    result = json.loads(opener.open(request, timeout=30).read())
    safety_request = Request(
        BASE + "/config",
        data=json.dumps(
            {
                "auto_announce_interval_seconds": 0,
                "auto_resend_failed_messages_when_announce_received": False,
                "allow_auto_resending_failed_messages_with_attachments": False,
                "auto_send_failed_messages_to_propagation_node": False,
                "lxmf_preferred_propagation_node_auto_select": False,
                "lxmf_preferred_propagation_node_auto_sync_interval_seconds": 0,
                "lxmf_local_propagation_node_enabled": True,
                "telephone_announce_enabled": False,
            }
        ).encode(),
        method="PATCH",
        headers={"Content-Type": "application/json", "X-CSRF-Token": csrf},
    )
    opener.open(safety_request, timeout=30).read()
    print(
        json.dumps(
            {
                "imported": result.get("imported", 0),
                "skipped": result.get("skipped", 0),
                "contacts_added": result.get("contacts_added", 0),
                "contacts_skipped": result.get("contacts_skipped", 0),
                "display_names_imported": result.get("display_names_imported", 0),
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
