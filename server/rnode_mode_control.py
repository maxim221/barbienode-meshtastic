#!/usr/bin/env python3
"""Narrow loopback control used to release RNode before a firmware switch."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import subprocess
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

try:
    import LXMF
except ImportError:
    LXMF = None


HOST = "127.0.0.1"
PORT = 8768
MESHCHATX_SERVICE = "barbienode-meshchatx.service"
MESHCHATX_STORAGE_DIR = Path(os.environ.get(
    "MESHCHATX_STORAGE_DIR", "/var/lib/barbienode-meshchatx/storage",
))
RETICULUM_CONFIG_PATH = Path(os.environ.get(
    "RETICULUM_CONFIG_PATH", "/var/lib/barbienode-meshchatx/reticulum/config",
))
ANNOUNCE_EVENTS_PATH = Path(os.environ.get(
    "ANNOUNCE_EVENTS_PATH", "/var/lib/barbienode-reticulum/announce-events.jsonl",
))
MAX_ANNOUNCE_EVENTS = 5000
MAX_ANNOUNCE_ARCHIVE_BYTES = 8 * 1024 * 1024


def parse_db_time(value: object) -> int:
    if isinstance(value, (int, float)):
        return max(0, int(value))
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(0, int(parsed.timestamp()))
    except ValueError:
        return 0


def decode_display_name(app_data: object) -> str:
    if not app_data or LXMF is None:
        return ""
    try:
        decoded = LXMF.display_name_from_app_data(base64.b64decode(str(app_data)))
        return str(decoded or "").replace("\x00", "").strip()[:80]
    except Exception:
        return ""


def rnode_is_only_enabled_interface() -> bool:
    """Return true only when every enabled Reticulum interface is an RNode."""
    try:
        lines = RETICULUM_CONFIG_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    interfaces: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    in_interfaces = False
    for raw in lines:
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[[") and line.endswith("]]" ) and in_interfaces:
            current = {}
            interfaces.append(current)
            continue
        if line.startswith("[") and line.endswith("]") and not line.startswith("[["):
            in_interfaces = line.lower() == "[interfaces]"
            current = None
            continue
        if in_interfaces and current is not None and "=" in line:
            key, value = line.split("=", 1)
            current[key.strip().lower()] = value.strip()
    enabled = [
        item for item in interfaces
        if item.get("enabled", "yes").lower() not in {"no", "false", "off", "0"}
    ]
    return bool(enabled) and all(item.get("type", "").lower() == "rnodeinterface" for item in enabled)


class AnnounceJournal:
    """Persist a provenance-labelled view of MeshChatX announce receptions."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.seen: dict[str, tuple[str, int]] = {}

    @staticmethod
    def _database_path() -> Path | None:
        candidates = list(MESHCHATX_STORAGE_DIR.glob("identities/*/database.db"))
        return max(candidates, key=lambda path: path.stat().st_mtime, default=None)

    def _current_rows(self) -> list[dict]:
        database_path = self._database_path()
        if database_path is None:
            return []
        connection = sqlite3.connect(
            f"file:{database_path}?mode=ro", uri=True, timeout=2,
        )
        connection.row_factory = sqlite3.Row
        try:
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'",
                )
            }
            if "announces" not in tables:
                return []
            announce_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(announces)")
            }
            count_sql = "a.announce_count" if "announce_count" in announce_columns else "1"
            name_sql = "c.display_name" if "custom_destination_display_names" in tables else "NULL"
            join_sql = (
                "LEFT JOIN custom_destination_display_names c "
                "ON c.destination_hash = a.destination_hash"
                if "custom_destination_display_names" in tables else ""
            )
            rows = connection.execute(
                f"""
                SELECT a.destination_hash, a.aspect, a.app_data,
                       a.rssi, a.snr, a.quality, a.created_at, a.updated_at,
                       {count_sql} AS announce_count, {name_sql} AS custom_name
                  FROM announces a
                  {join_sql}
                 ORDER BY a.updated_at DESC
                 LIMIT {MAX_ANNOUNCE_EVENTS}
                """,
            ).fetchall()
        finally:
            connection.close()
        output = []
        for row in rows:
            destination = str(row["destination_hash"] or "").lower()
            if not destination:
                continue
            name = str(row["custom_name"] or "").strip()
            if not name:
                name = decode_display_name(row["app_data"])
            output.append({
                "destinationHash": destination,
                "name": name or destination[:12],
                "aspect": str(row["aspect"] or "unknown"),
                "rssi": row["rssi"],
                "snr": row["snr"],
                "quality": row["quality"],
                "firstSeen": parse_db_time(row["created_at"]),
                "receivedAt": parse_db_time(row["updated_at"]),
                "updatedKey": str(row["updated_at"] or ""),
                "announceCount": int(row["announce_count"] or 1),
            })
        return output

    def _load_existing_events(self) -> list[dict]:
        try:
            lines = ANNOUNCE_EVENTS_PATH.read_text(encoding="utf-8").splitlines()
        except OSError:
            return []
        output = []
        for line in lines[-MAX_ANNOUNCE_EVENTS:]:
            try:
                row = json.loads(line)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(row, dict):
                output.append(row)
        return output

    def _append(self, row: dict, source: str) -> None:
        received_at = int(row.get("receivedAt") or 0)
        event = {
            "id": hashlib.sha256(
                f"{row['destinationHash']}:{row['updatedKey']}:{row['announceCount']}".encode(),
            ).hexdigest()[:24],
            "destinationHash": row["destinationHash"],
            "name": row["name"],
            "aspect": row["aspect"],
            "rssi": row["rssi"],
            "snr": row["snr"],
            "quality": row["quality"],
            "firstSeen": row["firstSeen"],
            "receivedAt": received_at,
            "announceCount": row["announceCount"],
            "updatedKey": row["updatedKey"],
            "source": source,
            "historical": source != "lora-rnode",
        }
        ANNOUNCE_EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ANNOUNCE_EVENTS_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        os.chmod(ANNOUNCE_EVENTS_PATH, 0o600)
        if ANNOUNCE_EVENTS_PATH.stat().st_size > MAX_ANNOUNCE_ARCHIVE_BYTES:
            events = self._load_existing_events()[-MAX_ANNOUNCE_EVENTS // 2:]
            temporary = ANNOUNCE_EVENTS_PATH.with_suffix(".tmp")
            temporary.write_text(
                "".join(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n" for item in events),
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            os.replace(temporary, ANNOUNCE_EVENTS_PATH)

    def poll(self) -> None:
        with self.lock:
            existing = self._load_existing_events()
            for event in existing:
                destination = str(event.get("destinationHash") or "")
                updated = str(event.get("updatedKey") or event.get("receivedAt") or "")
                count = int(event.get("announceCount") or 1)
                if destination:
                    self.seen[destination] = (updated, count)
            rows = self._current_rows()
            first_snapshot = not existing and not self.seen
            now = int(time.time())
            rnode_only = rnode_is_only_enabled_interface()
            for row in reversed(rows):
                destination = row["destinationHash"]
                state = (row["updatedKey"], row["announceCount"])
                if self.seen.get(destination) == state:
                    continue
                is_live = (
                    not first_snapshot and rnode_only and row["receivedAt"] > 0
                    and abs(now - row["receivedAt"]) <= 30
                )
                source = "lora-rnode" if is_live else "meshchatx-archive"
                self._append(row, source)
                self.seen[destination] = state

    def run(self) -> None:
        while True:
            try:
                self.poll()
            except Exception:
                pass
            time.sleep(2)

    def events(self, limit: int) -> list[dict]:
        with self.lock:
            return list(reversed(self._load_existing_events()))[:limit]


ANNOUNCE_JOURNAL = AnnounceJournal()


def restart_later() -> None:
    # Keep the LXMF backend available across firmware changes. While another
    # firmware is active it remains receive-only/offline and retries RNode.
    time.sleep(20)
    subprocess.run(
        ["sudo", "-n", "systemctl", "start", MESHCHATX_SERVICE],
        check=False,
        timeout=20,
    )


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path != "/announces":
            self.send_error(404)
            return
        try:
            limit = int(parse_qs(parsed.query).get("limit", ["300"])[0])
        except (TypeError, ValueError):
            limit = 300
        limit = max(1, min(MAX_ANNOUNCE_EVENTS, limit))
        self._json({
            "events": ANNOUNCE_JOURNAL.events(limit),
            "rnodeOnly": rnode_is_only_enabled_interface(),
            "transmitted": False,
        })

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/prepare-boot":
            self.send_error(404)
            return
        result = subprocess.run(
            ["sudo", "-n", "systemctl", "stop", MESHCHATX_SERVICE],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if result.returncode != 0:
            self._json({"error": "Не удалось освободить RNode"}, 503)
            return
        threading.Thread(target=restart_later, daemon=True).start()
        self._json({"ok": True, "transmitted": False})

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    threading.Thread(
        target=ANNOUNCE_JOURNAL.run,
        name="meshchatx-announce-journal",
        daemon=True,
    ).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
