#!/usr/bin/env python3
"""Archive NightBot JSONL logs from a Meshtastic node and serve a local viewer."""

from __future__ import annotations

import hashlib
import html
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEVICE_URL = os.environ.get("DEVICE_URL", "http://meshtastic.local").rstrip("/")
POLL_SECONDS = max(10, int(os.environ.get("POLL_SECONDS", "60")))
PORT = int(os.environ.get("PORT", "8081"))
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
DB_PATH = Path(os.environ.get("DB_PATH", "/var/lib/nightbot-archive/nightbot.sqlite3"))
LOG_PATHS = ("/nightbot.previous.jsonl", "/nightbot.jsonl")
MOSCOW = timezone(timedelta(hours=3), name="MSK")

status_lock = threading.Lock()
status = {"last_attempt": None, "last_success": None, "last_error": None, "inserted": 0}


def connect_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            ts INTEGER NOT NULL,
            event TEXT NOT NULL,
            sender TEXT NOT NULL,
            recipient TEXT NOT NULL,
            channel INTEGER NOT NULL,
            rssi INTEGER,
            snr REAL,
            text TEXT NOT NULL,
            raw TEXT NOT NULL,
            first_seen INTEGER NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def fetch_log(path: str) -> list[str]:
    request = Request(f"{DEVICE_URL}{path}", headers={"User-Agent": "NightBotArchive/1.0"})
    with urlopen(request, timeout=10) as response:
        payload = response.read(1024 * 1024).decode("utf-8", errors="replace")
    return payload.splitlines()


def parse_line(line: str) -> dict | None:
    try:
        item = json.loads(line)
    except json.JSONDecodeError:
        return None
    required = {"ts", "event", "from", "to", "channel", "text"}
    if not isinstance(item, dict) or not required.issubset(item):
        return None
    return item


def archive_once() -> int:
    now = int(time.time())
    all_lines: list[str] = []
    errors: list[str] = []
    for path in LOG_PATHS:
        try:
            all_lines.extend(fetch_log(path))
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"{path}: {exc}")

    inserted = 0
    with connect_db() as database:
        for line in all_lines:
            item = parse_line(line)
            if item is None:
                continue
            normalized = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            event_id = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
            cursor = database.execute(
                """
                INSERT OR IGNORE INTO events
                    (id, ts, event, sender, recipient, channel, rssi, snr, text, raw, first_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    int(item.get("ts", 0)),
                    str(item.get("event", "")),
                    str(item.get("from", "")),
                    str(item.get("to", "")),
                    int(item.get("channel", 0)),
                    item.get("rssi"),
                    item.get("snr"),
                    str(item.get("text", "")),
                    normalized,
                    now,
                ),
            )
            inserted += cursor.rowcount

    with status_lock:
        status["last_attempt"] = now
        if all_lines:
            status["last_success"] = now
        status["last_error"] = "; ".join(errors) if errors and not all_lines else None
        status["inserted"] += inserted
    return inserted


def poll_forever() -> None:
    while True:
        try:
            archive_once()
        except Exception as exc:  # Keep the collector alive after transient or malformed input.
            with status_lock:
                status["last_attempt"] = int(time.time())
                status["last_error"] = repr(exc)
        time.sleep(POLL_SECONDS)


def get_events(limit: int = 1000) -> list[dict]:
    with connect_db() as database:
        rows = database.execute(
            "SELECT * FROM events ORDER BY ts DESC, first_seen DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def render_page() -> bytes:
    events = get_events(500)
    with status_lock:
        current_status = dict(status)

    rows = []
    for event in events:
        stamp = datetime.fromtimestamp(event["ts"], MOSCOW).strftime("%Y-%m-%d %H:%M:%S") if event["ts"] else "—"
        rows.append(
            "<tr>"
            f"<td>{html.escape(stamp)}</td>"
            f"<td><span class='badge {html.escape(event['event'])}'>{html.escape(event['event'])}</span></td>"
            f"<td>{html.escape(event['sender'])}</td>"
            f"<td>{html.escape(event['recipient'])}</td>"
            f"<td>{event['channel']}</td>"
            f"<td>{html.escape(event['text'])}</td>"
            f"<td>{event['rssi'] if event['rssi'] is not None else '—'} / {event['snr'] if event['snr'] is not None else '—'}</td>"
            "</tr>"
        )

    last_success = current_status["last_success"]
    synced = datetime.fromtimestamp(last_success, MOSCOW).strftime("%H:%M:%S") if last_success else "ещё не было"
    error = html.escape(current_status["last_error"] or "нет")
    body = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60"><title>Архив NightBot</title>
<style>
body{{font:14px system-ui,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}}
h1{{margin:0 0 8px}} .summary{{color:#94a3b8;margin-bottom:20px}} a{{color:#5eead4}}
table{{width:100%;border-collapse:collapse;background:#111c31}} th,td{{padding:9px;border-bottom:1px solid #26334b;text-align:left}}
th{{position:sticky;top:0;background:#1e293b}} .badge{{padding:2px 7px;border-radius:10px;background:#334155}}
.beacon{{background:#0369a1}} .rx{{background:#166534}} .autoreply{{background:#7c3aed}}
</style></head><body>
<h1>Архив NightBot</h1>
<div class="summary">Записей: {len(events)} · последняя синхронизация: {synced} MSK · ошибка: {error} ·
<a href="/api/events.json">JSON</a> · <a href="/api/export.jsonl">JSONL</a></div>
<table><thead><tr><th>Время MSK</th><th>Событие</th><th>От</th><th>Кому</th><th>Канал</th><th>Текст</th><th>RSSI / SNR</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></body></html>"""
    return body.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def send_body(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path == "/":
            self.send_body(200, "text/html; charset=utf-8", render_page())
        elif self.path == "/healthz":
            self.send_body(200, "text/plain; charset=utf-8", b"ok\n")
        elif self.path == "/api/events.json":
            body = json.dumps(get_events(), ensure_ascii=False, indent=2).encode("utf-8")
            self.send_body(200, "application/json; charset=utf-8", body)
        elif self.path == "/api/export.jsonl":
            events = reversed(get_events(100000))
            body = ("\n".join(event["raw"] for event in events) + "\n").encode("utf-8")
            self.send_body(200, "application/x-ndjson; charset=utf-8", body)
        else:
            self.send_body(404, "text/plain; charset=utf-8", b"not found\n")

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.client_address[0]} - {fmt % args}", flush=True)


def main() -> None:
    connect_db().close()
    threading.Thread(target=poll_forever, name="nightbot-poller", daemon=True).start()
    server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    print(
        f"NightBot archive listening on {BIND_HOST}:{PORT}; "
        f"polling {DEVICE_URL} every {POLL_SECONDS}s",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
