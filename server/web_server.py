#!/usr/bin/env python3
"""Serve the Meshtastic single-page application without external dependencies."""

from __future__ import annotations

import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen


WEB_ROOT = Path(os.environ.get("WEB_ROOT", "/opt/barbienode-web")).resolve()
BIND_HOST = os.environ.get("BIND_HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8082"))
DEVICE_URL = os.environ.get("DEVICE_URL", "http://meshtastic.local").rstrip("/")
PROXY_PREFIXES = (
    "/api/",
    "/json/",
    "/dualboot/",
    "/nightbot",
    "/notifications/",
    "/upload",
)


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

    def _is_device_request(self) -> bool:
        return urlsplit(self.path).path.startswith(PROXY_PREFIXES)

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
        if self._is_device_request():
            self._proxy_device_request()
            return
        self.send_error(405)

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self._is_device_request():
            self._proxy_device_request()
            return
        self.send_error(405)

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "same-origin")
        if self._is_device_request():
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Pragma", "no-cache")
        elif self.path == "/index.html" or self.path == "/":
            self.send_header("Cache-Control", "no-cache")
        else:
            self.send_header("Cache-Control", "public, max-age=7776000, immutable")
        super().end_headers()


def main() -> None:
    server = ThreadingHTTPServer((BIND_HOST, PORT), SPAHandler)
    print(
        f"Meshtastic Web listening on http://{BIND_HOST}:{PORT}; "
        f"proxying Meshtastic device endpoints to {DEVICE_URL}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
