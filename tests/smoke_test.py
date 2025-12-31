"""Local smoke tests for server handler.

This avoids long-lived background processes.
"""

from __future__ import annotations

import io
import json
import threading
import time
import urllib.request
from http.client import HTTPConnection
from pathlib import Path

from novel_proofer.server import Handler
from http.server import ThreadingHTTPServer


def _start_server(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def _request_health(port: int) -> bytes:
    return urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3).read()


def _multipart_body(boundary: str, fields: dict[str, str], file_field: str, filename: str, content: bytes) -> bytes:
    b = io.BytesIO()

    def w(s: str) -> None:
        b.write(s.encode("utf-8"))

    for k, v in fields.items():
        w(f"--{boundary}\r\n")
        w(f"Content-Disposition: form-data; name=\"{k}\"\r\n\r\n")
        w(v)
        w("\r\n")

    w(f"--{boundary}\r\n")
    w(f"Content-Disposition: form-data; name=\"{file_field}\"; filename=\"{filename}\"\r\n")
    w("Content-Type: text/plain\r\n\r\n")
    b.write(content)
    w("\r\n")
    w(f"--{boundary}--\r\n")

    return b.getvalue()


def _post_format_stats(port: int, text: str) -> dict:
    boundary = "----npboundary1234"
    fields = {
        "return": "stats",
        "max_chunk_chars": "60000",
        "paragraph_indent": "1",
        "normalize_ellipsis": "1",
        "normalize_em_dash": "1",
    }
    body = _multipart_body(boundary, fields, "file", "demo.txt", text.encode("utf-8"))

    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request(
        "POST",
        "/format",
        body=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
    )
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    if resp.status != 200:
        raise RuntimeError(f"HTTP {resp.status}: {raw}")
    return json.loads(raw)


def main() -> int:
    port = 8011
    httpd = _start_server(port)
    try:
        # Allow server to bind.
        time.sleep(0.1)
        health = _request_health(port)
        assert b"ok" in health

        stats = _post_format_stats(port, '第1章\n\n你好...\n')
        assert "stats" in stats
        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
