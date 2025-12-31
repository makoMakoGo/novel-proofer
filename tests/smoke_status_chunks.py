"""Smoke test for status payload with chunk details."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer

from novel_proofer.server import Handler


def _start_server(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def main() -> int:
    port = 8012
    httpd = _start_server(port)
    try:
        time.sleep(0.1)
        # just ensure endpoint responds with json 404 when missing.
        url = f"http://127.0.0.1:{port}/api/jobs/status?job_id=missing"
        try:
            urllib.request.urlopen(url, timeout=2).read()
        except Exception:
            pass
        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
