from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from novel_proofer.llm.client import LLMError
from novel_proofer.llm.config import LLMConfig
from novel_proofer.runner import _llm_process_chunk


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        # Always return 504 to force split logic.
        self.send_response(504)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"504")


def start_stub(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def main() -> int:
    port = 8123
    httpd = start_stub(port)
    try:
        cfg = LLMConfig(
            enabled=True,
            provider="openai_compatible",
            base_url=f"http://127.0.0.1:{port}",
            api_key="",
            model="DeepSeek-V3.2-Instruct",
            timeout_seconds=1,
            max_retries=0,
            split_min_chars=10,
        )

        try:
            _llm_process_chunk(cfg, "a\n\n" * 200)
        except LLMError:
            pass

        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
