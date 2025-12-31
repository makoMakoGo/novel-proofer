"""Smoke test for job cancel flow.

Runs a short-lived server in-process.
"""

from __future__ import annotations

import io
import json
import sys
import threading
import time
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from novel_proofer.server import Handler


def _start_server(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


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


def _create_job(port: int) -> str:
    boundary = "----npboundarycancel"
    fields = {
        "max_chunk_chars": "2000",
        "llm_enabled": "0",  # keep it fast
        "suffix": "_rev",
    }
    body = _multipart_body(boundary, fields, "file", "demo.txt", ("第1章\n\n你好...\n" * 2000).encode("utf-8"))

    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request(
        "POST",
        "/api/jobs/create",
        body=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
    )
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    if resp.status != 200:
        raise RuntimeError(f"HTTP {resp.status}: {raw}")
    payload = json.loads(raw)
    return str(payload.get("job_id") or "")


def _cancel_job(port: int, job_id: str) -> None:
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    data = json.dumps({"job_id": job_id}).encode("utf-8")
    conn.request(
        "POST",
        "/api/jobs/cancel",
        body=data,
        headers={"Content-Type": "application/json", "Content-Length": str(len(data))},
    )
    resp = conn.getresponse()
    resp.read()


def _get_status(port: int, job_id: str) -> dict:
    conn = HTTPConnection("127.0.0.1", port, timeout=3)
    conn.request("GET", f"/api/jobs/status?job_id={job_id}")
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def main() -> int:
    port = 8013
    httpd = _start_server(port)
    try:
        time.sleep(0.1)

        # Create a job with LLM enabled and a stub base_url that won't resolve.
        # This forces the job to run long enough so cancellation is observable.
        boundary = "----npboundarycancel"
        fields = {
            "max_chunk_chars": "2000",
            "llm_enabled": "1",
            "llm_provider": "openai_compatible",
            "llm_base_url": "http://127.0.0.1:1",
            "llm_model": "x",
            "llm_timeout_seconds": "0.2",
            "llm_max_concurrency": "4",
            "suffix": "_rev",
        }
        body = _multipart_body(boundary, fields, "file", "demo.txt", ("第1章\n\n你好...\n" * 5000).encode("utf-8"))

        conn = HTTPConnection("127.0.0.1", port, timeout=5)
        conn.request(
            "POST",
            "/api/jobs/create",
            body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
        )
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {raw}")
        job_id = str(json.loads(raw).get("job_id") or "")
        assert job_id

        # Wait until job transitions out of queued.
        for _ in range(40):
            st = _get_status(port, job_id)
            if st.get("state") in {"running", "error", "done"}:
                break
            time.sleep(0.05)

        _cancel_job(port, job_id)

        last = {}
        for _ in range(80):
            last = _get_status(port, job_id)
            if last.get("state") == "cancelled":
                return 0
            time.sleep(0.05)

        raise AssertionError(f"expected cancelled, got {last}")
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
