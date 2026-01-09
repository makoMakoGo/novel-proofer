"""Smoke test for manual job cleanup flow (error case).

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


def _create_job_that_errors(port: int) -> str:
    boundary = "----npboundarycleanup"
    fields = {
        "max_chunk_chars": "2000",
        "llm_enabled": "1",
        "llm_provider": "openai_compatible",
        "llm_base_url": "http://127.0.0.1:1",
        "llm_model": "x",
        "llm_timeout_seconds": "0.2",
        "llm_max_concurrency": "1",
        "suffix": "_rev",
    }
    body = _multipart_body(boundary, fields, "file", "demo.txt", ("第1章\n\n你好...\n" * 200).encode("utf-8"))

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


def _get_status(port: int, job_id: str) -> tuple[int, dict]:
    conn = HTTPConnection("127.0.0.1", port, timeout=3)
    conn.request("GET", f"/api/jobs/status?job_id={job_id}")
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw}
    return resp.status, payload


def _cleanup_job(port: int, job_id: str) -> tuple[int, dict]:
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    data = json.dumps({"job_id": job_id}).encode("utf-8")
    conn.request(
        "POST",
        "/api/jobs/cleanup",
        body=data,
        headers={"Content-Type": "application/json", "Content-Length": str(len(data))},
    )
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {"raw": raw}
    return resp.status, payload


def main() -> int:
    port = 8015
    httpd = _start_server(port)
    try:
        time.sleep(0.1)
        job_id = _create_job_that_errors(port)
        assert job_id

        # Wait for job to reach error state.
        last: dict = {}
        for _ in range(200):
            code, st = _get_status(port, job_id)
            last = st
            if code == 200 and st.get("state") == "error":
                break
            time.sleep(0.05)
        else:
            raise AssertionError(f"expected error, got {last}")

        code, out = _cleanup_job(port, job_id)
        assert code == 200 and out.get("ok") is True

        # Job record should be deleted after cleanup.
        code, _ = _get_status(port, job_id)
        assert code == 404
        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

