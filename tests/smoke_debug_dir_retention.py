"""Smoke test for debug directory cleanup/retention on successful jobs."""

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

from novel_proofer.server import Handler, JOBS_DIR  # noqa: E402


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


def _create_job(port: int, fields: dict[str, str], text: str) -> str:
    boundary = "----npboundarydebugret"
    body = _multipart_body(boundary, fields, "file", "demo.txt", text.encode("utf-8"))

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


def _get_status(port: int, job_id: str) -> dict:
    conn = HTTPConnection("127.0.0.1", port, timeout=3)
    conn.request("GET", f"/api/jobs/status?job_id={job_id}&include_chunks=0")
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _wait_done(port: int, job_id: str, timeout_s: float = 10.0) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        last = _get_status(port, job_id)
        if last.get("state") in {"done", "error", "cancelled"}:
            return last
        time.sleep(0.05)
    raise AssertionError(f"timeout waiting for job to finish; last={last}")


def _wait_exists(path: Path, *, want: bool, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists() is want:
            return
        time.sleep(0.05)
    raise AssertionError(f"expected exists={want} for {path}")


def main() -> int:
    port = 8016
    httpd = _start_server(port)
    try:
        time.sleep(0.1)

        text = "第1章\n\n你好...\n" * 200

        # Default behavior: cleanup after success.
        job_id = _create_job(
            port,
            {
                "max_chunk_chars": "2000",
                "llm_enabled": "0",
                "cleanup_debug_dir": "1",
                "suffix": "_rev",
            },
            text,
        )
        assert job_id
        st = _wait_done(port, job_id)
        assert st.get("state") == "done"
        _wait_exists(JOBS_DIR / job_id, want=False)

        # Opt-out: keep debug directory after success.
        job_id = _create_job(
            port,
            {
                "max_chunk_chars": "2000",
                "llm_enabled": "0",
                "cleanup_debug_dir": "0",
                "suffix": "_rev",
            },
            text,
        )
        assert job_id
        st = _wait_done(port, job_id)
        assert st.get("state") == "done"
        _wait_exists(JOBS_DIR / job_id, want=True)
        return 0
    finally:
        httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

