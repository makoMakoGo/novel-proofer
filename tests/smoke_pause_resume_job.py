"""Smoke test for pause/resume job flow.

Runs:
- A short-lived novel_proofer server in-process
- A fake OpenAI-compatible SSE LLM server
"""

from __future__ import annotations

import io
import json
import sys
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from novel_proofer.server import Handler  # noqa: E402


class _FakeOpenAILLMHandler(BaseHTTPRequestHandler):
    server_version = "FakeLLM/0.1"

    def log_message(self, fmt: str, *args) -> None:  # noqa: D401
        # keep tests quiet
        return

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/chat/completions":
            self.send_error(404, "not found")
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8", errors="replace") or "{}")
        except Exception:
            payload = {}

        user_text = ""
        try:
            msgs = payload.get("messages") or []
            user_text = str(msgs[-1].get("content") or "")
        except Exception:
            user_text = ""

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.end_headers()

        # Stream the user text back in chunks so runner validation passes (ratio ~1.0),
        # and add a small delay so pause is observable.
        if not user_text:
            user_text = "OK"

        parts = []
        step = max(1, len(user_text) // 4)
        for i in range(0, len(user_text), step):
            parts.append(user_text[i : i + step])

        for p in parts:
            data = json.dumps({"choices": [{"delta": {"content": p}}]}, ensure_ascii=False)
            self.wfile.write(f"data: {data}\n".encode("utf-8"))
            self.wfile.flush()
            time.sleep(0.05)

        self.wfile.write(b"data: [DONE]\n")
        self.wfile.flush()


def _start_server(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return httpd


def _start_fake_llm(port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer(("127.0.0.1", port), _FakeOpenAILLMHandler)
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
    boundary = "----npboundarypause"
    body = _multipart_body(boundary, fields, "file", "demo.txt", text.encode("utf-8"))

    conn = HTTPConnection("127.0.0.1", port, timeout=10)
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
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", f"/api/jobs/status?job_id={job_id}&include_chunks=0")
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _post_json(port: int, path: str, payload: dict) -> dict:
    conn = HTTPConnection("127.0.0.1", port, timeout=10)
    data = json.dumps(payload).encode("utf-8")
    conn.request(
        "POST",
        path,
        body=data,
        headers={"Content-Type": "application/json", "Content-Length": str(len(data))},
    )
    resp = conn.getresponse()
    raw = resp.read().decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw, "status": resp.status}


def main() -> int:
    port = 8017
    llm_port = 8018
    llm_httpd = _start_fake_llm(llm_port)
    httpd = _start_server(port)
    try:
        time.sleep(0.1)

        text = ("第1章\n\n" + ("你好...\n" * 200)) * 50
        fields = {
            "max_chunk_chars": "2000",
            "llm_enabled": "1",
            "llm_provider": "openai_compatible",
            "llm_base_url": f"http://127.0.0.1:{llm_port}",
            "llm_model": "m",
            "llm_timeout_seconds": "10",
            "llm_max_concurrency": "1",
            "cleanup_debug_dir": "0",
            "suffix": "_rev",
        }

        job_id = _create_job(port, fields, text)
        assert job_id

        # Wait for job to start and make some progress.
        last = {}
        for _ in range(200):
            last = _get_status(port, job_id)
            if last.get("state") == "running" and int(last.get("done_chunks") or 0) >= 1:
                break
            time.sleep(0.05)
        else:
            raise AssertionError(f"expected running with progress, got {last}")

        out = _post_json(port, "/api/jobs/pause", {"job_id": job_id})
        assert out.get("ok") is True

        # Wait for paused.
        paused_state = {}
        for _ in range(200):
            paused_state = _get_status(port, job_id)
            if paused_state.get("state") == "paused":
                break
            if paused_state.get("state") in {"done", "error", "cancelled"}:
                raise AssertionError(f"expected paused, got {paused_state}")
            time.sleep(0.05)

        assert paused_state.get("state") == "paused"
        assert int(paused_state.get("done_chunks") or 0) < int(paused_state.get("total_chunks") or 0)

        out = _post_json(
            port,
            "/api/jobs/resume",
            {
                "job_id": job_id,
                "llm_provider": "openai_compatible",
                "llm_base_url": f"http://127.0.0.1:{llm_port}",
                "llm_api_key": "",
                "llm_model": "m",
                "llm_temperature": 0,
                "llm_timeout_seconds": 10,
                "llm_max_concurrency": 1,
                "llm_filter_think_tags": True,
                "llm_extra_params": None,
            },
        )
        assert out.get("ok") is True

        # Wait done.
        done_state = {}
        for _ in range(400):
            done_state = _get_status(port, job_id)
            if done_state.get("state") == "done":
                return 0
            if done_state.get("state") in {"error", "cancelled"}:
                raise AssertionError(f"expected done, got {done_state}")
            time.sleep(0.05)

        raise AssertionError(f"timeout waiting for done; last={done_state}")
    finally:
        httpd.shutdown()
        llm_httpd.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())

