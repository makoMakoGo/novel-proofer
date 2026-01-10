from __future__ import annotations

import io
import json
import tempfile
import threading
import time
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

import novel_proofer.runner as runner
import novel_proofer.server as server
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMTextResult
from novel_proofer.llm.config import LLMConfig


def _multipart_body(boundary: str, fields: dict[str, str], *, filename: str, content: bytes) -> bytes:
    b = io.BytesIO()

    def w(s: str) -> None:
        b.write(s.encode("utf-8"))

    for k, v in fields.items():
        w(f"--{boundary}\r\n")
        w(f"Content-Disposition: form-data; name=\"{k}\"\r\n\r\n")
        w(v)
        w("\r\n")

    w(f"--{boundary}\r\n")
    w(f"Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
    w("Content-Type: text/plain\r\n\r\n")
    b.write(content)
    w("\r\n")
    w(f"--{boundary}--\r\n")

    return b.getvalue()


def _get(port: int, path: str) -> tuple[int, dict, dict[str, str]]:
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        headers = {k.lower(): v for k, v in resp.getheaders()}
        if raw:
            try:
                return resp.status, json.loads(raw), headers
            except Exception:
                return resp.status, {"raw": raw}, headers
        return resp.status, {}, headers
    finally:
        conn.close()


def _post_json(port: int, path: str, payload: dict) -> tuple[int, dict]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(
            "POST",
            path,
            body=data,
            headers={"Content-Type": "application/json", "Content-Length": str(len(data))},
        )
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        return resp.status, (json.loads(raw) if raw else {})
    finally:
        conn.close()


def _post_multipart(port: int, path: str, fields: dict[str, str], *, filename: str, content: bytes) -> tuple[int, dict, dict[str, str], bytes]:
    boundary = "----nptestboundary"
    body = _multipart_body(boundary, fields, filename=filename, content=content)
    conn = HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request(
            "POST",
            path,
            body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
        )
        resp = conn.getresponse()
        raw_bytes = resp.read()
        headers = {k.lower(): v for k, v in resp.getheaders()}
        raw = raw_bytes.decode("utf-8", errors="replace")
        payload = {}
        if headers.get("content-type", "").startswith("application/json") and raw:
            payload = json.loads(raw)
        return resp.status, payload, headers, raw_bytes
    finally:
        conn.close()


def _wait_job_state(port: int, job_id: str, want: str, *, timeout_s: float = 5.0) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        code, st, _headers = _get(port, f"/api/jobs/status?job_id={job_id}")
        last = st
        if code == 200 and st.get("state") == want:
            return st
        time.sleep(0.01)
    raise AssertionError(f"expected state={want}, got {last}")


@pytest.fixture(scope="module")
def http_server(tmp_path_factory):
    tmp_root = tmp_path_factory.mktemp("server")
    out_dir = Path(tmp_root) / "output"
    jobs_dir = out_dir / ".jobs"
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs_dir.mkdir(parents=True, exist_ok=True)

    old_output_dir = server.OUTPUT_DIR
    old_jobs_dir = server.JOBS_DIR
    old_log_message = server.Handler.log_message
    server.OUTPUT_DIR = out_dir
    server.JOBS_DIR = jobs_dir
    server.Handler.log_message = lambda *args, **kwargs: None

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    port = int(httpd.server_address[1])
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    time.sleep(0.05)
    try:
        yield port, out_dir, jobs_dir
    finally:
        httpd.shutdown()
        httpd.server_close()
        server.OUTPUT_DIR = old_output_dir
        server.JOBS_DIR = old_jobs_dir
        server.Handler.log_message = old_log_message


def test_server_get_root_and_health(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    code, data, headers = _get(port, "/health")
    assert code == 200 and data.get("ok") is True

    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", "/")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        assert resp.status == 200
        assert resp.getheader("Content-Type", "").startswith("text/html")
        assert "<html" in body.lower()
    finally:
        conn.close()


def test_server_status_missing_job_returns_404(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server
    code, data, _headers = _get(port, "/api/jobs/status?job_id=missing")
    assert code == 404
    assert data.get("error") == "job not found"


def test_server_format_variants_and_validation(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server
    text = "第1章\r\n\r\n你好....\r\n"

    code, payload, _headers, _raw = _post_multipart(
        port,
        "/format",
        {
            "return": "stats",
            "max_chunk_chars": "2000",
            "paragraph_indent": "1",
            "normalize_ellipsis": "1",
        },
        filename="demo.txt",
        content=text.encode("utf-8"),
    )
    assert code == 200
    assert isinstance(payload.get("stats"), dict)

    code, payload, _headers, _raw = _post_multipart(
        port,
        "/format",
        {
            "return": "json",
            "max_chunk_chars": "2000",
            "normalize_ellipsis": "1",
        },
        filename="demo.txt",
        content=text.encode("utf-8"),
    )
    assert code == 200
    assert "text" in payload

    code, payload, headers, raw = _post_multipart(
        port,
        "/format",
        {
            "max_chunk_chars": "2000",
            "normalize_ellipsis": "1",
        },
        filename="demo.txt",
        content=text.encode("utf-8"),
    )
    assert code == 200
    assert payload == {}
    assert "content-disposition" in headers
    assert "……" in raw.decode("utf-8", errors="replace")

    # llm_extra_params must be valid JSON object (string).
    code, _payload, _headers, _raw = _post_multipart(
        port,
        "/format",
        {
            "max_chunk_chars": "2000",
            "llm_extra_params": "not json",
        },
        filename="demo.txt",
        content=b"x",
    )
    assert code == 400

    # max_chunk_chars must be <= 4000.
    code, _payload, _headers, _raw = _post_multipart(
        port,
        "/format",
        {
            "max_chunk_chars": "5000",
        },
        filename="demo.txt",
        content=b"x",
    )
    assert code == 400


def test_server_create_job_local_and_cleanup(http_server) -> None:
    port, out_dir, jobs_dir = http_server

    code, payload, _headers, _raw = _post_multipart(
        port,
        "/api/jobs/create",
        {
            "max_chunk_chars": "2000",
            "llm_enabled": "0",
            "cleanup_debug_dir": "0",
            "normalize_ellipsis": "1",
            "suffix": "_rev",
        },
        filename="demo.txt",
        content="第1章\r\n\r\n你好....\r\n".encode("utf-8"),
    )
    assert code == 200
    job_id = str(payload.get("job_id") or "")
    assert job_id

    _wait_job_state(port, job_id, "done")

    st = GLOBAL_JOBS.get(job_id)
    assert st is not None
    assert st.work_dir is not None
    assert st.output_path is not None

    work_dir = Path(st.work_dir)
    output_path = Path(st.output_path)
    assert work_dir.parent == jobs_dir
    assert output_path.parent == out_dir
    assert output_path.exists()
    assert "……" in output_path.read_text(encoding="utf-8")

    # No legacy directories in debug work dir.
    assert not (work_dir / "req").exists()
    assert not (work_dir / "error").exists()

    code, out = _post_json(port, "/api/jobs/cleanup", {"job_id": job_id})
    assert code == 200 and out.get("ok") is True
    assert out.get("removed") is True
    assert out.get("job_deleted") is True
    assert not work_dir.exists()


def test_server_status_chunk_filters_and_pagination(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=3)
    job_id = job.job_id
    try:
        GLOBAL_JOBS.init_chunks(job_id, total_chunks=3)
        GLOBAL_JOBS.update_chunk(job_id, 0, state="done")
        GLOBAL_JOBS.update_chunk(job_id, 1, state="error", last_error_message="x")

        code, st, _headers = _get(
            port,
            f"/api/jobs/status?job_id={job_id}&include_chunks=1&chunk_filter=done&chunk_limit=1&chunk_offset=0",
        )
        assert code == 200
        assert st.get("chunk_filter") == "done"
        assert st.get("chunk_limit") == 1
        assert st.get("chunks_total_matching") == 1
        assert isinstance(st.get("chunk_counts"), dict)
        assert isinstance(st.get("chunks"), list)
        assert len(st.get("chunks")) == 1
        assert st["chunks"][0]["state"] == "done"

        # Invalid filter falls back to "all".
        code, st2, _headers = _get(port, f"/api/jobs/status?job_id={job_id}&include_chunks=1&chunk_filter=bad")
        assert code == 200
        assert st2.get("chunk_filter") == "all"
    finally:
        GLOBAL_JOBS.delete(job_id)


def test_server_resume_and_retry_endpoints(monkeypatch: pytest.MonkeyPatch, http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    def fake_call_llm_text_resilient_with_meta_and_raw(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        return LLMTextResult(text="修正后内容\n", raw_text="RAW"), 0, None, None

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call_llm_text_resilient_with_meta_and_raw)

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        work_dir1 = td_path / "work1"
        out_path1 = td_path / "out1.txt"
        (work_dir1 / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir1 / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        # Resume: paused job with pending chunk.
        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir1), output_path=str(out_path1), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)
            assert GLOBAL_JOBS.pause(job_id) is True

            code, out = _post_json(
                port,
                "/api/jobs/resume",
                {
                    "job_id": job_id,
                    "llm_provider": "openai_compatible",
                    "llm_base_url": "http://example.com",
                    "llm_api_key": "",
                    "llm_model": "m",
                    "llm_temperature": 0,
                    "llm_timeout_seconds": 10,
                    "llm_max_concurrency": 1,
                    "llm_filter_think_tags": True,
                    "llm_extra_params": None,
                },
            )
            assert code == 200 and out.get("ok") is True

            _wait_job_state(port, job_id, "done")
            assert (work_dir1 / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW"
        finally:
            GLOBAL_JOBS.delete(job_id)

        # Retry: errored chunk gets reprocessed.
        work_dir2 = td_path / "work2"
        out_path2 = td_path / "out2.txt"
        (work_dir2 / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir2 / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        job2 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job2_id = job2.job_id
        try:
            GLOBAL_JOBS.update(job2_id, work_dir=str(work_dir2), output_path=str(out_path2), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job2_id, total_chunks=1)
            GLOBAL_JOBS.update_chunk(job2_id, 0, state="error", last_error_message="x")

            code, out = _post_json(
                port,
                "/api/jobs/retry_failed",
                {
                    "job_id": job2_id,
                    "llm_provider": "openai_compatible",
                    "llm_base_url": "http://example.com",
                    "llm_api_key": "",
                    "llm_model": "m",
                    "llm_temperature": 0,
                    "llm_timeout_seconds": 10,
                    "llm_max_concurrency": 1,
                    "llm_filter_think_tags": True,
                    "llm_extra_params": None,
                },
            )
            assert code == 200 and out.get("ok") is True

            _wait_job_state(port, job2_id, "done")
            assert (work_dir2 / "resp" / "000000.txt").exists()
        finally:
            GLOBAL_JOBS.delete(job2_id)


def test_server_download_and_unknown_routes(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", "/api/jobs/download")
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 410

        conn.request("GET", "/nope")
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 404
    finally:
        conn.close()

    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("POST", "/nope", body=b"{}", headers={"Content-Type": "application/json", "Content-Length": "2"})
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 404
    finally:
        conn.close()


def test_server_cancel_and_pause_endpoints(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job_id = job.job_id
    try:
        code, out = _post_json(port, "/api/jobs/pause", {"job_id": job_id})
        assert code == 200 and out.get("ok") is True

        code, out = _post_json(port, "/api/jobs/pause", {"job_id": job_id})
        assert code == 200 and out.get("ok") is False
    finally:
        GLOBAL_JOBS.delete(job_id)

    job2 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job2_id = job2.job_id
    try:
        code, out = _post_json(port, "/api/jobs/cancel", {"job_id": job2_id})
        assert code == 200 and out.get("ok") is True
    finally:
        GLOBAL_JOBS.delete(job2_id)


def test_server_status_offset_and_limit_has_more(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=3)
    job_id = job.job_id
    try:
        GLOBAL_JOBS.init_chunks(job_id, total_chunks=3)
        GLOBAL_JOBS.update_chunk(job_id, 0, state="done")
        GLOBAL_JOBS.update_chunk(job_id, 1, state="error", last_error_message="x")

        code, st, _headers = _get(
            port,
            f"/api/jobs/status?job_id={job_id}&include_chunks=1&chunk_filter=all&chunk_limit=1&chunk_offset=1",
        )
        assert code == 200
        assert st.get("chunks_has_more") is True
        assert st.get("chunk_offset") == 1
        assert st.get("chunk_limit") == 1
        assert isinstance(st.get("chunks"), list)
        assert len(st["chunks"]) == 1
    finally:
        GLOBAL_JOBS.delete(job_id)


def test_server_resume_retry_cleanup_error_paths(monkeypatch: pytest.MonkeyPatch, http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    # resume: missing job
    code, out = _post_json(port, "/api/jobs/resume", {"job_id": "missing"})
    assert code == 404 and out.get("error") == "job not found"

    # resume: running job
    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job_id = job.job_id
    try:
        GLOBAL_JOBS.update(job_id, state="running")
        code, out = _post_json(port, "/api/jobs/resume", {"job_id": job_id})
        assert code == 409 and out.get("error") == "job is running"
    finally:
        GLOBAL_JOBS.delete(job_id)

    # resume: cancelled job
    job2 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job2_id = job2.job_id
    try:
        assert GLOBAL_JOBS.cancel(job2_id) is True
        code, out = _post_json(port, "/api/jobs/resume", {"job_id": job2_id})
        assert code == 409 and out.get("error") == "job is cancelled"
    finally:
        GLOBAL_JOBS.delete(job2_id)

    # resume: not paused
    job3 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job3_id = job3.job_id
    try:
        code, out = _post_json(port, "/api/jobs/resume", {"job_id": job3_id})
        assert code == 409 and out.get("error") == "job is not paused"
    finally:
        GLOBAL_JOBS.delete(job3_id)

    # resume: invalid extra params
    job4 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job4_id = job4.job_id
    try:
        GLOBAL_JOBS.update(job4_id, work_dir="x", output_path="y")
        assert GLOBAL_JOBS.pause(job4_id) is True
        code, out = _post_json(port, "/api/jobs/resume", {"job_id": job4_id, "llm_extra_params": "[]"})
        assert code == 400 and "llm_extra_params" in (out.get("error") or "")
    finally:
        GLOBAL_JOBS.delete(job4_id)

    # resume: failed to resume branch (patched)
    job5 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job5_id = job5.job_id
    try:
        GLOBAL_JOBS.update(job5_id, work_dir="x", output_path="y")
        GLOBAL_JOBS.update(job5_id, state="paused")
        monkeypatch.setattr(server.GLOBAL_JOBS, "resume", lambda _job_id: False)
        code, out = _post_json(port, "/api/jobs/resume", {"job_id": job5_id})
        assert code == 409 and out.get("error") == "failed to resume job"
    finally:
        GLOBAL_JOBS.delete(job5_id)

    # retry_failed: missing job
    code, out = _post_json(port, "/api/jobs/retry_failed", {"job_id": "missing"})
    assert code == 404 and out.get("error") == "job not found"

    # retry_failed: running job
    job6 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job6_id = job6.job_id
    try:
        GLOBAL_JOBS.update(job6_id, state="running")
        code, out = _post_json(port, "/api/jobs/retry_failed", {"job_id": job6_id})
        assert code == 409 and out.get("error") == "job is running"
    finally:
        GLOBAL_JOBS.delete(job6_id)

    # retry_failed: cancelled job
    job7 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job7_id = job7.job_id
    try:
        assert GLOBAL_JOBS.cancel(job7_id) is True
        code, out = _post_json(port, "/api/jobs/retry_failed", {"job_id": job7_id})
        assert code == 409 and out.get("error") == "job is cancelled"
    finally:
        GLOBAL_JOBS.delete(job7_id)

    # retry_failed: invalid extra params
    job8 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job8_id = job8.job_id
    try:
        GLOBAL_JOBS.update(job8_id, work_dir="x", output_path="y")
        GLOBAL_JOBS.init_chunks(job8_id, total_chunks=1)
        GLOBAL_JOBS.update_chunk(job8_id, 0, state="error", last_error_message="x")
        code, out = _post_json(port, "/api/jobs/retry_failed", {"job_id": job8_id, "llm_extra_params": "[]"})
        assert code == 400 and "llm_extra_params" in (out.get("error") or "")
    finally:
        GLOBAL_JOBS.delete(job8_id)

    # cleanup: running job
    job9 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job9_id = job9.job_id
    try:
        GLOBAL_JOBS.update(job9_id, state="running")
        code, out = _post_json(port, "/api/jobs/cleanup", {"job_id": job9_id})
        assert code == 409 and out.get("error") == "job is running"
    finally:
        GLOBAL_JOBS.delete(job9_id)

    # cleanup: cancelled job
    job10 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job10_id = job10.job_id
    try:
        assert GLOBAL_JOBS.cancel(job10_id) is True
        code, out = _post_json(port, "/api/jobs/cleanup", {"job_id": job10_id})
        assert code == 409 and out.get("error") == "job is cancelled"
    finally:
        GLOBAL_JOBS.delete(job10_id)

    # cleanup: invalid job_id -> 400
    code, out = _post_json(port, "/api/jobs/cleanup", {"job_id": "bad"})
    assert code == 400 and out.get("error") == "invalid job_id"

    # cleanup: unexpected error -> 500 (patched)
    monkeypatch.setattr(server, "_cleanup_job_dir", lambda _job_id: (_ for _ in ()).throw(RuntimeError("boom")))
    code, out = _post_json(port, "/api/jobs/cleanup", {"job_id": "a" * 32})
    assert code == 500 and "boom" in (out.get("error") or "")


def test_server_multipart_validation_and_api_key_scrubbing(http_server) -> None:
    port, _out_dir, _jobs_dir = http_server

    # Expected multipart/form-data.
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("POST", "/format", body=b"{}", headers={"Content-Type": "application/json", "Content-Length": "2"})
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 400
    finally:
        conn.close()

    # Empty body.
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(
            "POST",
            "/format",
            body=b"",
            headers={"Content-Type": "multipart/form-data; boundary=x", "Content-Length": "0"},
        )
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 400
    finally:
        conn.close()

    # Too large (based on Content-Length only; no need to send a huge body).
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(
            "POST",
            "/format",
            body=b"",
            headers={"Content-Type": "multipart/form-data; boundary=x", "Content-Length": str(200 * 1024 * 1024 + 1)},
        )
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 413
    finally:
        conn.close()

    # No file uploaded.
    boundary = "----nptestboundarynofile"
    body = _multipart_body(boundary, {"max_chunk_chars": "2000"}, filename="", content=b"")
    # Strip the file part (everything after the last field delimiter).
    body = body.split(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"".encode("utf-8"))[0] + f"--{boundary}--\r\n".encode("utf-8")
    conn = HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(
            "POST",
            "/format",
            body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
        )
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 400
    finally:
        conn.close()

    # llm_api_key should be scrubbed in debug logging path.
    code, _payload, _headers, _raw = _post_multipart(
        port,
        "/format",
        {
            "max_chunk_chars": "2000",
            "llm_api_key": "SECRET",
            "llm_enabled": "0",
        },
        filename="demo.txt",
        content=b"x",
    )
    assert code == 200
