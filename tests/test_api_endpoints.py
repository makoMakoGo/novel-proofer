from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import novel_proofer.api as api
import novel_proofer.runner as runner
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMTextResult


def _wait_job_done(client: TestClient, job_id: str, *, timeout_seconds: float = 5.0) -> dict:
    deadline = time.time() + timeout_seconds
    last: dict | None = None
    while time.time() < deadline:
        res = client.get(f"/api/v1/jobs/{job_id}?chunks=0")
        assert res.status_code == 200
        last = res.json()
        state = (last.get("job") or {}).get("state")
        if state in {"done", "error", "cancelled", "paused"}:
            return last
        time.sleep(0.05)
    raise AssertionError(f"job did not finish in time; last={last}")


def test_healthz_ok():
    client = TestClient(api.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_create_job_local_mode_writes_output_and_is_queryable(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        runner,
        "call_llm_text_resilient_with_meta_and_raw",
        lambda cfg, input_text, *, should_stop=None, on_retry=None: (
            LLMTextResult(text=input_text, raw_text="RAW"),
            0,
            None,
            None,
        ),
    )

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out_dir = base / "output"
        jobs_dir = out_dir / ".jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(api, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

        client = TestClient(api.app)
        options = {
            "format": {"max_chunk_chars": 2000},
            "llm": {"base_url": "http://example.com", "model": "m", "max_concurrency": 1},
            "output": {"suffix": "_rev", "cleanup_debug_dir": True},
        }

        r = client.post(
            "/api/v1/jobs",
            data={"options": json.dumps(options, ensure_ascii=False)},
            files={"file": ("in.txt", "第1章\n\n正文一。\n正文二。\n", "text/plain; charset=utf-8")},
        )
        assert r.status_code == 201, r.text
        data = r.json()
        job = data.get("job") or {}
        job_id = job.get("id")
        assert isinstance(job_id, str) and len(job_id) == 32

        try:
            final = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (final.get("job") or {}).get("state") == "done"

            # Output file exists under patched OUTPUT_DIR.
            output_filename = (final.get("job") or {}).get("output_filename")
            assert isinstance(output_filename, str) and output_filename
            out_path = out_dir / output_filename
            assert out_path.exists()
            assert out_path.read_text(encoding="utf-8").strip() != ""
        finally:
            # Best-effort cleanup: delete job from store to avoid cross-test bleed.
            GLOBAL_JOBS.delete(str(job_id))


def test_get_job_chunk_filter_and_paging(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        runner,
        "call_llm_text_resilient_with_meta_and_raw",
        lambda cfg, input_text, *, should_stop=None, on_retry=None: (
            LLMTextResult(text=input_text, raw_text="RAW"),
            0,
            None,
            None,
        ),
    )

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out_dir = base / "output"
        jobs_dir = out_dir / ".jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(api, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

        client = TestClient(api.app)

        # Create enough text to produce multiple chunks.
        text = ("a" * 120 + "\n") * 100  # ~12k chars including newlines
        options = {
            "format": {"max_chunk_chars": 2000},
            "llm": {"base_url": "http://example.com", "model": "m", "max_concurrency": 1},
            "output": {"suffix": "_rev"},
        }
        r = client.post(
            "/api/v1/jobs",
            data={"options": json.dumps(options, ensure_ascii=False)},
            files={"file": ("big.txt", text, "text/plain; charset=utf-8")},
        )
        assert r.status_code == 201, r.text
        job_id = (r.json().get("job") or {}).get("id")
        assert isinstance(job_id, str) and len(job_id) == 32

        try:
            final = _wait_job_done(client, job_id, timeout_seconds=10.0)
            assert (final.get("job") or {}).get("state") == "done"

            r2 = client.get(f"/api/v1/jobs/{job_id}?chunks=1&chunk_state=done&limit=1&offset=0")
            assert r2.status_code == 200
            data2 = r2.json()
            assert isinstance(data2.get("chunks"), list)
            assert len(data2["chunks"]) == 1
            assert data2.get("has_more") is True
            assert (data2.get("chunk_counts") or {}).get("done", 0) >= 2
        finally:
            GLOBAL_JOBS.delete(str(job_id))


def test_job_not_found_error_envelope():
    client = TestClient(api.app)
    r = client.get("/api/v1/jobs/" + ("a" * 32))
    assert r.status_code == 404
    body = r.json()
    assert body.get("error", {}).get("code") == "not_found"


def test_create_job_llm_enabled_requires_base_url_and_model():
    client = TestClient(api.app)
    options = {"format": {"max_chunk_chars": 2000}, "llm": {"base_url": "", "model": ""}}
    r = client.post(
        "/api/v1/jobs",
        data={"options": json.dumps(options, ensure_ascii=False)},
        files={"file": ("x.txt", "x\n", "text/plain; charset=utf-8")},
    )
    assert r.status_code == 201, r.text
    job_id = (r.json().get("job") or {}).get("id")
    assert isinstance(job_id, str) and len(job_id) == 32

    try:
        final = _wait_job_done(client, job_id, timeout_seconds=5.0)
        assert (final.get("job") or {}).get("state") == "error"
    finally:
        GLOBAL_JOBS.delete(str(job_id))


def test_job_actions_cancel_pause_resume_retry_cleanup(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(api.app)

    # Cancel
    job1 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        r = client.post(f"/api/v1/jobs/{job1.job_id}/cancel")
        assert r.status_code == 200
        assert r.json().get("ok") is True
        st = GLOBAL_JOBS.get(job1.job_id)
        assert st is not None and st.state == "cancelled"
    finally:
        GLOBAL_JOBS.delete(job1.job_id)

    # Pause -> Resume (avoid background runner side effects)
    monkeypatch.setattr(api, "resume_paused_job", lambda *_a, **_k: None)
    job2 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        r = client.post(f"/api/v1/jobs/{job2.job_id}/pause")
        assert r.status_code == 200
        assert r.json().get("ok") is True
        st = GLOBAL_JOBS.get(job2.job_id)
        assert st is not None and st.state == "paused"

        r2 = client.post(f"/api/v1/jobs/{job2.job_id}/resume", json={"llm": {"base_url": "http://example.com", "model": "m"}})
        assert r2.status_code == 200
        assert r2.json().get("ok") is True
        st2 = GLOBAL_JOBS.get(job2.job_id)
        assert st2 is not None and st2.state in {"queued", "running"}
    finally:
        GLOBAL_JOBS.delete(job2.job_id)

    # Retry-failed (avoid background runner side effects)
    monkeypatch.setattr(api, "retry_failed_chunks", lambda *_a, **_k: None)
    job3 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        GLOBAL_JOBS.update(job3.job_id, state="error")
        r = client.post(
            f"/api/v1/jobs/{job3.job_id}/retry-failed",
            json={"llm": {"base_url": "http://example.com", "model": "m"}},
        )
        assert r.status_code == 200
        assert r.json().get("ok") is True
    finally:
        GLOBAL_JOBS.delete(job3.job_id)

    # Cleanup-debug deletes job + debug dir.
    with tempfile.TemporaryDirectory() as td:
        jobs_dir = Path(td) / ".jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

        job4 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job4_dir = jobs_dir / job4.job_id
        job4_dir.mkdir(parents=True, exist_ok=True)
        (job4_dir / "x.txt").write_text("x", encoding="utf-8")
        GLOBAL_JOBS.update(job4.job_id, state="done")

        r = client.post(f"/api/v1/jobs/{job4.job_id}/cleanup-debug")
        assert r.status_code == 200
        assert r.json().get("ok") is True
        assert not job4_dir.exists()
        assert GLOBAL_JOBS.get(job4.job_id) is None
