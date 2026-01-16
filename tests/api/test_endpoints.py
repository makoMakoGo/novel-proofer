from __future__ import annotations

import json
import tempfile
import time
from contextlib import suppress
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import novel_proofer.api as api
import novel_proofer.runner as runner
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMTextResult


def _wait_job_done(client: TestClient, job_id: str, *, timeout_seconds: float = 5.0) -> dict:
    deadline = time.time() + timeout_seconds
    last: dict = {}
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
            # Phase 1: validate (ends paused, ready to process)
            st1 = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (st1.get("job") or {}).get("state") == "paused"
            assert (st1.get("job") or {}).get("phase") == "process"

            # Phase 2: process (ends paused, ready to merge)
            r2 = client.post(
                f"/api/v1/jobs/{job_id}/resume",
                json={"llm": {"base_url": "http://example.com", "model": "m", "max_concurrency": 1}},
            )
            assert r2.status_code == 200, r2.text
            st2 = _wait_job_done(client, job_id, timeout_seconds=10.0)
            assert (st2.get("job") or {}).get("state") == "paused"
            assert (st2.get("job") or {}).get("phase") == "merge"

            # Phase 3: merge (ends done, output exists)
            r3 = client.post(f"/api/v1/jobs/{job_id}/merge", json={"cleanup_debug_dir": True})
            assert r3.status_code == 200, r3.text
            final = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (final.get("job") or {}).get("state") == "done"
            assert (final.get("job") or {}).get("phase") == "done"

            output_filename = (final.get("job") or {}).get("output_filename")
            assert isinstance(output_filename, str) and output_filename
            out_path = out_dir / output_filename
            assert out_path.exists()
            expected = out_path.read_text(encoding="utf-8")
            assert expected.strip() != ""

            dl = client.get(f"/api/v1/jobs/{job_id}/download")
            assert dl.status_code == 200, dl.text
            assert dl.text == expected
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
            st1 = _wait_job_done(client, job_id, timeout_seconds=10.0)
            assert (st1.get("job") or {}).get("state") == "paused"
            assert (st1.get("job") or {}).get("phase") == "process"

            client.post(
                f"/api/v1/jobs/{job_id}/resume",
                json={"llm": {"base_url": "http://example.com", "model": "m", "max_concurrency": 1}},
            )
            st2 = _wait_job_done(client, job_id, timeout_seconds=10.0)
            assert (st2.get("job") or {}).get("state") == "paused"
            assert (st2.get("job") or {}).get("phase") == "merge"

            client.post(f"/api/v1/jobs/{job_id}/merge", json={"cleanup_debug_dir": True})
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


def test_create_job_llm_enabled_requires_base_url_and_model(monkeypatch: pytest.MonkeyPatch):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out_dir = base / "output"
        jobs_dir = out_dir / ".jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(api, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

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
            # Validate succeeds without LLM config.
            st1 = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (st1.get("job") or {}).get("state") == "paused"
            assert (st1.get("job") or {}).get("phase") == "process"

            # Processing requires base_url/model and should fail when empty.
            client.post(f"/api/v1/jobs/{job_id}/resume", json={"llm": {"base_url": "", "model": ""}})
            st2 = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (st2.get("job") or {}).get("state") == "error"
        finally:
            GLOBAL_JOBS.delete(str(job_id))


def test_job_actions_pause_resume(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(api.app)

    # Pause -> Resume (avoid background runner side effects)
    monkeypatch.setattr(api, "resume_paused_job", lambda *_a, **_k: None)
    job2 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        # Ensure resume uses resume_paused_job (not validate stage).
        GLOBAL_JOBS.update(job2.job_id, phase="process")
        r = client.post(f"/api/v1/jobs/{job2.job_id}/pause")
        assert r.status_code == 200
        assert r.json().get("ok") is True
        st = GLOBAL_JOBS.get(job2.job_id)
        assert st is not None and st.state == "paused"

        r2 = client.post(
            f"/api/v1/jobs/{job2.job_id}/resume", json={"llm": {"base_url": "http://example.com", "model": "m"}}
        )
        assert r2.status_code == 200
        assert r2.json().get("ok") is True
        st2 = GLOBAL_JOBS.get(job2.job_id)
        assert st2 is not None and st2.state in {"queued", "running"}
    finally:
        GLOBAL_JOBS.delete(job2.job_id)


def test_job_input_stats_endpoint(monkeypatch: pytest.MonkeyPatch):
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out_dir = base / "output"
        jobs_dir = out_dir / ".jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(api, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

        client = TestClient(api.app)
        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        try:
            api._write_input_cache(job.job_id, "a b\n　　c\n")
            r = client.get(f"/api/v1/jobs/{job.job_id}/input-stats")
            assert r.status_code == 200, r.text
            data = r.json()
            assert data.get("job_id") == job.job_id
            assert data.get("input_chars") == 3

            # Fallback: if input cache is missing, derive from debug pre/ chunks.
            (out_dir / ".inputs" / f"{job.job_id}.txt").unlink(missing_ok=True)
            job_dir = jobs_dir / job.job_id
            pre_dir = job_dir / "pre"
            pre_dir.mkdir(parents=True, exist_ok=True)
            (pre_dir / "000000.txt").write_text("a b\n", encoding="utf-8")
            (pre_dir / "000001.txt").write_text("　　c\n", encoding="utf-8")
            GLOBAL_JOBS.update(job.job_id, work_dir=str(job_dir))

            r2 = client.get(f"/api/v1/jobs/{job.job_id}/input-stats")
            assert r2.status_code == 200, r2.text
            data2 = r2.json()
            assert data2.get("job_id") == job.job_id
            assert data2.get("input_chars") == 3
        finally:
            GLOBAL_JOBS.delete(job.job_id)

    # Retry-failed (avoid background runner side effects)
    monkeypatch.setattr(api, "retry_failed_chunks", lambda *_a, **_k: None)
    job3 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        GLOBAL_JOBS.update(job3.job_id, phase="process")
        GLOBAL_JOBS.init_chunks(job3.job_id, total_chunks=2)
        GLOBAL_JOBS.update_chunk(job3.job_id, 0, state="done")
        GLOBAL_JOBS.update_chunk(job3.job_id, 1, state="error")
        GLOBAL_JOBS.update(job3.job_id, state="error")
        r = client.post(
            f"/api/v1/jobs/{job3.job_id}/retry-failed",
            json={"llm": {"base_url": "http://example.com", "model": "m"}},
        )
        assert r.status_code == 200
        assert r.json().get("ok") is True
        st3 = GLOBAL_JOBS.get(job3.job_id)
        assert st3 is not None and st3.state == "queued"
        assert any(c.state == "pending" for c in st3.chunk_statuses)
    finally:
        GLOBAL_JOBS.delete(job3.job_id)

    # Cleanup-debug deletes job + debug dir.
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        out_dir = base / "output"
        jobs_dir = out_dir / ".jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(api, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(api, "JOBS_DIR", jobs_dir)

        job4 = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job4_dir = jobs_dir / job4.job_id
        job4_dir.mkdir(parents=True, exist_ok=True)
        (job4_dir / "x.txt").write_text("x", encoding="utf-8")
        GLOBAL_JOBS.update(job4.job_id, state="done")

        input_cache = out_dir / ".inputs" / f"{job4.job_id}.txt"
        input_cache.parent.mkdir(parents=True, exist_ok=True)
        input_cache.write_text("cached", encoding="utf-8")

        r = client.post(f"/api/v1/jobs/{job4.job_id}/cleanup-debug")
        assert r.status_code == 200
        assert r.json().get("ok") is True
        assert not job4_dir.exists()
        assert not input_cache.exists()
        assert GLOBAL_JOBS.get(job4.job_id) is None

        # After cleanup, rerun-all is unavailable.
        r2 = client.post(f"/api/v1/jobs/{job4.job_id}/rerun-all", json={"format": {"max_chunk_chars": 2000}})
        assert r2.status_code == 404


def test_pause_only_allowed_in_process_phase() -> None:
    client = TestClient(api.app)

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        r0 = client.post(f"/api/v1/jobs/{job.job_id}/pause")
        assert r0.status_code == 409

        GLOBAL_JOBS.update(job.job_id, state="running", phase="merge")
        r1 = client.post(f"/api/v1/jobs/{job.job_id}/pause")
        assert r1.status_code == 409

        GLOBAL_JOBS.update(job.job_id, state="queued", phase="process")
        r2 = client.post(f"/api/v1/jobs/{job.job_id}/pause")
        assert r2.status_code == 200, r2.text
    finally:
        GLOBAL_JOBS.delete(job.job_id)


def test_list_jobs_includes_created_job() -> None:
    client = TestClient(api.app)

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        r = client.get("/api/v1/jobs")
        assert r.status_code == 200, r.text
        jobs = (r.json() or {}).get("jobs") or []
        assert any(j.get("id") == job.job_id for j in jobs)

        r2 = client.get("/api/v1/jobs?state=queued")
        assert r2.status_code == 200, r2.text
        jobs2 = (r2.json() or {}).get("jobs") or []
        assert any(j.get("id") == job.job_id for j in jobs2)
    finally:
        GLOBAL_JOBS.delete(job.job_id)


def test_reset_job_deletes_job() -> None:
    client = TestClient(api.app)

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        r = client.post(f"/api/v1/jobs/{job.job_id}/reset")
        assert r.status_code == 200, r.text
        assert r.json().get("ok") is True

        r2 = client.get(f"/api/v1/jobs/{job.job_id}")
        assert r2.status_code == 404, r2.text
        assert GLOBAL_JOBS.get(job.job_id) is None
    finally:
        GLOBAL_JOBS.delete(job.job_id)


def test_resume_job_returns_409_when_background_submit_rejects(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(api.app)

    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    try:
        GLOBAL_JOBS.update(job.job_id, phase="process")
        r0 = client.post(f"/api/v1/jobs/{job.job_id}/pause")
        assert r0.status_code == 200

        def _boom(*_a, **_k):
            raise ValueError("job_id is already in flight")

        monkeypatch.setattr(api, "submit_background_job", _boom)

        r = client.post(
            f"/api/v1/jobs/{job.job_id}/resume",
            json={"llm": {"base_url": "http://example.com", "model": "m"}},
        )
        assert r.status_code == 409
        st = GLOBAL_JOBS.get(job.job_id)
        assert st is not None and st.state == "paused"
    finally:
        GLOBAL_JOBS.delete(job.job_id)


def test_llm_settings_get_put_preserves_unknown_lines(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(api.app)
    with tempfile.TemporaryDirectory() as td:
        dotenv = Path(td) / ".env"
        dotenv.write_text("# keep\nFOO=bar\nNOVEL_PROOFER_LLM_BASE_URL=http://old\n", encoding="utf-8")
        monkeypatch.setenv("NOVEL_PROOFER_DOTENV_PATH", str(dotenv))

        r0 = client.get("/api/v1/settings/llm")
        assert r0.status_code == 200

        payload = {
            "llm": {
                "base_url": "http://new",
                "model": "m",
                "api_key": "k",
                "temperature": 0.1,
                "timeout_seconds": 12,
                "max_concurrency": 3,
                "extra_params": {"max_tokens": 4096},
            }
        }
        r1 = client.put("/api/v1/settings/llm", json=payload)
        assert r1.status_code == 200, r1.text
        body = r1.json()
        assert (body.get("llm") or {}).get("base_url") == "http://new"
        assert (body.get("llm") or {}).get("model") == "m"
        assert (body.get("llm") or {}).get("api_key") == "k"
        assert (body.get("llm") or {}).get("extra_params", {}).get("max_tokens") == 4096

        raw = dotenv.read_text(encoding="utf-8")
        assert "# keep" in raw
        assert "FOO=bar" in raw
        assert "NOVEL_PROOFER_LLM_BASE_URL=http://new" in raw


def test_rerun_all_creates_new_job_without_reupload(monkeypatch: pytest.MonkeyPatch):
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
        job_id = (r.json().get("job") or {}).get("id")
        assert isinstance(job_id, str) and len(job_id) == 32

        try:
            st1 = _wait_job_done(client, job_id, timeout_seconds=5.0)
            assert (st1.get("job") or {}).get("state") == "paused"
            assert (st1.get("job") or {}).get("phase") == "process"

            input_cache = out_dir / ".inputs" / f"{job_id}.txt"
            assert input_cache.exists()

            r2 = client.post(f"/api/v1/jobs/{job_id}/rerun-all", json=options)
            assert r2.status_code == 201, r2.text
            job_id2 = (r2.json().get("job") or {}).get("id")
            assert isinstance(job_id2, str) and len(job_id2) == 32 and job_id2 != job_id

            st2 = _wait_job_done(client, job_id2, timeout_seconds=5.0)
            assert (st2.get("job") or {}).get("state") == "paused"
            assert (st2.get("job") or {}).get("phase") == "process"

            # Process + merge rerun job to ensure output path is produced.
            client.post(
                f"/api/v1/jobs/{job_id2}/resume",
                json={"llm": {"base_url": "http://example.com", "model": "m", "max_concurrency": 1}},
            )
            st3 = _wait_job_done(client, job_id2, timeout_seconds=5.0)
            assert (st3.get("job") or {}).get("state") == "paused"
            assert (st3.get("job") or {}).get("phase") == "merge"

            client.post(f"/api/v1/jobs/{job_id2}/merge", json={"cleanup_debug_dir": True})
            final2 = _wait_job_done(client, job_id2, timeout_seconds=5.0)
            assert (final2.get("job") or {}).get("state") == "done"
            assert (out_dir / (final2.get("job") or {}).get("output_filename")).exists()
        finally:
            GLOBAL_JOBS.delete(str(job_id))
            # Best-effort: rerun job may or may not exist if creation failed.
            with suppress(Exception):
                GLOBAL_JOBS.delete(str(job_id2))  # type: ignore[name-defined]
