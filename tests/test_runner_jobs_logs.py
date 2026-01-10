from __future__ import annotations

import tempfile
import threading
from pathlib import Path

import pytest

import novel_proofer.runner as runner
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMError, LLMTextResult
from novel_proofer.llm.config import LLMConfig


def _assert_no_legacy_log_dirs(work_dir: Path) -> None:
    assert not (work_dir / "req").exists()
    assert not (work_dir / "error").exists()


def _assert_resp_files(work_dir: Path, *, expect: bool) -> None:
    resp_dir = work_dir / "resp"
    if not expect:
        assert not resp_dir.exists()
        return

    assert resp_dir.is_dir()
    files = sorted([p.name for p in resp_dir.iterdir() if p.is_file()])
    assert files == ["000000.txt"]
    assert not any(name.endswith(".tmp") for name in files)
    assert not any("_" in name for name in files)


def test_llm_worker_success_writes_resp_index_file_only(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call_llm_text_resilient_with_meta_and_raw(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        return LLMTextResult(text="修正后内容\n", raw_text="RAW-1"), 0, None, None

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call_llm_text_resilient_with_meta_and_raw)

    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td)
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")

            runner._llm_worker(job_id, 0, work_dir, cfg)

            _assert_no_legacy_log_dirs(work_dir)
            _assert_resp_files(work_dir, expect=True)

            assert (work_dir / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW-1"
            assert (work_dir / "out" / "000000.txt").read_text(encoding="utf-8") == "修正后内容\n"

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.chunk_statuses[0].state == "done"
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_llm_worker_error_does_not_create_error_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call_llm_text_resilient_with_meta_and_raw(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        raise LLMError("boom", status_code=500)

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call_llm_text_resilient_with_meta_and_raw)

    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td)
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")

            runner._llm_worker(job_id, 0, work_dir, cfg)

            _assert_no_legacy_log_dirs(work_dir)
            _assert_resp_files(work_dir, expect=False)

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.chunk_statuses[0].state == "error"
            assert st.chunk_statuses[0].last_error_code == 500
            assert "boom" in (st.chunk_statuses[0].last_error_message or "")
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_retry_failed_chunks_overwrites_resp(monkeypatch: pytest.MonkeyPatch) -> None:
    lock = threading.Lock()
    call_count = 0

    def fake_call_llm_text_resilient_with_meta_and_raw(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        nonlocal call_count
        with lock:
            call_count += 1
            n = call_count
        if n == 1:
            # Force validation error but still write resp.
            return LLMTextResult(text="", raw_text="RAW-FAIL"), 0, None, None
        return LLMTextResult(text="修正后内容\n", raw_text="RAW-OK"), 0, None, None

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call_llm_text_resilient_with_meta_and_raw)

    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "final.txt"
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m", max_concurrency=1)

            runner._llm_worker(job_id, 0, work_dir, cfg)
            assert (work_dir / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW-FAIL"

            runner.retry_failed_chunks(job_id, cfg)

            _assert_no_legacy_log_dirs(work_dir)
            _assert_resp_files(work_dir, expect=True)
            assert (work_dir / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW-OK"
            assert out_path.read_text(encoding="utf-8") == "修正后内容\n"

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done"
            assert st.chunk_statuses[0].state == "done"
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_resume_paused_job_overwrites_existing_resp(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call_llm_text_resilient_with_meta_and_raw(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        return LLMTextResult(text="修正后内容\n", raw_text="RAW-RESUME"), 0, None, None

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call_llm_text_resilient_with_meta_and_raw)

    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "final.txt"
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("原始内容\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=1)
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            assert GLOBAL_JOBS.pause(job_id) is True
            assert GLOBAL_JOBS.resume(job_id) is True

            # Simulate an existing resp file from an earlier run.
            (work_dir / "resp").mkdir(parents=True, exist_ok=True)
            (work_dir / "resp" / "000000.txt").write_text("RAW-OLD", encoding="utf-8")

            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m", max_concurrency=1)
            runner.resume_paused_job(job_id, cfg)

            _assert_no_legacy_log_dirs(work_dir)
            _assert_resp_files(work_dir, expect=True)
            assert (work_dir / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW-RESUME"
            assert out_path.read_text(encoding="utf-8") == "修正后内容\n"
        finally:
            GLOBAL_JOBS.delete(job_id)
