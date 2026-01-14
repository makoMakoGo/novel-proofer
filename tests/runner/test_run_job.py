from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import novel_proofer.runner as runner
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMTextResult
from novel_proofer.llm.config import LLMConfig


def test_run_job_missing_paths_sets_error() -> None:
    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job_id = job.job_id
    try:
        runner.run_job(job_id, Path("x"), FormatConfig(max_chunk_chars=2000), LLMConfig())
        st = GLOBAL_JOBS.get(job_id)
        assert st is not None
        assert st.state == "error"
        assert "work_dir/output_path" in (st.error or "")
    finally:
        GLOBAL_JOBS.delete(job_id)


def test_run_job_local_mode_cleans_up_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
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
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"
        input_path = Path(td) / "in.txt"
        input_path.write_text("第1章\r\n\r\n你好...\r\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path))

            runner.run_job(
                job_id,
                input_path,
                FormatConfig(max_chunk_chars=2000, normalize_ellipsis=True),
                LLMConfig(base_url="http://example.com", model="m", max_concurrency=1),
            )

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done"
            assert out_path.exists()
            # cleanup_debug_dir defaults to True
            assert not work_dir.exists()
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_run_job_local_mode_keeps_debug_dir_when_opted_out(monkeypatch: pytest.MonkeyPatch) -> None:
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
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"
        input_path = Path(td) / "in.txt"
        input_path.write_text("第1章\r\n\r\n你好...\r\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            runner.run_job(
                job_id,
                input_path,
                FormatConfig(max_chunk_chars=2000, normalize_ellipsis=True),
                LLMConfig(base_url="http://example.com", model="m", max_concurrency=1),
            )

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done"
            assert out_path.exists()
            assert work_dir.exists()
            assert (work_dir / "README.txt").exists()
            assert (work_dir / "pre").exists()
            assert (work_dir / "out").exists()
            assert not (work_dir / "req").exists()
            assert not (work_dir / "error").exists()
        finally:
            GLOBAL_JOBS.delete(job_id)
