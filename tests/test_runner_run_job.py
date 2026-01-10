from __future__ import annotations

import tempfile
from pathlib import Path

import novel_proofer.runner as runner
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.config import LLMConfig


def test_run_job_missing_paths_sets_error() -> None:
    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
    job_id = job.job_id
    try:
        runner.run_job(job_id, "x", FormatConfig(max_chunk_chars=2000), LLMConfig(enabled=False))
        st = GLOBAL_JOBS.get(job_id)
        assert st is not None
        assert st.state == "error"
        assert "work_dir/output_path" in (st.error or "")
    finally:
        GLOBAL_JOBS.delete(job_id)


def test_run_job_local_mode_cleans_up_by_default() -> None:
    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path))

            runner.run_job(
                job_id,
                "第1章\r\n\r\n你好...\r\n",
                FormatConfig(max_chunk_chars=2000, normalize_ellipsis=True),
                LLMConfig(enabled=False),
            )

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done"
            assert out_path.exists()
            # cleanup_debug_dir defaults to True
            assert not work_dir.exists()
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_run_job_local_mode_keeps_debug_dir_when_opted_out() -> None:
    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            runner.run_job(
                job_id,
                "第1章\r\n\r\n你好...\r\n",
                FormatConfig(max_chunk_chars=2000, normalize_ellipsis=True),
                LLMConfig(enabled=False),
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

