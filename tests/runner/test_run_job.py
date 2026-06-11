from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import novel_proofer.paths as paths
import novel_proofer.runner as runner
from novel_proofer.executions import GLOBAL_EXECUTIONS
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS, JobStore
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


def test_run_job_pause_during_validation_stays_in_validate_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"
        input_path = Path(td) / "in.txt"
        input_path.write_text("第1章\r\n\r\n你好。\r\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        execution = GLOBAL_EXECUTIONS.begin(job_id, "validate")

        def pause_before_first_chunk(*args: object, **kwargs: object):
            assert GLOBAL_EXECUTIONS.request_stop(job_id, "pause") is True
            yield "第1章\n\n你好。"

        monkeypatch.setattr(runner, "iter_chunks_by_lines_with_first_chunk_max_from_file", pause_before_first_chunk)
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path))

            runner.run_job(
                job_id,
                input_path,
                FormatConfig(max_chunk_chars=2000),
                LLMConfig(base_url="http://example.com", model="m", max_concurrency=1),
            )

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "paused"
            assert st.phase == "validate"
            assert st.wait_reason == "user_paused"
            assert st.error is None
        finally:
            GLOBAL_EXECUTIONS.finish(execution.attempt_id)
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

            st1 = GLOBAL_JOBS.get(job_id)
            assert st1 is not None
            assert st1.state == "paused"
            assert getattr(st1, "phase", None) == "process"

            runner.resume_paused_job(job_id, LLMConfig(base_url="http://example.com", model="m", max_concurrency=1))
            st2 = GLOBAL_JOBS.get(job_id)
            assert st2 is not None
            assert st2.state == "paused"
            assert getattr(st2, "phase", None) == "merge"

            runner.merge_outputs(job_id)
            st3 = GLOBAL_JOBS.get(job_id)
            assert st3 is not None
            assert st3.state == "done"
            assert getattr(st3, "phase", None) == "done"
            assert out_path.exists()
            # cleanup_debug_dir defaults to True (cleanup happens at merge).
            assert not work_dir.exists()
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_run_job_pause_after_chunk_initialization_stays_in_process_phase(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"
        input_path = Path(td) / "in.txt"
        input_path.write_text("第1章\r\n\r\n你好。\r\n", encoding="utf-8")

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        execution = GLOBAL_EXECUTIONS.begin(job_id, "validate")
        original_init_chunks = GLOBAL_JOBS.init_chunks

        def pause_after_init_chunks(target_job_id: str, *args: object, **kwargs: object) -> None:
            original_init_chunks(target_job_id, *args, **kwargs)
            assert GLOBAL_EXECUTIONS.request_stop(target_job_id, "pause") is True

        monkeypatch.setattr(GLOBAL_JOBS, "init_chunks", pause_after_init_chunks)
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path))

            runner.run_job(
                job_id,
                input_path,
                FormatConfig(max_chunk_chars=2000),
                LLMConfig(base_url="http://example.com", model="m", max_concurrency=1),
            )

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "paused"
            assert st.phase == "process"
            assert st.wait_reason == "user_paused"
        finally:
            GLOBAL_EXECUTIONS.finish(execution.attempt_id)
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

            st1 = GLOBAL_JOBS.get(job_id)
            assert st1 is not None
            assert st1.state == "paused"
            assert getattr(st1, "phase", None) == "process"

            runner.resume_paused_job(job_id, LLMConfig(base_url="http://example.com", model="m", max_concurrency=1))
            st2 = GLOBAL_JOBS.get(job_id)
            assert st2 is not None
            assert st2.state == "paused"
            assert getattr(st2, "phase", None) == "merge"

            runner.merge_outputs(job_id)
            st3 = GLOBAL_JOBS.get(job_id)
            assert st3 is not None
            assert st3.state == "done"
            assert getattr(st3, "phase", None) == "done"
            assert out_path.exists()
            assert work_dir.exists()
            assert (work_dir / "README.txt").exists()
            assert (work_dir / "pre").exists()
            assert (work_dir / "out").exists()
            assert (work_dir / "resp").exists()
            assert not (work_dir / "req").exists()
            assert not (work_dir / "error").exists()
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_resume_after_persisted_recovery_rebuilds_pre_texts_from_input_cache(monkeypatch: pytest.MonkeyPatch) -> None:
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
        state_dir = out_dir / ".state" / "jobs"
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)
        state_dir.mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(paths, "OUTPUT_DIR", out_dir)
        monkeypatch.setattr(paths, "JOBS_DIR", jobs_dir)

        input_path = base / "in.txt"
        input_text = "第1章\n\n你好。\n"
        input_path.write_text(input_text, encoding="utf-8")

        original_store = runner.GLOBAL_JOBS
        recovered_store = JobStore()
        recovered_store.configure_persistence(persist_dir=state_dir)
        monkeypatch.setattr(runner, "GLOBAL_JOBS", recovered_store)
        try:
            job = recovered_store.create("in.txt", "out.txt", total_chunks=0)
            job_id = job.job_id
            work_dir = jobs_dir / job_id
            out_path = out_dir / f"{job_id}_out.txt"
            recovered_store.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)
            paths._write_input_cache(job_id, input_text)

            runner.run_job(
                job_id,
                input_path,
                FormatConfig(max_chunk_chars=2000),
                LLMConfig(base_url="http://example.com", model="m", max_concurrency=1),
            )

            persisted = recovered_store.get(job_id)
            assert persisted is not None
            assert persisted.state == "paused"
            assert persisted.phase == "process"

            recovered_store.flush_persistence(job_id)
            recovered_store.shutdown_persistence(wait=True)

            reloaded_store = JobStore()
            reloaded_store.configure_persistence(persist_dir=state_dir)
            assert reloaded_store.load_persisted_jobs() == 1
            monkeypatch.setattr(runner, "GLOBAL_JOBS", reloaded_store)
            try:
                reloaded = reloaded_store.get(job_id)
                assert reloaded is not None
                assert reloaded.wait_reason == "ready_to_process"
                assert reloaded_store.get_chunk_pre_text(job_id, 0) is None
                assert not (work_dir / "pre" / "000000.txt").exists()

                runner.resume_paused_job(job_id, LLMConfig(base_url="http://example.com", model="m", max_concurrency=1))

                resumed = reloaded_store.get(job_id)
                assert resumed is not None
                assert resumed.state == "paused"
                assert resumed.phase == "merge"
                assert resumed.wait_reason == "ready_to_merge"
                assert (work_dir / "out" / "000000.txt").read_text(encoding="utf-8").strip() != ""
            finally:
                reloaded_store.shutdown_persistence(wait=True)
        finally:
            monkeypatch.setattr(runner, "GLOBAL_JOBS", original_store)
