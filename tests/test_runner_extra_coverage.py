from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import novel_proofer.runner as runner
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMError, LLMTextResult
from novel_proofer.llm.config import LLMConfig


def _mk_job(work_dir: Path, out_path: Path, *, total_chunks: int = 1, cleanup_debug_dir: bool = False) -> str:
    job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=total_chunks)
    job_id = job.job_id
    GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=cleanup_debug_dir)
    GLOBAL_JOBS.init_chunks(job_id, total_chunks=total_chunks)
    return job_id


def test_llm_worker_records_retries_and_aligns_newlines(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call(  # noqa: ANN001
        cfg: LLMConfig,
        input_text: str,
        *,
        should_stop=None,
        on_retry=None,
    ):
        if on_retry is not None:
            on_retry(1, 429, "rate limit")
        return LLMTextResult(text="OK\n", raw_text="RAW"), 2, 429, "rate limit"

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call)

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        work_dir = base / "work"
        out_path = base / "out.txt"
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("IN\n\n", encoding="utf-8")

        job_id = _mk_job(work_dir, out_path, total_chunks=1, cleanup_debug_dir=False)
        try:
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")
            runner._llm_worker(job_id, 0, work_dir, cfg)

            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            cs = st.chunk_statuses[0]
            assert cs.state == "done"
            assert cs.retries == 2
            assert cs.last_error_code == 429
            assert cs.output_chars == len("OK\n\n")

            assert (work_dir / "resp" / "000000.txt").read_text(encoding="utf-8") == "RAW"
            assert (work_dir / "out" / "000000.txt").read_text(encoding="utf-8") == "OK\n\n"
            assert not (work_dir / "req").exists()
            assert not (work_dir / "error").exists()
        finally:
            GLOBAL_JOBS.delete(job_id)


def test_llm_worker_cancel_behaviors(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        work_dir = base / "work"
        out_path = base / "out.txt"
        (work_dir / "pre").mkdir(parents=True, exist_ok=True)
        (work_dir / "pre" / "000000.txt").write_text("IN\n", encoding="utf-8")

        # Cancel before worker starts -> early return.
        job_id = _mk_job(work_dir, out_path, total_chunks=1, cleanup_debug_dir=False)
        try:
            assert GLOBAL_JOBS.cancel(job_id) is True
            runner._llm_worker(job_id, 0, work_dir, LLMConfig(enabled=True, base_url="", model=""))
            assert not (work_dir / "resp").exists()
        finally:
            GLOBAL_JOBS.delete(job_id)

        # Cancel after LLM returns -> return before writing resp/out.
        job_id2 = _mk_job(work_dir, out_path, total_chunks=1, cleanup_debug_dir=False)
        try:
            def fake_cancel_then_return(  # noqa: ANN001
                cfg: LLMConfig,
                input_text: str,
                *,
                should_stop=None,
                on_retry=None,
            ):
                GLOBAL_JOBS.cancel(job_id2)
                return LLMTextResult(text="OK\n", raw_text="RAW"), 0, None, None

            monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_cancel_then_return)
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")
            runner._llm_worker(job_id2, 0, work_dir, cfg)
            assert not (work_dir / "resp").exists()
            assert not (work_dir / "out").exists()
        finally:
            GLOBAL_JOBS.delete(job_id2)

        # Cancelled inside LLMError handler -> early return (no chunk error state set).
        job_id3 = _mk_job(work_dir, out_path, total_chunks=1, cleanup_debug_dir=False)
        try:
            def fake_cancel_then_raise(  # noqa: ANN001
                cfg: LLMConfig,
                input_text: str,
                *,
                should_stop=None,
                on_retry=None,
            ):
                GLOBAL_JOBS.cancel(job_id3)
                raise LLMError("x", status_code=500)

            monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_cancel_then_raise)
            cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")
            runner._llm_worker(job_id3, 0, work_dir, cfg)
            st = GLOBAL_JOBS.get(job_id3)
            assert st is not None
            assert st.chunk_statuses[0].state == "pending"
        finally:
            GLOBAL_JOBS.delete(job_id3)


def test_llm_worker_ratio_validation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    pre = "a" * 200
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m")

        def run_case(sub: str, out_text: str, expect: str) -> None:
            work_dir = base / sub
            out_path = base / f"{sub}.txt"
            (work_dir / "pre").mkdir(parents=True, exist_ok=True)
            (work_dir / "pre" / "000000.txt").write_text(pre, encoding="utf-8")
            job_id = _mk_job(work_dir, out_path, total_chunks=1, cleanup_debug_dir=False)
            try:
                monkeypatch.setattr(
                    runner,
                    "call_llm_text_resilient_with_meta_and_raw",
                    lambda *a, **k: (LLMTextResult(text=out_text, raw_text="RAW"), 0, None, None),
                )
                runner._llm_worker(job_id, 0, work_dir, cfg)
                st = GLOBAL_JOBS.get(job_id)
                assert st is not None
                assert st.chunk_statuses[0].state == "error"
                assert expect in (st.chunk_statuses[0].last_error_message or "")
            finally:
                GLOBAL_JOBS.delete(job_id)

        run_case("short", "b" * 10, "too short")
        run_case("long", "b" * 400, "too long")


def test_run_llm_for_indices_paused_cancelled_and_worker_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = LLMConfig(enabled=True, base_url="http://example.com", model="m", max_concurrency=1)
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        work_dir = base / "work"

        # Paused: no work launched.
        job_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=2).job_id
        try:
            GLOBAL_JOBS.init_chunks(job_id, total_chunks=2)
            assert GLOBAL_JOBS.pause(job_id) is True
            assert runner._run_llm_for_indices(job_id, [0, 1], work_dir, cfg) == "paused"
        finally:
            GLOBAL_JOBS.delete(job_id)

        # Cancelled: no work launched.
        job2_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=2).job_id
        try:
            GLOBAL_JOBS.init_chunks(job2_id, total_chunks=2)
            assert GLOBAL_JOBS.cancel(job2_id) is True
            assert runner._run_llm_for_indices(job2_id, [0, 1], work_dir, cfg) == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job2_id)

        # Worker exception is ignored at f.result().
        monkeypatch.setattr(runner, "_llm_worker", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        job3_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1).job_id
        try:
            GLOBAL_JOBS.init_chunks(job3_id, total_chunks=1)
            assert runner._run_llm_for_indices(job3_id, [0], work_dir, cfg) == "done"
        finally:
            GLOBAL_JOBS.delete(job3_id)


def test_run_job_cancellation_llm_outcomes_and_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    fmt = runner.FormatConfig(max_chunk_chars=2000)
    llm_off = LLMConfig(enabled=False)
    llm_on = LLMConfig(enabled=True, base_url="http://example.com", model="m")

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)

        # Cancelled during preprocessing loop (second chunk).
        job_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w1"
            out_path = base / "o1.txt"
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            calls = 0

            def cancel_after_first(chunk: str, _cfg):  # noqa: ANN001
                nonlocal calls
                calls += 1
                if calls == 1:
                    GLOBAL_JOBS.cancel(job_id)
                return chunk, {}

            monkeypatch.setattr(runner, "apply_rules", cancel_after_first)
            runner.run_job(job_id, ("x\n" * 3000), fmt, llm_off)
            st = GLOBAL_JOBS.get(job_id)
            assert st is not None and st.state == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job_id)

        # Cancelled after preprocessing loop.
        job2_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w2"
            out_path = base / "o2.txt"
            GLOBAL_JOBS.update(job2_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            def cancel_during_apply(chunk: str, _cfg):  # noqa: ANN001
                GLOBAL_JOBS.cancel(job2_id)
                return chunk, {}

            monkeypatch.setattr(runner, "apply_rules", cancel_during_apply)
            runner.run_job(job2_id, "x\n", fmt, llm_off)
            st = GLOBAL_JOBS.get(job2_id)
            assert st is not None and st.state == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job2_id)

        # LLM enabled: outcome paused/cancelled.
        job3_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w3"
            out_path = base / "o3.txt"
            GLOBAL_JOBS.update(job3_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)
            monkeypatch.setattr(runner, "_run_llm_for_indices", lambda *a, **k: "paused")
            runner.run_job(job3_id, "x\n", fmt, llm_on)
            st = GLOBAL_JOBS.get(job3_id)
            assert st is not None and st.state == "paused"
        finally:
            GLOBAL_JOBS.delete(job3_id)

        job4_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w4"
            out_path = base / "o4.txt"
            GLOBAL_JOBS.update(job4_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)
            monkeypatch.setattr(runner, "_run_llm_for_indices", lambda *a, **k: "cancelled")
            runner.run_job(job4_id, "x\n", fmt, llm_on)
            st = GLOBAL_JOBS.get(job4_id)
            assert st is not None and st.state == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job4_id)

        # Cancelled during local output loop.
        job5_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w5"
            out_path = base / "o5.txt"
            GLOBAL_JOBS.update(job5_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)

            orig_atomic = runner._atomic_write_text
            cancelled = False

            def atomic_and_cancel(path: Path, content: str) -> None:
                nonlocal cancelled
                orig_atomic(path, content)
                if not cancelled and path.parent.name == "out" and path.name == "000000.txt":
                    cancelled = True
                    GLOBAL_JOBS.cancel(job5_id)

            monkeypatch.setattr(runner, "_atomic_write_text", atomic_and_cancel)
            runner.run_job(job5_id, ("x\n" * 3000), fmt, llm_off)
            st = GLOBAL_JOBS.get(job5_id)
            assert st is not None and st.state == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job5_id)

        # Exception handler sets job error.
        job6_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            work_dir = base / "w6"
            out_path = base / "o6.txt"
            GLOBAL_JOBS.update(job6_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=False)
            monkeypatch.setattr(runner, "chunk_by_lines", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
            runner.run_job(job6_id, "x\n", fmt, llm_off)
            st = GLOBAL_JOBS.get(job6_id)
            assert st is not None and st.state == "error"
            assert "boom" in (st.error or "")
        finally:
            GLOBAL_JOBS.delete(job6_id)


def test_retry_failed_and_resume_paused_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    llm = LLMConfig(enabled=True, base_url="http://example.com", model="m", max_concurrency=1)

    runner.retry_failed_chunks("missing", llm)
    runner.resume_paused_job("missing", llm)

    # Missing work_dir/output_path.
    job_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
    try:
        runner.retry_failed_chunks(job_id, llm)
        runner.resume_paused_job(job_id, llm)
        st = GLOBAL_JOBS.get(job_id)
        assert st is not None and st.state == "error"
    finally:
        GLOBAL_JOBS.delete(job_id)

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)

        # No chunk statuses.
        job2_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0).job_id
        try:
            GLOBAL_JOBS.update(job2_id, work_dir=str(base / "w1"), output_path=str(base / "o1.txt"), cleanup_debug_dir=False)
            runner.retry_failed_chunks(job2_id, llm)
            runner.resume_paused_job(job2_id, llm)
            st = GLOBAL_JOBS.get(job2_id)
            assert st is not None and st.state == "error"
        finally:
            GLOBAL_JOBS.delete(job2_id)

        # No failed chunks and output exists -> done.
        job3_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1).job_id
        try:
            out_path = base / "o2.txt"
            out_path.write_text("x", encoding="utf-8")
            GLOBAL_JOBS.update(job3_id, work_dir=str(base / "w2"), output_path=str(out_path), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job3_id, total_chunks=1)
            GLOBAL_JOBS.update_chunk(job3_id, 0, state="done")
            runner.retry_failed_chunks(job3_id, llm)
            st = GLOBAL_JOBS.get(job3_id)
            assert st is not None and st.state == "done"
        finally:
            GLOBAL_JOBS.delete(job3_id)

        # Outcome paused/cancelled branches.
        job4_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1).job_id
        try:
            GLOBAL_JOBS.update(job4_id, work_dir=str(base / "w3"), output_path=str(base / "o3.txt"), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job4_id, total_chunks=1)
            GLOBAL_JOBS.update_chunk(job4_id, 0, state="error", last_error_message="x")
            monkeypatch.setattr(runner, "_run_llm_for_indices", lambda *a, **k: "paused")
            runner.retry_failed_chunks(job4_id, llm)
            st = GLOBAL_JOBS.get(job4_id)
            assert st is not None and st.state == "paused"
        finally:
            GLOBAL_JOBS.delete(job4_id)

        job5_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1).job_id
        try:
            GLOBAL_JOBS.update(job5_id, work_dir=str(base / "w4"), output_path=str(base / "o4.txt"), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job5_id, total_chunks=1)
            GLOBAL_JOBS.update_chunk(job5_id, 0, state="error", last_error_message="x")
            monkeypatch.setattr(runner, "_run_llm_for_indices", lambda *a, **k: "cancelled")
            runner.retry_failed_chunks(job5_id, llm)
            st = GLOBAL_JOBS.get(job5_id)
            assert st is not None and st.state == "cancelled"
        finally:
            GLOBAL_JOBS.delete(job5_id)

        # Resume: no pending chunks finalizes and returns.
        job6_id = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=1).job_id
        try:
            work_dir = base / "w5"
            (work_dir / "out").mkdir(parents=True, exist_ok=True)
            (work_dir / "out" / "000000.txt").write_text("x", encoding="utf-8")
            GLOBAL_JOBS.update(job6_id, work_dir=str(work_dir), output_path=str(base / "o5.txt"), cleanup_debug_dir=False)
            GLOBAL_JOBS.init_chunks(job6_id, total_chunks=1)
            GLOBAL_JOBS.update_chunk(job6_id, 0, state="done")
            runner.resume_paused_job(job6_id, llm)
            st = GLOBAL_JOBS.get(job6_id)
            assert st is not None and st.state == "done"
        finally:
            GLOBAL_JOBS.delete(job6_id)
