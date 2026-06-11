from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.job_records import JOB_RECORD_VERSION, job_record_from_payload
from novel_proofer.jobs import JobStore
from novel_proofer.states import ChunkState, JobPhase, JobState, WaitReason


def _chunk_item(
    *,
    index: int = 0,
    state: ChunkState | str = ChunkState.PENDING,
    started_at: float | None = None,
    finished_at: float | None = None,
    retries: int = 0,
    last_error_code: int | None = None,
    last_error_message: str | None = None,
    llm_model: str | None = None,
    input_chars: int | None = None,
    output_chars: int | None = None,
) -> dict[str, Any]:
    return {
        "index": index,
        "state": state,
        "started_at": started_at,
        "finished_at": finished_at,
        "retries": retries,
        "last_error_code": last_error_code,
        "last_error_message": last_error_message,
        "llm_model": llm_model,
        "input_chars": input_chars,
        "output_chars": output_chars,
    }


def _chunk_counts(chunks: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "pending": 0,
        "processing": 0,
        "retrying": 0,
        "done": 0,
        "error": 0,
    }
    for chunk in chunks:
        state = str(chunk["state"])
        counts[state] += 1
    return counts


def _raw_job_record(
    *,
    job_id: str = "a" * 32,
    state: JobState | str = JobState.PAUSED,
    phase: JobPhase | str = JobPhase.PROCESS,
    wait_reason: WaitReason | str | None = WaitReason.USER_PAUSED,
    chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    chunk_items = [] if chunks is None else chunks
    return {
        "version": JOB_RECORD_VERSION,
        "job_record": {
            "job_id": job_id,
            "workflow": {
                "state": state,
                "phase": phase,
                "wait_reason": wait_reason,
            },
            "timestamps": {
                "created_at": 1.0,
                "started_at": None,
                "finished_at": None,
            },
            "artifacts": {
                "input_filename": "in.txt",
                "output_filename": "out.txt",
                "output_path": None,
                "work_dir": None,
                "cleanup_debug_dir": True,
            },
            "format": json.loads(json.dumps(FormatConfig().__dict__)),
            "llm": {
                "last_error_code": None,
                "last_retry_count": 0,
                "last_llm_model": None,
            },
            "chunks": {
                "total": len(chunk_items),
                "done": sum(1 for chunk in chunk_items if str(chunk["state"]) == ChunkState.DONE.value),
                "counts": _chunk_counts(chunk_items),
                "items": chunk_items,
            },
            "diagnostics": {
                "stats": {},
                "error": None,
            },
        },
    }


def test_job_store_update_respects_started_at_and_pause_rules() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=0)
    job_id = st.job_id

    js.update(job_id, started_at=123.0)
    js.update(job_id, started_at=456.0)
    st = js.get(job_id)
    assert st is not None
    assert st.started_at == 123.0

    assert js.pause(job_id) is True
    paused = js.get(job_id)
    assert paused is not None
    assert paused.wait_reason == WaitReason.USER_PAUSED
    # update() should not move paused -> running/queued.
    js.update(job_id, state="running")
    st2 = js.get(job_id)
    assert st2 is not None
    assert st2.state == "paused"
    assert st2.wait_reason == WaitReason.USER_PAUSED

    with pytest.raises(ValueError, match="wait_reason must be one of"):
        js.update(job_id, wait_reason="bogus")

    # Marking terminal state should clear paused flag.
    js.update(job_id, state="done", phase="done", finished_at=time.time())
    assert js.is_paused(job_id) is False


def test_job_store_update_rejects_invalid_workflow_combinations() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=0)
    job_id = st.job_id

    with pytest.raises(ValueError, match="job\\.phase must be 'done'"):
        js.update(job_id, state="done", phase="process")

    with pytest.raises(ValueError, match="job\\.state must be 'done'"):
        js.update(job_id, state="running", phase="done")

    with pytest.raises(ValueError, match="wait_reason is required when state is paused"):
        js.update(job_id, state="paused")

    with pytest.raises(ValueError, match="wait_reason=ready_to_merge is invalid for phase=process"):
        js.update(job_id, state="paused", phase="process", wait_reason="ready_to_merge")


def test_job_store_update_chunk_tracks_done_chunks() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=2)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=2)

    st = js.get(job_id)
    assert st is not None
    assert st.done_chunks == 0

    js.update_chunk(job_id, 0, state="done")
    st = js.get(job_id)
    assert st is not None
    assert st.done_chunks == 1

    js.update_chunk(job_id, 0, state="pending")
    st = js.get(job_id)
    assert st is not None
    assert st.done_chunks == 0

    # Out of range should be ignored.
    js.update_chunk(job_id, 99, state="done")
    st = js.get(job_id)
    assert st is not None
    assert st.done_chunks == 0
    assert st.chunk_counts.get("pending", 0) == 2
    assert st.chunk_counts.get("done", 0) == 0


def test_job_store_summary_and_chunk_page() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=3)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=3)
    js.update_chunk(job_id, 0, state="done")
    js.update_chunk(job_id, 1, state="error")
    js.update_chunk(job_id, 2, state="processing")

    summary = js.get_summary(job_id)
    assert summary is not None
    assert summary.chunk_statuses == []
    assert summary.chunk_counts.get("done", 0) == 1
    assert summary.chunk_counts.get("error", 0) == 1
    assert summary.chunk_counts.get("processing", 0) == 1

    page = js.get_chunks_page(job_id, chunk_state="active", limit=10, offset=0)
    assert page is not None
    chunks, counts, has_more = page
    assert has_more is False
    assert len(chunks) == 1
    assert chunks[0].state == "processing"
    assert counts.get("processing", 0) == 1


def test_job_store_add_retry_updates_job_and_chunk() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=1)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=1)

    js.add_retry(job_id, 0, 2, 429, "rate limit")
    got = js.get(job_id)
    assert got is not None
    assert got.last_retry_count == 2
    assert got.last_error_code == 429
    assert got.chunk_statuses[0].retries == 2
    assert got.chunk_statuses[0].last_error_code == 429
    assert "rate limit" in (got.chunk_statuses[0].last_error_message or "")

    # Invalid index still updates job-level counters.
    js.add_retry(job_id, 99, 1, 500, "oops")
    got2 = js.get(job_id)
    assert got2 is not None
    assert got2.last_retry_count == 3
    assert got2.last_error_code == 500


def test_job_store_cancel_resets_processing_chunks() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=2)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=2)
    js.update(job_id, state="running", started_at=time.time())
    js.update_chunk(job_id, 0, state="processing", started_at=1.0)
    js.update_chunk(job_id, 1, state="retrying", started_at=2.0, last_error_message=None)

    assert js.cancel(job_id) is True

    got = js.get(job_id)
    assert got is not None
    assert got.state == "cancelled"
    assert got.finished_at is not None
    assert got.chunk_statuses[0].state == "pending"
    assert got.chunk_statuses[0].started_at is None
    assert got.chunk_statuses[1].state == "pending"
    assert got.chunk_statuses[1].last_error_message == "cancelled"


def test_job_store_pause_resume_and_delete() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=0)
    job_id = st.job_id

    assert js.resume(job_id) is False
    assert js.pause(job_id) is True
    assert js.pause(job_id) is False
    assert js.is_paused(job_id) is True

    assert js.resume(job_id) is True
    assert js.is_paused(job_id) is False
    resumed = js.get(job_id)
    assert resumed is not None
    assert resumed.wait_reason is None

    assert js.delete(job_id) is True
    assert js.delete(job_id) is False


def test_job_store_ignores_unknown_jobs_and_cancelled_updates() -> None:
    js = JobStore()

    js.update("missing", state="running")
    js.init_chunks("missing", total_chunks=1)
    js.update_chunk("missing", 0, state="done")
    js.add_retry("missing", 0, 1, None, None)
    js.add_stat("missing", "x", 1)
    assert js.cancel("missing") is False
    assert js.pause("missing") is False
    assert js.resume("missing") is False

    st = js.create("in.txt", "out.txt", total_chunks=1)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=1)
    assert js.cancel(job_id) is True

    # update() should no-op for cancelled jobs.
    js.update(job_id, state="running")
    got = js.get(job_id)
    assert got is not None
    assert got.state == "cancelled"

    # update_chunk() should no-op for cancelled jobs.
    js.update_chunk(job_id, 0, state="done")
    got2 = js.get(job_id)
    assert got2 is not None
    assert got2.chunk_statuses[0].state == "pending"


def test_job_store_persistence_is_throttled_and_flushable(tmp_path: Path) -> None:
    js = JobStore(persist_interval_s=60.0)
    js.configure_persistence(persist_dir=tmp_path)
    try:
        st = js.create("in.txt", "out.txt", total_chunks=1)
        job_id = st.job_id
        js.init_chunks(job_id, total_chunks=1)

        calls = 0
        orig = js._atomic_write_json

        def wrapped(path: Path, payload: dict) -> None:
            nonlocal calls
            calls += 1
            orig(path, payload)

        js._atomic_write_json = wrapped  # type: ignore[method-assign]

        js.update_chunk(job_id, 0, state="processing")
        js.update_chunk(job_id, 0, state="done")
        assert calls == 0

        js.flush_persistence(job_id)
        assert calls == 1
    finally:
        js.shutdown_persistence(wait=True)


def test_job_record_rejects_missing_phase() -> None:
    raw = _raw_job_record()
    del raw["job_record"]["workflow"]["phase"]

    with pytest.raises(ValueError, match="missing field 'phase'"):
        job_record_from_payload(raw)


def test_job_record_rejects_unknown_root_fields() -> None:
    raw = _raw_job_record()
    raw["job"] = {}

    with pytest.raises(ValueError, match="job record file contains unknown fields: job"):
        job_record_from_payload(raw)


def test_job_record_rejects_paused_without_wait_reason() -> None:
    raw = _raw_job_record(wait_reason=None)

    with pytest.raises(ValueError, match="paused workflow state requires wait_reason"):
        job_record_from_payload(raw)


def test_job_record_rejects_non_paused_with_wait_reason() -> None:
    raw = _raw_job_record(state=JobState.RUNNING, phase=JobPhase.VALIDATE, wait_reason=WaitReason.USER_PAUSED)

    with pytest.raises(ValueError, match="workflow wait_reason must be None"):
        job_record_from_payload(raw)


def test_job_store_persists_job_record_without_volatile_execution(tmp_path: Path) -> None:
    js = JobStore(persist_interval_s=60.0)
    js.configure_persistence(persist_dir=tmp_path)
    try:
        st = js.create("in.txt", "out.txt", total_chunks=1)
        job_id = st.job_id
        js.init_chunks(job_id, total_chunks=1)
        js.update(job_id, state=JobState.RUNNING, phase=JobPhase.PROCESS, started_at=2.0)
        js.update_chunk(job_id, 0, state=ChunkState.PROCESSING, started_at=3.0, input_chars=10)

        runtime = js.get(job_id)
        assert runtime is not None
        assert runtime.state == JobState.RUNNING
        assert runtime.chunk_statuses[0].state == ChunkState.PROCESSING

        js.flush_persistence(job_id)
        payload = json.loads((tmp_path / f"{job_id}.json").read_text(encoding="utf-8"))

        assert payload["version"] == JOB_RECORD_VERSION
        assert "job" not in payload
        record = payload["job_record"]
        assert record["workflow"] == {
            "state": JobState.PAUSED,
            "phase": JobPhase.PROCESS,
            "wait_reason": WaitReason.SERVER_RECOVERED,
        }
        assert record["chunks"]["counts"]["pending"] == 1
        assert record["chunks"]["counts"]["processing"] == 0
        assert record["chunks"]["items"][0]["state"] == ChunkState.PENDING
        assert record["chunks"]["items"][0]["started_at"] is None
        assert record["chunks"]["items"][0]["input_chars"] == 10
        job_record_from_payload(payload)
    finally:
        js.shutdown_persistence(wait=True)


def test_job_store_load_persisted_jobs_loads_clean_record(tmp_path: Path) -> None:
    job_id = "b" * 32
    snap = tmp_path / f"{job_id}.json"
    snap.write_text(
        json.dumps(
            _raw_job_record(
                job_id=job_id,
                state=JobState.PAUSED,
                phase=JobPhase.PROCESS,
                wait_reason=WaitReason.READY_TO_PROCESS,
                chunks=[_chunk_item(index=0, state=ChunkState.PENDING)],
            )
        ),
        encoding="utf-8",
    )

    js = JobStore()
    js.configure_persistence(persist_dir=tmp_path)
    try:
        assert js.load_persisted_jobs() == 1
        loaded = js.get(job_id)
        assert loaded is not None
        assert loaded.state == JobState.PAUSED
        assert loaded.phase == JobPhase.PROCESS
        assert loaded.wait_reason == WaitReason.READY_TO_PROCESS
        assert loaded.chunk_counts == _chunk_counts([_chunk_item(index=0, state=ChunkState.PENDING)])
    finally:
        js.shutdown_persistence(wait=True)


def test_job_store_load_persisted_jobs_preserves_server_recovered_record(tmp_path: Path) -> None:
    job_id = "c" * 32
    snap = tmp_path / f"{job_id}.json"
    snap.write_text(
        json.dumps(
            _raw_job_record(
                job_id=job_id,
                state=JobState.PAUSED,
                phase=JobPhase.VALIDATE,
                wait_reason=WaitReason.SERVER_RECOVERED,
            )
        ),
        encoding="utf-8",
    )

    js = JobStore()
    js.configure_persistence(persist_dir=tmp_path)
    try:
        assert js.load_persisted_jobs() == 1
        loaded = js.get(job_id)
        assert loaded is not None
        assert loaded.state == JobState.PAUSED
        assert loaded.phase == JobPhase.VALIDATE
        assert loaded.wait_reason == WaitReason.SERVER_RECOVERED
    finally:
        js.shutdown_persistence(wait=True)


def test_job_store_load_persisted_jobs_rejects_corrupt_record(tmp_path: Path) -> None:
    bad = tmp_path / f"{'d' * 32}.json"
    bad.write_text(
        json.dumps(
            _raw_job_record(
                job_id="d" * 32,
                state=JobState.PAUSED,
                phase=JobPhase.MERGE,
                wait_reason=WaitReason.READY_TO_MERGE,
                chunks=[_chunk_item(index=0, state=ChunkState.PENDING)],
            )
        ),
        encoding="utf-8",
    )

    js = JobStore()
    js.configure_persistence(persist_dir=tmp_path)
    try:
        with pytest.raises(ValueError, match="delete the corrupted state file and retry"):
            js.load_persisted_jobs()
        assert js.list_summaries() == []
    finally:
        js.shutdown_persistence(wait=True)


def test_job_store_load_persisted_jobs_restores_in_flight_record_as_paused(tmp_path: Path) -> None:
    job_id = "e" * 32
    snap = tmp_path / f"{job_id}.json"
    snap.write_text(
        json.dumps(
            _raw_job_record(
                job_id=job_id,
                state=JobState.RUNNING,
                phase=JobPhase.PROCESS,
                wait_reason=None,
                chunks=[_chunk_item(index=0, state=ChunkState.PROCESSING, started_at=3.0, input_chars=10)],
            )
        ),
        encoding="utf-8",
    )

    js = JobStore()
    js.configure_persistence(persist_dir=tmp_path)
    try:
        assert js.load_persisted_jobs() == 1
        loaded = js.get(job_id)
        assert loaded is not None
        assert loaded.state == JobState.PAUSED
        assert loaded.phase == JobPhase.PROCESS
        assert loaded.wait_reason == WaitReason.SERVER_RECOVERED
        assert loaded.chunk_statuses[0].state == ChunkState.PENDING
        assert loaded.chunk_statuses[0].started_at is None
    finally:
        js.shutdown_persistence(wait=True)
