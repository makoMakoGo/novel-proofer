from __future__ import annotations

import time
from pathlib import Path

from novel_proofer.jobs import JobStore


def test_job_store_update_respects_started_at_and_pause_rules() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=0)
    job_id = st.job_id

    js.update(job_id, started_at=123.0)
    js.update(job_id, started_at=456.0)
    assert js.get(job_id) is not None
    assert js.get(job_id).started_at == 123.0  # type: ignore[union-attr]

    assert js.pause(job_id) is True
    # update() should not move paused -> running/queued.
    js.update(job_id, state="running")
    assert js.get(job_id).state == "paused"  # type: ignore[union-attr]

    # Marking terminal state should clear paused flag.
    js.update(job_id, state="done", finished_at=time.time())
    assert js.is_paused(job_id) is False


def test_job_store_update_chunk_tracks_done_chunks() -> None:
    js = JobStore()
    st = js.create("in.txt", "out.txt", total_chunks=2)
    job_id = st.job_id
    js.init_chunks(job_id, total_chunks=2)

    assert js.get(job_id).done_chunks == 0  # type: ignore[union-attr]
    js.update_chunk(job_id, 0, state="done")
    assert js.get(job_id).done_chunks == 1  # type: ignore[union-attr]
    js.update_chunk(job_id, 0, state="pending")
    assert js.get(job_id).done_chunks == 0  # type: ignore[union-attr]

    # Out of range should be ignored.
    js.update_chunk(job_id, 99, state="done")
    assert js.get(job_id).done_chunks == 0  # type: ignore[union-attr]


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
