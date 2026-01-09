from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class ChunkStatus:
    index: int
    # UI contract: pending|processing|retrying|done|error
    state: str
    started_at: float | None = None
    finished_at: float | None = None
    retries: int = 0
    last_error_code: int | None = None
    last_error_message: str | None = None

    # Diagnostics (optional)
    input_chars: int | None = None
    output_chars: int | None = None


@dataclass
class JobStatus:
    job_id: str
    # queued|running|paused|done|error|cancelled
    state: str
    created_at: float
    started_at: float | None
    finished_at: float | None

    input_filename: str
    output_filename: str
    total_chunks: int
    done_chunks: int

    # Diagnostics
    last_error_code: int | None = None
    last_retry_count: int = 0

    stats: dict[str, int] = field(default_factory=dict)
    chunk_statuses: list[ChunkStatus] = field(default_factory=list)

    error: str | None = None
    output_path: str | None = None
    work_dir: str | None = None
    cleanup_debug_dir: bool = True


class JobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()

    def _snapshot_chunk(self, cs: ChunkStatus) -> ChunkStatus:
        return ChunkStatus(
            index=cs.index,
            state=cs.state,
            started_at=cs.started_at,
            finished_at=cs.finished_at,
            retries=cs.retries,
            last_error_code=cs.last_error_code,
            last_error_message=cs.last_error_message,
            input_chars=cs.input_chars,
            output_chars=cs.output_chars,
        )

    def _snapshot_job(self, st: JobStatus) -> JobStatus:
        return JobStatus(
            job_id=st.job_id,
            state=st.state,
            created_at=st.created_at,
            started_at=st.started_at,
            finished_at=st.finished_at,
            input_filename=st.input_filename,
            output_filename=st.output_filename,
            output_path=st.output_path,
            total_chunks=st.total_chunks,
            done_chunks=st.done_chunks,
            last_error_code=st.last_error_code,
            last_retry_count=st.last_retry_count,
            stats=dict(st.stats),
            chunk_statuses=[self._snapshot_chunk(c) for c in st.chunk_statuses],
            error=st.error,
            work_dir=st.work_dir,
            cleanup_debug_dir=st.cleanup_debug_dir,
        )

    def create(self, input_filename: str, output_filename: str, total_chunks: int) -> JobStatus:
        job_id = uuid.uuid4().hex
        status = JobStatus(
            job_id=job_id,
            state="queued",
            created_at=time.time(),
            started_at=None,
            finished_at=None,
            input_filename=input_filename,
            output_filename=output_filename,
            total_chunks=total_chunks,
            done_chunks=0,
        )
        with self._lock:
            self._jobs[job_id] = status
            return self._snapshot_job(status)

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            return self._snapshot_job(st)

    def update(self, job_id: str, **kwargs) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if st.state == "cancelled":
                return
            for k, v in kwargs.items():
                if k == "started_at" and st.started_at is not None and v is not None:
                    continue
                if k == "state" and st.state == "paused" and v in {"queued", "running"}:
                    continue
                if k == "state" and v in {"done", "error", "cancelled"}:
                    self._paused.discard(job_id)
                setattr(st, k, v)

    def init_chunks(self, job_id: str, total_chunks: int) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.total_chunks = total_chunks
            st.done_chunks = 0
            st.chunk_statuses = [ChunkStatus(index=i, state="pending") for i in range(total_chunks)]

    def update_chunk(self, job_id: str, index: int, **kwargs) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if st.state == "cancelled" or job_id in self._cancelled:
                return
            if index < 0 or index >= len(st.chunk_statuses):
                return
            cs = st.chunk_statuses[index]
            prev_state = cs.state
            for k, v in kwargs.items():
                setattr(cs, k, v)
            if "state" in kwargs and cs.state != prev_state:
                if prev_state == "done" and st.done_chunks > 0:
                    st.done_chunks -= 1
                if cs.state == "done":
                    st.done_chunks += 1

    def add_retry(self, job_id: str, index: int, inc: int, last_error_code: int | None, last_error_message: str | None) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.last_retry_count += inc
            if last_error_code is not None:
                st.last_error_code = last_error_code
            if not (0 <= index < len(st.chunk_statuses)):
                return
            cs = st.chunk_statuses[index]
            cs.retries += inc
            cs.last_error_code = last_error_code
            cs.last_error_message = last_error_message

    def add_stat(self, job_id: str, key: str, inc: int = 1) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.stats[key] = st.stats.get(key, 0) + inc

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False

            now = time.time()
            self._cancelled.add(job_id)
            self._paused.discard(job_id)

            # Update visible state immediately so clients can stop polling.
            if st.state not in {"done", "error"}:
                st.state = "cancelled"
                st.finished_at = now

            for cs in st.chunk_statuses:
                if cs.state in {"processing", "retrying"}:
                    cs.state = "pending"
                    cs.started_at = None
                    cs.finished_at = None
                    cs.last_error_message = cs.last_error_message or "cancelled"

            return True

    def pause(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False
            if st.state not in {"queued", "running"}:
                return False

            self._paused.add(job_id)
            st.state = "paused"
            st.finished_at = None
            return True

    def resume(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False
            if st.state != "paused" and job_id not in self._paused:
                return False

            self._paused.discard(job_id)
            if st.state == "paused":
                st.state = "queued"
                st.finished_at = None
            return True

    def delete(self, job_id: str) -> bool:
        with self._lock:
            existed = job_id in self._jobs
            self._jobs.pop(job_id, None)
            self._cancelled.discard(job_id)
            self._paused.discard(job_id)
            return existed

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled

    def is_paused(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._paused


GLOBAL_JOBS = JobStore()
