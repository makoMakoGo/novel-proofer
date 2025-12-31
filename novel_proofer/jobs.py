from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class ChunkStatus:
    index: int
    state: str  # queued|running|done|error
    started_at: float | None = None
    finished_at: float | None = None
    retries: int = 0
    last_error_code: int | None = None
    last_error_message: str | None = None
    splits: int = 0


@dataclass
class JobStatus:
    job_id: str
    state: str  # queued|running|done|error|cancelled
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


class JobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()

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
        return status

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            return self._jobs.get(job_id)

    def update(self, job_id: str, **kwargs) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            for k, v in kwargs.items():
                # Once cancelled, do not let other code paths flip state back.
                if k == "state" and st.state == "cancelled":
                    continue
                setattr(st, k, v)

    def init_chunks(self, job_id: str, total_chunks: int) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.total_chunks = total_chunks
            st.done_chunks = 0
            st.chunk_statuses = [ChunkStatus(index=i, state="queued") for i in range(total_chunks)]

    def update_chunk(self, job_id: str, index: int, **kwargs) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if index < 0 or index >= len(st.chunk_statuses):
                return
            cs = st.chunk_statuses[index]
            for k, v in kwargs.items():
                setattr(cs, k, v)

    def add_retry(self, job_id: str, index: int, inc: int, last_error_code: int | None, last_error_message: str | None) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.last_retry_count += inc
            if last_error_code is not None:
                st.last_error_code = last_error_code
            if 0 <= index < len(st.chunk_statuses):
                cs = st.chunk_statuses[index]
                cs.retries += inc
                cs.last_error_code = last_error_code
                cs.last_error_message = last_error_message

    def add_split(self, job_id: str, index: int, inc: int = 1) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if 0 <= index < len(st.chunk_statuses):
                st.chunk_statuses[index].splits += inc

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

            self._cancelled.add(job_id)

            # Update visible state immediately so clients can stop polling.
            if st.state not in {"done", "error"}:
                st.state = "cancelled"
                st.finished_at = time.time()

            return True

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled


GLOBAL_JOBS = JobStore()
