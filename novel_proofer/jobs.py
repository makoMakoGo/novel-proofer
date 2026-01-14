from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

_JOB_STATE_VERSION = 1
_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)


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
    llm_model: str | None = None

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
    last_llm_model: str | None = None

    stats: dict[str, int] = field(default_factory=dict)
    chunk_statuses: list[ChunkStatus] = field(default_factory=list)

    error: str | None = None
    output_path: str | None = None
    work_dir: str | None = None
    cleanup_debug_dir: bool = True


def _chunk_to_dict(cs: ChunkStatus) -> dict:
    return {
        "index": int(cs.index),
        "state": str(cs.state),
        "started_at": cs.started_at,
        "finished_at": cs.finished_at,
        "retries": int(cs.retries),
        "last_error_code": cs.last_error_code,
        "last_error_message": cs.last_error_message,
        "llm_model": cs.llm_model,
        "input_chars": cs.input_chars,
        "output_chars": cs.output_chars,
    }


def _chunk_from_dict(d: dict) -> ChunkStatus:
    llm_model = d.get("llm_model")
    if llm_model is not None:
        llm_model = str(llm_model).strip() or None
    return ChunkStatus(
        index=int(d.get("index", 0)),
        state=str(d.get("state", "pending")),
        started_at=d.get("started_at"),
        finished_at=d.get("finished_at"),
        retries=int(d.get("retries", 0) or 0),
        last_error_code=d.get("last_error_code"),
        last_error_message=d.get("last_error_message"),
        llm_model=llm_model,
        input_chars=d.get("input_chars"),
        output_chars=d.get("output_chars"),
    )


def _job_to_dict(st: JobStatus) -> dict:
    return {
        "version": _JOB_STATE_VERSION,
        "job": {
            "job_id": str(st.job_id),
            "state": str(st.state),
            "created_at": float(st.created_at),
            "started_at": st.started_at,
            "finished_at": st.finished_at,
            "input_filename": str(st.input_filename),
            "output_filename": str(st.output_filename),
            "output_path": st.output_path,
            "total_chunks": int(st.total_chunks),
            "done_chunks": int(st.done_chunks),
            "last_error_code": st.last_error_code,
            "last_retry_count": int(st.last_retry_count),
            "last_llm_model": st.last_llm_model,
            "stats": dict(st.stats),
            "error": st.error,
            "work_dir": st.work_dir,
            "cleanup_debug_dir": bool(st.cleanup_debug_dir),
            "chunk_statuses": [_chunk_to_dict(c) for c in st.chunk_statuses],
        },
    }


def _job_from_dict(d: dict) -> JobStatus:
    version_raw = d.get("version", _JOB_STATE_VERSION) if isinstance(d, dict) else _JOB_STATE_VERSION
    try:
        version = int(version_raw)
    except Exception:
        version = None
    if version is not None and version != _JOB_STATE_VERSION:
        logger.warning("job state version mismatch: expected %d, got %s", _JOB_STATE_VERSION, version_raw)

    job = d.get("job") if isinstance(d, dict) else None
    if not isinstance(job, dict):
        raise ValueError("invalid job state file: missing job object")

    chunk_dicts = job.get("chunk_statuses", [])
    chunks: list[ChunkStatus] = []
    if isinstance(chunk_dicts, list):
        for item in chunk_dicts:
            if isinstance(item, dict):
                chunks.append(_chunk_from_dict(item))

    stats = job.get("stats")
    if not isinstance(stats, dict):
        stats = {}

    last_llm_model = job.get("last_llm_model")
    if last_llm_model is not None:
        last_llm_model = str(last_llm_model).strip() or None

    return JobStatus(
        job_id=str(job.get("job_id", "")),
        state=str(job.get("state", "queued")),
        created_at=float(job.get("created_at", time.time())),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        input_filename=str(job.get("input_filename", "")),
        output_filename=str(job.get("output_filename", "")),
        total_chunks=int(job.get("total_chunks", len(chunks)) or 0),
        done_chunks=int(job.get("done_chunks", 0) or 0),
        last_error_code=job.get("last_error_code"),
        last_retry_count=int(job.get("last_retry_count", 0) or 0),
        last_llm_model=last_llm_model,
        stats=dict(stats),
        chunk_statuses=chunks,
        error=job.get("error"),
        output_path=job.get("output_path"),
        work_dir=job.get("work_dir"),
        cleanup_debug_dir=bool(job.get("cleanup_debug_dir", True)),
    )


class JobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()
        self._persist_dir: Path | None = None

    def configure_persistence(self, *, persist_dir: Path) -> None:
        persist_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._persist_dir = persist_dir

    def _persist_path_for_job_id(self, job_id: str) -> Path | None:
        if not self._persist_dir:
            return None
        job_id = (job_id or "").strip()
        if not job_id or not _JOB_ID_RE.fullmatch(job_id):
            return None
        return self._persist_dir / f"{job_id}.json"

    def _atomic_write_json(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
        finally:
            with suppress(Exception):
                tmp.unlink(missing_ok=True)

    def _persist_snapshot(self, snapshot: JobStatus) -> None:
        path = self._persist_path_for_job_id(snapshot.job_id)
        if path is None:
            return
        try:
            self._atomic_write_json(path, _job_to_dict(snapshot))
        except Exception:
            logger.exception("failed to persist job state: job_id=%s", snapshot.job_id)

    def _heal_loaded_job(self, st: JobStatus) -> JobStatus:
        # After a server restart, there is no in-flight work. Make state explicit and resumable.
        if st.state in {"queued", "running"}:
            st.state = "paused"
            st.finished_at = None

        # Any in-flight chunks are no longer running; convert them back to pending.
        for cs in st.chunk_statuses:
            if cs.state in {"processing", "retrying"}:
                cs.state = "pending"
                cs.started_at = None
                cs.finished_at = None

        # Keep counters consistent even if older files were incomplete.
        st.total_chunks = max(int(st.total_chunks), len(st.chunk_statuses))
        st.done_chunks = sum(1 for c in st.chunk_statuses if c.state == "done")
        return st

    def load_persisted_jobs(self) -> int:
        persist_dir = self._persist_dir
        if persist_dir is None or not persist_dir.exists():
            return 0

        loaded: list[JobStatus] = []
        for p in persist_dir.glob("*.json"):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                st = self._heal_loaded_job(_job_from_dict(obj))
                if not st.job_id:
                    continue
                loaded.append(st)
            except Exception:
                logger.exception("failed to load persisted job: %s", p)

        with self._lock:
            for st in loaded:
                self._jobs[st.job_id] = st
                if st.state == "cancelled":
                    self._cancelled.add(st.job_id)
                if st.state == "paused":
                    self._paused.add(st.job_id)

        return len(loaded)

    def _snapshot_chunk(self, cs: ChunkStatus) -> ChunkStatus:
        return ChunkStatus(
            index=cs.index,
            state=cs.state,
            started_at=cs.started_at,
            finished_at=cs.finished_at,
            retries=cs.retries,
            last_error_code=cs.last_error_code,
            last_error_message=cs.last_error_message,
            llm_model=cs.llm_model,
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
            last_llm_model=st.last_llm_model,
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
            snap = self._snapshot_job(status)
        self._persist_snapshot(snap)
        return snap

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            return self._snapshot_job(st)

    def update(self, job_id: str, **kwargs) -> None:
        snap: JobStatus | None = None
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
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)

    def init_chunks(self, job_id: str, total_chunks: int, *, llm_model: str | None = None) -> None:
        snap: JobStatus | None = None
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.total_chunks = total_chunks
            st.done_chunks = 0
            st.chunk_statuses = [
                ChunkStatus(index=i, state="pending", llm_model=llm_model) for i in range(total_chunks)
            ]
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)

    def update_chunk(self, job_id: str, index: int, **kwargs) -> None:
        snap: JobStatus | None = None
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
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)

    def add_retry(
        self, job_id: str, index: int, inc: int, last_error_code: int | None, last_error_message: str | None
    ) -> None:
        snap: JobStatus | None = None
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
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)

    def add_stat(self, job_id: str, key: str, inc: int = 1) -> None:
        snap: JobStatus | None = None
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.stats[key] = st.stats.get(key, 0) + inc
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)

    def cancel(self, job_id: str) -> bool:
        snap: JobStatus | None = None
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

            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)
        return True

    def pause(self, job_id: str) -> bool:
        snap: JobStatus | None = None
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False
            if st.state not in {"queued", "running"}:
                return False

            self._paused.add(job_id)
            st.state = "paused"
            st.finished_at = None
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)
        return True

    def resume(self, job_id: str) -> bool:
        snap: JobStatus | None = None
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
            snap = self._snapshot_job(st)
        if snap is not None:
            self._persist_snapshot(snap)
        return True

    def delete(self, job_id: str) -> bool:
        path: Path | None = None
        existed: bool
        with self._lock:
            existed = job_id in self._jobs
            if existed:
                path = self._persist_path_for_job_id(job_id)
            self._jobs.pop(job_id, None)
            self._cancelled.discard(job_id)
            self._paused.discard(job_id)
        if path is not None:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                logger.exception("failed to delete persisted job state: job_id=%s", job_id)
        return existed

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled

    def is_paused(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._paused


GLOBAL_JOBS = JobStore()
