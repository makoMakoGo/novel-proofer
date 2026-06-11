from __future__ import annotations

import itertools
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from novel_proofer.env import env_float
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.job_records import (
    ArtifactRecord,
    ChunkRecord,
    ChunkSetRecord,
    DiagnosticsRecord,
    JobRecord,
    LLMRecord,
    TimestampRecord,
    WorkflowRecord,
    job_record_from_payload,
    job_record_to_payload,
)
from novel_proofer.states import ChunkState, JobPhase, JobState, WaitReason
from novel_proofer.workflow import (
    WorkflowContext,
    WorkflowEvent,
    WorkflowInvariantError,
    WorkflowState,
    require_event,
)

logger = logging.getLogger(__name__)

_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

_JOB_STATES = {state.value for state in JobState}
_WAIT_REASONS = {reason.value for reason in WaitReason}
_CHUNK_STATES = (
    ChunkState.PENDING,
    ChunkState.PROCESSING,
    ChunkState.RETRYING,
    ChunkState.DONE,
    ChunkState.ERROR,
)

_ALLOWED_JOB_UPDATE_FIELDS = {
    "state",
    "phase",
    "wait_reason",
    "started_at",
    "finished_at",
    "output_filename",
    "output_path",
    "work_dir",
    "total_chunks",
    "done_chunks",
    "format",
    "last_error_code",
    "last_retry_count",
    "last_llm_model",
    "error",
    "cleanup_debug_dir",
}

_ALLOWED_CHUNK_UPDATE_FIELDS = {
    "state",
    "started_at",
    "finished_at",
    "retries",
    "last_error_code",
    "last_error_message",
    "llm_model",
    "input_chars",
    "output_chars",
}

_tmp_seq = itertools.count()


def _tmp_suffix() -> str:
    return f".{os.getpid()}_{next(_tmp_seq)}.tmp"


@dataclass(frozen=True)
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
    # validate|process|merge|done
    phase: str
    # ready_to_process|user_paused|ready_to_merge|server_recovered when state=paused; otherwise None
    wait_reason: str | None
    created_at: float
    started_at: float | None
    finished_at: float | None

    input_filename: str
    output_filename: str
    total_chunks: int
    done_chunks: int

    # Options snapshot (used for resume/recovery and UI locks)
    format: FormatConfig = field(default_factory=FormatConfig)

    # Diagnostics
    last_error_code: int | None = None
    last_retry_count: int = 0
    last_llm_model: str | None = None

    stats: dict[str, int] = field(default_factory=dict)
    chunk_statuses: list[ChunkStatus] = field(default_factory=list)
    chunk_counts: dict[str, int] = field(default_factory=dict)

    error: str | None = None
    output_path: str | None = None
    work_dir: str | None = None
    cleanup_debug_dir: bool = True


def _new_chunk_counts() -> dict[str, int]:
    return {state.value: 0 for state in _CHUNK_STATES}


def _compute_chunk_counts(chunks: list[ChunkStatus]) -> dict[str, int]:
    out = _new_chunk_counts()
    for cs in chunks:
        out[cs.state] = out.get(cs.state, 0) + 1
    return out


def _chunk_to_record(cs: ChunkStatus) -> ChunkRecord:
    state = str(cs.state)
    started_at = cs.started_at
    finished_at = cs.finished_at
    if cs.state in {ChunkState.PROCESSING, ChunkState.RETRYING}:
        state = ChunkState.PENDING.value
        started_at = None
        finished_at = None
    return ChunkRecord(
        index=cs.index,
        state=state,
        started_at=started_at,
        finished_at=finished_at,
        retries=cs.retries,
        last_error_code=cs.last_error_code,
        last_error_message=cs.last_error_message,
        llm_model=cs.llm_model,
        input_chars=cs.input_chars,
        output_chars=cs.output_chars,
    )


def _chunk_from_record(record: ChunkRecord) -> ChunkStatus:
    return ChunkStatus(
        index=record.index,
        state=record.state,
        started_at=record.started_at,
        finished_at=record.finished_at,
        retries=record.retries,
        last_error_code=record.last_error_code,
        last_error_message=record.last_error_message,
        llm_model=record.llm_model,
        input_chars=record.input_chars,
        output_chars=record.output_chars,
    )


def _durable_workflow_for_snapshot(st: JobStatus) -> WorkflowRecord:
    if st.state in {JobState.QUEUED, JobState.RUNNING}:
        return WorkflowRecord(
            state=JobState.PAUSED.value,
            phase=str(st.phase),
            wait_reason=WaitReason.SERVER_RECOVERED.value,
        )
    return WorkflowRecord(
        state=str(st.state),
        phase=str(st.phase),
        wait_reason=None if st.wait_reason is None else str(st.wait_reason),
    )


def _job_to_record(st: JobStatus) -> JobRecord:
    chunks = [_chunk_to_record(chunk) for chunk in st.chunk_statuses]
    chunk_counts = _compute_chunk_counts([_chunk_from_record(chunk) for chunk in chunks])
    return JobRecord(
        job_id=st.job_id,
        workflow=_durable_workflow_for_snapshot(st),
        timestamps=TimestampRecord(
            created_at=st.created_at,
            started_at=st.started_at,
            finished_at=st.finished_at,
        ),
        artifacts=ArtifactRecord(
            input_filename=st.input_filename,
            output_filename=st.output_filename,
            output_path=st.output_path,
            work_dir=st.work_dir,
            cleanup_debug_dir=st.cleanup_debug_dir,
        ),
        format=st.format,
        llm=LLMRecord(
            last_error_code=st.last_error_code,
            last_retry_count=st.last_retry_count,
            last_llm_model=st.last_llm_model,
        ),
        chunks=ChunkSetRecord(
            total=len(chunks),
            done=sum(1 for chunk in chunks if chunk.state == ChunkState.DONE),
            counts=chunk_counts,
            items=chunks,
        ),
        diagnostics=DiagnosticsRecord(
            stats=dict(st.stats),
            error=st.error,
        ),
    )


def _job_from_record(record: JobRecord) -> JobStatus:
    chunks = [_chunk_from_record(chunk) for chunk in record.chunks.items]
    return JobStatus(
        job_id=record.job_id,
        state=record.workflow.state,
        phase=record.workflow.phase,
        wait_reason=record.workflow.wait_reason,
        created_at=record.timestamps.created_at,
        started_at=record.timestamps.started_at,
        finished_at=record.timestamps.finished_at,
        input_filename=record.artifacts.input_filename,
        output_filename=record.artifacts.output_filename,
        total_chunks=record.chunks.total,
        done_chunks=record.chunks.done,
        format=record.format,
        last_error_code=record.llm.last_error_code,
        last_retry_count=record.llm.last_retry_count,
        last_llm_model=record.llm.last_llm_model,
        stats=dict(record.diagnostics.stats),
        chunk_statuses=chunks,
        chunk_counts=dict(record.chunks.counts),
        error=record.diagnostics.error,
        output_path=record.artifacts.output_path,
        work_dir=record.artifacts.work_dir,
        cleanup_debug_dir=record.artifacts.cleanup_debug_dir,
    )


class JobStore:
    def __init__(self, *, persist_interval_s: float | None = None) -> None:
        self._lock = threading.Lock()
        self._persist_cv = threading.Condition(self._lock)
        self._persist_lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()
        # In-memory store for pre-processed chunk texts to eliminate small file I/O.
        # Not persisted to disk; cleared per-chunk after LLM processing or when jobs are deleted.
        self._pre_texts: dict[str, dict[int, str]] = {}
        self._persist_dir: Path | None = None
        interval = (
            env_float("NOVEL_PROOFER_JOB_PERSIST_INTERVAL_S", 5.0)
            if persist_interval_s is None
            else float(persist_interval_s)
        )
        self._persist_interval_s = max(0.1, interval)
        self._persist_dirty_since: dict[str, float] = {}
        self._persist_seq: dict[str, int] = {}
        self._persist_thread: threading.Thread | None = None
        self._persist_stop = False

    def configure_persistence(self, *, persist_dir: Path) -> None:
        persist_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._persist_dir = persist_dir
            self._start_persist_thread_locked()

    def shutdown_persistence(self, *, wait: bool = False) -> None:
        t: threading.Thread | None
        with self._lock:
            self._persist_stop = True
            self._persist_cv.notify_all()
            t = self._persist_thread
        if wait and t is not None:
            try:
                t.join(timeout=1.0)
            except Exception:
                logger.exception("failed to join job persistence thread")

    def _start_persist_thread_locked(self) -> None:
        if self._persist_thread is not None and self._persist_thread.is_alive():
            return
        self._persist_stop = False
        t = threading.Thread(target=self._persist_loop, name="novel-proofer-job-persist", daemon=True)
        self._persist_thread = t
        t.start()

    def _persist_path_for_job_id(self, job_id: str) -> Path | None:
        if not self._persist_dir:
            return None
        job_id = (job_id or "").strip()
        if not job_id or not _JOB_ID_RE.fullmatch(job_id):
            return None
        return self._persist_dir / f"{job_id}.json"

    def _atomic_write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + _tmp_suffix())
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            for attempt in range(10):
                try:
                    tmp.replace(path)
                    break
                except PermissionError:
                    if attempt >= 9:
                        raise
                    # Windows can transiently lock files (e.g., AV scanners / concurrent readers).
                    time.sleep(0.02 * (attempt + 1))
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                logger.exception("failed to cleanup temp job record file: %s", tmp)

    def _persist_snapshot_unlocked(self, snapshot: JobStatus) -> None:
        path = self._persist_path_for_job_id(snapshot.job_id)
        if path is None:
            return
        try:
            self._atomic_write_json(path, job_record_to_payload(_job_to_record(snapshot)))
        except Exception:
            logger.exception("failed to persist job record: job_id=%s", snapshot.job_id)

    def _persist_snapshot_direct(self, snapshot: JobStatus) -> None:
        with self._persist_lock:
            self._persist_snapshot_unlocked(snapshot)

    def _bump_persist_seq_locked(self, job_id: str, *, mark_dirty: bool) -> int:
        seq = int(self._persist_seq.get(job_id, 0) or 0) + 1
        self._persist_seq[job_id] = seq
        if mark_dirty and self._persist_dir is not None:
            self._persist_dirty_since.setdefault(job_id, time.monotonic())
            self._persist_cv.notify_all()
        return seq

    def _mark_dirty_locked(self, job_id: str) -> None:
        if self._persist_dir is None:
            return
        self._bump_persist_seq_locked(job_id, mark_dirty=True)

    def flush_persistence(self, job_id: str | None = None) -> None:
        if self._persist_dir is None:
            return
        if job_id is not None:
            self._flush_job(job_id, require_dirty=False)
            return
        with self._lock:
            job_ids = list(self._persist_dirty_since.keys())
        for jid in job_ids:
            self._flush_job(jid, require_dirty=True)

    def _persist_loop(self) -> None:
        while True:
            with self._lock:
                while not self._persist_stop and not self._persist_dirty_since:
                    self._persist_cv.wait()
                if self._persist_stop:
                    return

                now = time.monotonic()
                due: list[str] = []
                next_deadline: float | None = None
                for job_id, since in self._persist_dirty_since.items():
                    deadline = since + self._persist_interval_s
                    if deadline <= now:
                        due.append(job_id)
                        continue
                    if next_deadline is None or deadline < next_deadline:
                        next_deadline = deadline

                if not due:
                    timeout = 0.5
                    if next_deadline is not None:
                        timeout = max(0.0, next_deadline - now)
                    self._persist_cv.wait(timeout=timeout)
                    continue

            for jid in due:
                try:
                    self._flush_job(jid, require_dirty=True)
                except Exception:
                    logger.exception("failed to flush dirty job record: job_id=%s", jid)

    def _flush_job(self, job_id: str, *, require_dirty: bool) -> None:
        if self._persist_dir is None:
            return

        with self._persist_lock:
            snap: JobStatus | None = None
            seq: int = 0
            with self._lock:
                if self._persist_dir is None:
                    self._persist_dirty_since.pop(job_id, None)
                    return
                if require_dirty and job_id not in self._persist_dirty_since:
                    return
                st = self._jobs.get(job_id)
                if st is None:
                    self._persist_dirty_since.pop(job_id, None)
                    self._persist_seq.pop(job_id, None)
                    return
                seq = int(self._persist_seq.get(job_id, 0) or 0)
                snap = self._snapshot_job(st)

            if snap is not None:
                self._persist_snapshot_unlocked(snap)

            with self._lock:
                if job_id not in self._jobs:
                    self._persist_dirty_since.pop(job_id, None)
                    self._persist_seq.pop(job_id, None)
                    return
                if int(self._persist_seq.get(job_id, 0) or 0) == seq:
                    self._persist_dirty_since.pop(job_id, None)
                    return
                self._persist_dirty_since[job_id] = time.monotonic()
                self._persist_cv.notify_all()

    def _restore_loaded_job_after_restart(self, st: JobStatus) -> tuple[JobStatus, bool]:
        changed = False
        if st.state in {JobState.QUEUED, JobState.RUNNING}:
            logger.warning("restoring in-flight job as paused after restart: job_id=%s state=%s", st.job_id, st.state)
            if st.chunk_counts:
                context = WorkflowContext.from_counts(
                    state=st.state,
                    phase=st.phase,
                    wait_reason=st.wait_reason,
                    total_chunks=st.total_chunks,
                    chunk_counts=dict(st.chunk_counts),
                )
            else:
                context = WorkflowContext.from_values(
                    state=st.state,
                    phase=st.phase,
                    wait_reason=st.wait_reason,
                    chunks=[chunk.state for chunk in st.chunk_statuses],
                )
            transition = require_event(context, WorkflowEvent.RESTART_RECOVERY)
            assert transition.next_state is not None
            st.state = transition.next_state.state
            st.phase = transition.next_state.phase
            st.wait_reason = transition.next_state.wait_reason
            st.finished_at = None
            changed = True

        _in_flight = {ChunkState.PROCESSING, ChunkState.RETRYING}
        original_chunks = list(st.chunk_statuses)
        st.chunk_statuses = [
            replace(cs, state=ChunkState.PENDING, started_at=None, finished_at=None) if cs.state in _in_flight else cs
            for cs in original_chunks
        ]
        st.chunk_counts = _compute_chunk_counts(st.chunk_statuses)
        for old_cs, new_cs in zip(original_chunks, st.chunk_statuses, strict=False):
            if old_cs is not new_cs:
                logger.warning(
                    "restoring in-flight chunk as pending after restart: job_id=%s chunk=%s state=%s",
                    st.job_id,
                    old_cs.index,
                    old_cs.state,
                )
                changed = True
        return st, changed

    def load_persisted_jobs(self) -> int:
        persist_dir = self._persist_dir
        if persist_dir is None or not persist_dir.exists():
            return 0

        loaded: list[tuple[JobStatus, bool]] = []
        for p in sorted(persist_dir.glob("*.json")):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                st, needs_rewrite = self._restore_loaded_job_after_restart(
                    _job_from_record(job_record_from_payload(obj))
                )
            except Exception as e:
                raise ValueError(
                    f"failed to load persisted job record {p}; delete the corrupted state file and retry"
                ) from e
            loaded.append((st, needs_rewrite))

        with self._lock:
            for st, _needs_rewrite in loaded:
                self._jobs[st.job_id] = st
                if st.state == JobState.CANCELLED:
                    self._cancelled.add(st.job_id)
                if st.state == JobState.PAUSED:
                    self._paused.add(st.job_id)

        for st, needs_rewrite in loaded:
            if needs_rewrite:
                self._persist_snapshot_direct(self._snapshot_job(st))

        return len(loaded)

    def _snapshot_job(self, st: JobStatus, *, include_chunks: bool = True) -> JobStatus:
        return JobStatus(
            job_id=st.job_id,
            state=st.state,
            phase=st.phase,
            wait_reason=st.wait_reason,
            created_at=st.created_at,
            started_at=st.started_at,
            finished_at=st.finished_at,
            input_filename=st.input_filename,
            output_filename=st.output_filename,
            output_path=st.output_path,
            total_chunks=st.total_chunks,
            done_chunks=st.done_chunks,
            format=st.format,
            last_error_code=st.last_error_code,
            last_retry_count=st.last_retry_count,
            last_llm_model=st.last_llm_model,
            stats=dict(st.stats),
            chunk_statuses=list(st.chunk_statuses) if include_chunks else [],
            chunk_counts=dict(st.chunk_counts),
            error=st.error,
            work_dir=st.work_dir,
            cleanup_debug_dir=st.cleanup_debug_dir,
        )

    def create(self, input_filename: str, output_filename: str, total_chunks: int) -> JobStatus:
        job_id = uuid.uuid4().hex
        status = JobStatus(
            job_id=job_id,
            state=JobState.QUEUED,
            phase=JobPhase.VALIDATE,
            wait_reason=None,
            created_at=time.time(),
            started_at=None,
            finished_at=None,
            input_filename=input_filename,
            output_filename=output_filename,
            total_chunks=total_chunks,
            done_chunks=0,
            chunk_counts=_new_chunk_counts(),
        )
        with self._lock:
            self._jobs[job_id] = status
            snap = self._snapshot_job(status)
        self._persist_snapshot_direct(snap)
        return snap

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            return self._snapshot_job(st)

    def get_summary(self, job_id: str) -> JobStatus | None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            return self._snapshot_job(st, include_chunks=False)

    def list_summaries(self) -> list[JobStatus]:
        with self._lock:
            snapshots = [self._snapshot_job(s, include_chunks=False) for s in self._jobs.values()]
        snapshots.sort(key=lambda s: s.created_at, reverse=True)
        return snapshots

    def get_chunks_page(
        self,
        job_id: str,
        *,
        chunk_state: str,
        limit: int,
        offset: int,
    ) -> tuple[list[ChunkStatus], dict[str, int], bool] | None:
        wanted = str(chunk_state or "all").strip().lower()
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            counts = dict(st.chunk_counts)
            if (not counts) and st.chunk_statuses:
                counts = _compute_chunk_counts(st.chunk_statuses)
                st.chunk_counts = dict(counts)
            out: list[ChunkStatus] = []
            matched = 0
            has_more = False
            for cs in st.chunk_statuses:
                if wanted == "active":
                    if cs.state not in {ChunkState.PROCESSING, ChunkState.RETRYING}:
                        continue
                elif wanted != "all" and cs.state != wanted:
                    continue
                if matched < offset:
                    matched += 1
                    continue
                if limit > 0 and len(out) >= limit:
                    has_more = True
                    break
                out.append(cs)
                matched += 1
            return out, counts, has_more

    def update(self, job_id: str, **kwargs: Any) -> None:
        bad = kwargs.keys() - _ALLOWED_JOB_UPDATE_FIELDS
        if bad:
            raise ValueError(f"JobStore.update: unknown fields {bad}")
        flush_now = False
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if st.state == JobState.CANCELLED:
                return
            requested_state = str(kwargs.get("state", st.state))
            if requested_state not in _JOB_STATES:
                raise ValueError(f"JobStore.update: state must be one of {sorted(_JOB_STATES)!r}")
            target_state = (
                str(st.state)
                if str(st.state) == JobState.PAUSED.value
                and requested_state in {JobState.QUEUED.value, JobState.RUNNING.value}
                else requested_state
            )
            raw_wait_reason = kwargs.get(
                "wait_reason", st.wait_reason if target_state == JobState.PAUSED.value else None
            )
            target_wait_reason = None if raw_wait_reason is None else str(raw_wait_reason)
            if target_wait_reason is not None and target_wait_reason not in _WAIT_REASONS:
                raise ValueError(f"JobStore.update: wait_reason must be one of {sorted(_WAIT_REASONS)!r}")
            if target_state == JobState.PAUSED.value and target_wait_reason is None:
                raise ValueError("JobStore.update: wait_reason is required when state is paused")
            if target_state != JobState.PAUSED.value and target_wait_reason is not None:
                raise ValueError("JobStore.update: wait_reason must be None unless state is paused")
            target_phase = str(kwargs.get("phase", st.phase))
            try:
                WorkflowState.from_values(target_state, target_phase, target_wait_reason)
            except WorkflowInvariantError as e:
                raise ValueError(f"JobStore.update: {e}") from e
            normalized = dict(kwargs)
            if "state" in normalized:
                normalized["state"] = target_state
            normalized["wait_reason"] = target_wait_reason if target_state == JobState.PAUSED.value else None
            for k, v in normalized.items():
                if k == "started_at" and st.started_at is not None and v is not None:
                    continue
                if k == "state" and str(v) in {JobState.DONE.value, JobState.ERROR.value, JobState.CANCELLED.value}:
                    self._paused.discard(job_id)
                setattr(st, k, v)
            self._mark_dirty_locked(job_id)
            flush_now = st.state in {JobState.DONE, JobState.ERROR, JobState.CANCELLED}
        if flush_now:
            self._flush_job(job_id, require_dirty=False)

    def init_chunks(self, job_id: str, total_chunks: int, *, llm_model: str | None = None) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.total_chunks = total_chunks
            st.done_chunks = 0
            st.chunk_statuses = [
                ChunkStatus(index=i, state=ChunkState.PENDING, llm_model=llm_model) for i in range(total_chunks)
            ]
            st.chunk_counts = _new_chunk_counts()
            st.chunk_counts[ChunkState.PENDING] = total_chunks
            self._mark_dirty_locked(job_id)
        self._flush_job(job_id, require_dirty=False)

    def update_chunk(self, job_id: str, index: int, **kwargs: Any) -> None:
        bad = kwargs.keys() - _ALLOWED_CHUNK_UPDATE_FIELDS
        if bad:
            raise ValueError(f"JobStore.update_chunk: unknown fields {bad}")
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            if st.state == JobState.CANCELLED or job_id in self._cancelled:
                return
            if index < 0 or index >= len(st.chunk_statuses):
                return
            cs = st.chunk_statuses[index]
            prev_state = cs.state
            should_persist = False
            cs = replace(cs, **kwargs)
            st.chunk_statuses[index] = cs
            if "state" in kwargs and cs.state != prev_state:
                should_persist = True
                st.chunk_counts[prev_state] = max(0, st.chunk_counts.get(prev_state, 0) - 1)
                st.chunk_counts[cs.state] = st.chunk_counts.get(cs.state, 0) + 1
                if prev_state == ChunkState.DONE and st.done_chunks > 0:
                    st.done_chunks -= 1
                if cs.state == ChunkState.DONE:
                    st.done_chunks += 1
            if any(k in kwargs for k in ("retries", "last_error_code", "last_error_message")):
                should_persist = True
            if should_persist:
                self._mark_dirty_locked(job_id)

    def add_retry(
        self, job_id: str, index: int, inc: int, last_error_code: int | None, last_error_message: str | None
    ) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.last_retry_count += inc
            if last_error_code is not None:
                st.last_error_code = last_error_code
            if 0 <= index < len(st.chunk_statuses):
                cs = st.chunk_statuses[index]
                st.chunk_statuses[index] = replace(
                    cs, retries=cs.retries + inc, last_error_code=last_error_code, last_error_message=last_error_message
                )
            self._mark_dirty_locked(job_id)

    def add_stat(self, job_id: str, key: str, inc: int = 1) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.stats[key] = st.stats.get(key, 0) + inc
        # Stats are non-critical diagnostics; avoid persisting on every increment for performance.

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False

            now = time.time()
            self._cancelled.add(job_id)
            self._paused.discard(job_id)

            # Update visible state immediately so clients can stop polling.
            if st.state not in {JobState.DONE, JobState.ERROR}:
                st.state = JobState.CANCELLED
                st.wait_reason = None
                st.finished_at = now

            for i, cs in enumerate(st.chunk_statuses):
                if cs.state in {ChunkState.PROCESSING, ChunkState.RETRYING}:
                    st.chunk_counts[cs.state] = max(0, st.chunk_counts.get(cs.state, 0) - 1)
                    st.chunk_counts[ChunkState.PENDING] = st.chunk_counts.get(ChunkState.PENDING, 0) + 1
                    st.chunk_statuses[i] = replace(
                        cs,
                        state=ChunkState.PENDING,
                        started_at=None,
                        finished_at=None,
                        last_error_message=cs.last_error_message or "cancelled",
                    )
            self._mark_dirty_locked(job_id)
        self._flush_job(job_id, require_dirty=False)
        return True

    def pause(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False
            if st.state not in {JobState.QUEUED, JobState.RUNNING}:
                return False

            self._paused.add(job_id)
            st.state = JobState.PAUSED
            st.wait_reason = WaitReason.USER_PAUSED
            st.finished_at = None
            self._mark_dirty_locked(job_id)
        return True

    def resume(self, job_id: str) -> bool:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return False
            if st.state != JobState.PAUSED and job_id not in self._paused:
                return False

            self._paused.discard(job_id)
            if st.state == JobState.PAUSED:
                st.state = JobState.QUEUED
                st.wait_reason = None
                st.finished_at = None
            self._mark_dirty_locked(job_id)
        return True

    def delete(self, job_id: str) -> bool:
        path: Path | None = None
        existed: bool
        with self._persist_lock:
            with self._lock:
                existed = job_id in self._jobs
                # Drop in-memory pre-texts, if any.
                self._pre_texts.pop(job_id, None)
                if existed:
                    path = self._persist_path_for_job_id(job_id)
                self._jobs.pop(job_id, None)
                self._cancelled.discard(job_id)
                self._paused.discard(job_id)
                self._persist_dirty_since.pop(job_id, None)
                self._persist_seq.pop(job_id, None)
            if path is None:
                return existed
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                logger.exception("failed to delete persisted job record: job_id=%s", job_id)
            return existed

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._cancelled

    # Pre-chunk text accessors (memory-only, thread-safe)
    def set_chunk_pre_text(self, job_id: str, index: int, text: str) -> None:
        with self._lock:
            self._pre_texts.setdefault(job_id, {})[int(index)] = text

    def get_chunk_pre_text(self, job_id: str, index: int) -> str | None:
        with self._lock:
            d = self._pre_texts.get(job_id)
            return None if d is None else d.get(int(index))

    def pop_chunk_pre_text(self, job_id: str, index: int) -> str | None:
        with self._lock:
            d = self._pre_texts.get(job_id)
            if not d:
                return None
            val = d.pop(int(index), None)
            if not d:
                self._pre_texts.pop(job_id, None)
            return val

    def clear_all_pre_texts(self, job_id: str) -> None:
        with self._lock:
            self._pre_texts.pop(job_id, None)

    def is_paused(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._paused


GLOBAL_JOBS = JobStore()
