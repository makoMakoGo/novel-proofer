from __future__ import annotations

import itertools
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from novel_proofer.env import env_float
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.states import ChunkState, JobPhase, JobState

logger = logging.getLogger(__name__)

_JOB_STATE_VERSION = 2
_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

_JOB_PHASES = {phase.value for phase in JobPhase}
_JOB_STATES = {state.value for state in JobState}
_CHUNK_STATE_VALUES = {state.value for state in ChunkState}
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


_FORMAT_DEFAULTS = FormatConfig()


def _new_chunk_counts() -> dict[str, int]:
    return {s: 0 for s in _CHUNK_STATES}


def _compute_chunk_counts(chunks: list[ChunkStatus]) -> dict[str, int]:
    out = _new_chunk_counts()
    for cs in chunks:
        out[cs.state] = out.get(cs.state, 0) + 1
    return out


def _require_dict(raw: object, *, context: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{context} must be an object")
    return raw


def _reject_unknown_fields(raw: dict[str, Any], allowed: set[str], *, context: str) -> None:
    extra = sorted(set(raw) - allowed)
    if extra:
        raise ValueError(f"{context} contains unknown fields: {', '.join(extra)}")


def _require_field(raw: dict[str, Any], key: str, *, context: str) -> Any:
    if key not in raw:
        raise ValueError(f"{context} missing field {key!r}")
    return raw[key]


def _parse_int(raw: Any, *, context: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"{context} must be an integer")
    return raw


def _parse_float(raw: Any, *, context: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"{context} must be a number")
    return float(raw)


def _parse_bool(raw: Any, *, context: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{context} must be a boolean")
    return raw


def _parse_optional_float(raw: Any, *, context: str) -> float | None:
    if raw is None:
        return None
    return _parse_float(raw, context=context)


def _parse_optional_int(raw: Any, *, context: str) -> int | None:
    if raw is None:
        return None
    return _parse_int(raw, context=context)


def _parse_non_negative_int(raw: Any, *, context: str) -> int:
    value = _parse_int(raw, context=context)
    if value < 0:
        raise ValueError(f"{context} must be >= 0")
    return value


def _parse_optional_non_negative_int(raw: Any, *, context: str) -> int | None:
    if raw is None:
        return None
    return _parse_non_negative_int(raw, context=context)


def _parse_str(raw: Any, *, context: str, allow_empty: bool = True) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{context} must be a string")
    if not allow_empty and not raw.strip():
        raise ValueError(f"{context} must not be empty")
    return raw


def _parse_optional_str(raw: Any, *, context: str) -> str | None:
    if raw is None:
        return None
    return _parse_str(raw, context=context)


def _parse_enum(raw: Any, *, context: str, allowed: set[str]) -> str:
    value = _parse_str(raw, context=context, allow_empty=False)
    if value not in allowed:
        raise ValueError(f"{context} must be one of {sorted(allowed)!r}")
    return value


def _parse_stats(raw: object) -> dict[str, int]:
    stats = _require_dict(raw, context="job.stats")
    out: dict[str, int] = {}
    for key, value in stats.items():
        if not isinstance(key, str) or not key:
            raise ValueError("job.stats keys must be non-empty strings")
        out[key] = _parse_non_negative_int(value, context=f"job.stats[{key!r}]")
    return out


def _parse_chunk_counts(raw: object, chunks: list[ChunkStatus]) -> dict[str, int]:
    counts = _require_dict(raw, context="job.chunk_counts")
    expected = {str(state) for state in _CHUNK_STATES}
    extra = sorted(set(counts) - expected)
    if extra:
        raise ValueError(f"job.chunk_counts contains unknown fields: {', '.join(extra)}")
    missing = sorted(expected - set(counts))
    if missing:
        raise ValueError(f"job.chunk_counts missing fields: {', '.join(missing)}")
    out = {key: _parse_non_negative_int(value, context=f"job.chunk_counts[{key!r}]") for key, value in counts.items()}
    computed = _compute_chunk_counts(chunks)
    if out != computed:
        raise ValueError("job.chunk_counts does not match chunk_statuses")
    return out


def _format_config_from_dict(raw: object) -> FormatConfig:
    config = _require_dict(raw, context="job.format")
    allowed = {f.name for f in fields(FormatConfig)}
    _reject_unknown_fields(config, allowed, context="job.format")
    kwargs: dict[str, Any] = {}
    for f in fields(FormatConfig):
        value = _require_field(config, f.name, context="job.format")
        default = getattr(_FORMAT_DEFAULTS, f.name)
        if isinstance(default, bool):
            kwargs[f.name] = _parse_bool(value, context=f"job.format.{f.name}")
            continue
        if isinstance(default, int):
            kwargs[f.name] = _parse_int(value, context=f"job.format.{f.name}")
            continue
        kwargs[f.name] = value
    return FormatConfig(**kwargs)


def _chunk_to_dict(cs: ChunkStatus) -> dict:
    return asdict(cs)


def _chunk_from_dict(d: dict) -> ChunkStatus:
    raw = _require_dict(d, context="chunk")
    _reject_unknown_fields(
        raw,
        {
            "index",
            "state",
            "started_at",
            "finished_at",
            "retries",
            "last_error_code",
            "last_error_message",
            "llm_model",
            "input_chars",
            "output_chars",
        },
        context="chunk",
    )
    return ChunkStatus(
        index=_parse_non_negative_int(_require_field(raw, "index", context="chunk"), context="chunk.index"),
        state=_parse_enum(
            _require_field(raw, "state", context="chunk"), context="chunk.state", allowed=_CHUNK_STATE_VALUES
        ),
        started_at=_parse_optional_float(raw.get("started_at"), context="chunk.started_at"),
        finished_at=_parse_optional_float(raw.get("finished_at"), context="chunk.finished_at"),
        retries=_parse_non_negative_int(_require_field(raw, "retries", context="chunk"), context="chunk.retries"),
        last_error_code=_parse_optional_int(raw.get("last_error_code"), context="chunk.last_error_code"),
        last_error_message=_parse_optional_str(raw.get("last_error_message"), context="chunk.last_error_message"),
        llm_model=_parse_optional_str(raw.get("llm_model"), context="chunk.llm_model"),
        input_chars=_parse_optional_non_negative_int(raw.get("input_chars"), context="chunk.input_chars"),
        output_chars=_parse_optional_non_negative_int(raw.get("output_chars"), context="chunk.output_chars"),
    )


def _job_to_dict(st: JobStatus) -> dict:
    return {"version": _JOB_STATE_VERSION, "job": asdict(st)}


def _job_from_dict(d: dict) -> JobStatus:
    payload = _require_dict(d, context="job state file")
    version = _parse_int(_require_field(payload, "version", context="job state file"), context="job state file.version")
    if version != _JOB_STATE_VERSION:
        raise ValueError(f"unsupported job state version {version}; expected {_JOB_STATE_VERSION}")

    job = _require_dict(_require_field(payload, "job", context="job state file"), context="job")
    _reject_unknown_fields(
        job,
        {
            "job_id",
            "state",
            "phase",
            "created_at",
            "started_at",
            "finished_at",
            "input_filename",
            "output_filename",
            "total_chunks",
            "done_chunks",
            "format",
            "last_error_code",
            "last_retry_count",
            "last_llm_model",
            "stats",
            "chunk_statuses",
            "chunk_counts",
            "error",
            "output_path",
            "work_dir",
            "cleanup_debug_dir",
        },
        context="job",
    )

    chunk_items = _require_field(job, "chunk_statuses", context="job")
    if not isinstance(chunk_items, list):
        raise ValueError("job.chunk_statuses must be a list")
    chunks = [_chunk_from_dict(item) for item in chunk_items]
    for expected_index, chunk in enumerate(chunks):
        if chunk.index != expected_index:
            raise ValueError(
                f"chunk index mismatch at position {expected_index}: expected {expected_index}, got {chunk.index}"
            )

    total_chunks = _parse_non_negative_int(
        _require_field(job, "total_chunks", context="job"), context="job.total_chunks"
    )
    done_chunks = _parse_non_negative_int(_require_field(job, "done_chunks", context="job"), context="job.done_chunks")
    counted_done = sum(1 for chunk in chunks if chunk.state == ChunkState.DONE)
    if total_chunks != len(chunks):
        raise ValueError("job.total_chunks does not match chunk_statuses length")
    if done_chunks != counted_done:
        raise ValueError("job.done_chunks does not match chunk_statuses")

    state = _parse_enum(_require_field(job, "state", context="job"), context="job.state", allowed=_JOB_STATES)
    phase = _parse_enum(_require_field(job, "phase", context="job"), context="job.phase", allowed=_JOB_PHASES)
    if state == JobState.DONE and phase != JobPhase.DONE:
        raise ValueError("job.phase must be 'done' when job.state is 'done'")
    if phase == JobPhase.DONE and state != JobState.DONE:
        raise ValueError("job.state must be 'done' when job.phase is 'done'")
    if phase == JobPhase.MERGE and any(chunk.state != ChunkState.DONE for chunk in chunks):
        raise ValueError("job.phase 'merge' requires every chunk to be done")
    if state == JobState.DONE and any(chunk.state != ChunkState.DONE for chunk in chunks):
        raise ValueError("job.state 'done' requires every chunk to be done")

    job_id = _parse_str(_require_field(job, "job_id", context="job"), context="job.job_id", allow_empty=False)
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("job.job_id must be a 32-character lowercase hex string")
    chunk_counts = _parse_chunk_counts(_require_field(job, "chunk_counts", context="job"), chunks)

    return JobStatus(
        job_id=job_id,
        state=state,
        phase=phase,
        created_at=_parse_float(_require_field(job, "created_at", context="job"), context="job.created_at"),
        started_at=_parse_optional_float(job.get("started_at"), context="job.started_at"),
        finished_at=_parse_optional_float(job.get("finished_at"), context="job.finished_at"),
        input_filename=_parse_str(
            _require_field(job, "input_filename", context="job"),
            context="job.input_filename",
            allow_empty=False,
        ),
        output_filename=_parse_str(
            _require_field(job, "output_filename", context="job"),
            context="job.output_filename",
            allow_empty=False,
        ),
        total_chunks=total_chunks,
        done_chunks=done_chunks,
        format=_format_config_from_dict(_require_field(job, "format", context="job")),
        last_error_code=_parse_optional_int(job.get("last_error_code"), context="job.last_error_code"),
        last_retry_count=_parse_non_negative_int(
            _require_field(job, "last_retry_count", context="job"),
            context="job.last_retry_count",
        ),
        last_llm_model=_parse_optional_str(job.get("last_llm_model"), context="job.last_llm_model"),
        stats=_parse_stats(_require_field(job, "stats", context="job")),
        chunk_statuses=chunks,
        chunk_counts=chunk_counts,
        error=_parse_optional_str(job.get("error"), context="job.error"),
        output_path=_parse_optional_str(job.get("output_path"), context="job.output_path"),
        work_dir=_parse_optional_str(job.get("work_dir"), context="job.work_dir"),
        cleanup_debug_dir=_parse_bool(
            _require_field(job, "cleanup_debug_dir", context="job"),
            context="job.cleanup_debug_dir",
        ),
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

    def _atomic_write_json(self, path: Path, payload: dict) -> None:
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
                logger.exception("failed to cleanup temp job state file: %s", tmp)

    def _persist_snapshot_unlocked(self, snapshot: JobStatus) -> None:
        path = self._persist_path_for_job_id(snapshot.job_id)
        if path is None:
            return
        try:
            self._atomic_write_json(path, _job_to_dict(snapshot))
        except Exception:
            logger.exception("failed to persist job state: job_id=%s", snapshot.job_id)

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
                    logger.exception("failed to flush dirty job state: job_id=%s", jid)

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
            st.state = JobState.PAUSED
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
        return st, changed

    def load_persisted_jobs(self) -> int:
        persist_dir = self._persist_dir
        if persist_dir is None or not persist_dir.exists():
            return 0

        loaded: list[tuple[JobStatus, bool]] = []
        for p in sorted(persist_dir.glob("*.json")):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                st, needs_rewrite = self._restore_loaded_job_after_restart(_job_from_dict(obj))
            except Exception as e:
                raise ValueError(
                    f"failed to load persisted job state {p}; delete the corrupted state file and retry"
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

    def update(self, job_id: str, **kwargs) -> None:
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
            for k, v in kwargs.items():
                if k == "started_at" and st.started_at is not None and v is not None:
                    continue
                if k == "state" and st.state == JobState.PAUSED and v in {JobState.QUEUED, JobState.RUNNING}:
                    continue
                if k == "state" and v in {JobState.DONE, JobState.ERROR, JobState.CANCELLED}:
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

    def update_chunk(self, job_id: str, index: int, **kwargs) -> None:
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
                logger.exception("failed to delete persisted job state: job_id=%s", job_id)
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
