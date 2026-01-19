from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

from novel_proofer.formatting.config import FormatConfig

logger = logging.getLogger(__name__)

_JOB_STATE_VERSION = 2
_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

_JOB_PHASES = {"validate", "process", "merge", "done"}


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


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
            "phase": str(st.phase),
            "created_at": float(st.created_at),
            "started_at": st.started_at,
            "finished_at": st.finished_at,
            "input_filename": str(st.input_filename),
            "output_filename": str(st.output_filename),
            "output_path": st.output_path,
            "total_chunks": int(st.total_chunks),
            "done_chunks": int(st.done_chunks),
            "format": {
                "max_chunk_chars": int(st.format.max_chunk_chars),
                "paragraph_indent": bool(st.format.paragraph_indent),
                "indent_with_fullwidth_space": bool(st.format.indent_with_fullwidth_space),
                "normalize_blank_lines": bool(st.format.normalize_blank_lines),
                "trim_trailing_spaces": bool(st.format.trim_trailing_spaces),
                "normalize_ellipsis": bool(st.format.normalize_ellipsis),
                "normalize_em_dash": bool(st.format.normalize_em_dash),
                "normalize_cjk_punctuation": bool(st.format.normalize_cjk_punctuation),
                "fix_cjk_punct_spacing": bool(st.format.fix_cjk_punct_spacing),
                "normalize_quotes": bool(st.format.normalize_quotes),
            },
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
    if version is not None and version not in {1, _JOB_STATE_VERSION}:
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

    phase = job.get("phase")
    phase = str(phase).strip().lower() if phase is not None else ""
    if phase not in _JOB_PHASES:
        phase = ""

    fmt_obj: FormatConfig
    fmt_raw = job.get("format")
    if isinstance(fmt_raw, dict):
        try:
            fmt_obj = FormatConfig(
                max_chunk_chars=int(fmt_raw.get("max_chunk_chars", 2000) or 2000),
                paragraph_indent=bool(fmt_raw.get("paragraph_indent", True)),
                indent_with_fullwidth_space=bool(fmt_raw.get("indent_with_fullwidth_space", True)),
                normalize_blank_lines=bool(fmt_raw.get("normalize_blank_lines", True)),
                trim_trailing_spaces=bool(fmt_raw.get("trim_trailing_spaces", True)),
                normalize_ellipsis=bool(fmt_raw.get("normalize_ellipsis", True)),
                normalize_em_dash=bool(fmt_raw.get("normalize_em_dash", True)),
                normalize_cjk_punctuation=bool(fmt_raw.get("normalize_cjk_punctuation", True)),
                fix_cjk_punct_spacing=bool(fmt_raw.get("fix_cjk_punct_spacing", True)),
                normalize_quotes=bool(fmt_raw.get("normalize_quotes", False)),
            )
        except Exception:
            fmt_obj = FormatConfig()
    else:
        fmt_obj = FormatConfig()

    return JobStatus(
        job_id=str(job.get("job_id", "")),
        state=str(job.get("state", "queued")),
        phase=phase or "validate",
        created_at=float(job.get("created_at", time.time())),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        input_filename=str(job.get("input_filename", "")),
        output_filename=str(job.get("output_filename", "")),
        total_chunks=int(job.get("total_chunks", len(chunks)) or 0),
        done_chunks=int(job.get("done_chunks", 0) or 0),
        format=fmt_obj,
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
    def __init__(self, *, persist_interval_s: float | None = None) -> None:
        self._lock = threading.Lock()
        self._persist_cv = threading.Condition(self._lock)
        self._persist_lock = threading.Lock()
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()
        self._persist_dir: Path | None = None
        interval = (
            _env_float("NOVEL_PROOFER_JOB_PERSIST_INTERVAL_S", 5.0)
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
            with suppress(Exception):
                t.join(timeout=1.0)

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
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
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
            with suppress(Exception):
                tmp.unlink(missing_ok=True)

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
                with suppress(Exception):
                    self._flush_job(jid, require_dirty=True)

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

        # Phase migration / normalization (v1 files don't have phase).
        if st.phase not in _JOB_PHASES:
            st.phase = "validate"

        # Derive phase when missing or clearly stale.
        if not st.chunk_statuses:
            # Not validated yet (or legacy file missing chunk info).
            if st.state == "done":
                st.phase = "done"
            else:
                st.phase = "validate"
        else:
            if st.state == "done":
                st.phase = "done"
            else:
                # If everything is done but final output might not exist, allow explicit merge.
                all_done = all(c.state == "done" for c in st.chunk_statuses)
                if all_done:
                    st.phase = "merge"
                else:
                    st.phase = "process"

        # Ensure phase/state compatibility.
        if st.phase == "done" and st.state != "done":
            st.phase = "merge" if st.chunk_statuses and all(c.state == "done" for c in st.chunk_statuses) else "process"
        if st.state == "done":
            st.phase = "done"
        return st

    def load_persisted_jobs(self) -> int:
        persist_dir = self._persist_dir
        if persist_dir is None or not persist_dir.exists():
            return 0

        loaded: list[tuple[JobStatus, bool]] = []
        for p in persist_dir.glob("*.json"):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                src_version = obj.get("version")
                needs_rewrite = True
                try:
                    needs_rewrite = int(src_version) != _JOB_STATE_VERSION
                except Exception:
                    needs_rewrite = True
                st = self._heal_loaded_job(_job_from_dict(obj))
                if not st.job_id:
                    continue
                loaded.append((st, needs_rewrite))
            except Exception:
                logger.exception("failed to load persisted job: %s", p)

        with self._lock:
            for st, _needs_rewrite in loaded:
                self._jobs[st.job_id] = st
                if st.state == "cancelled":
                    self._cancelled.add(st.job_id)
                if st.state == "paused":
                    self._paused.add(st.job_id)

        # Rewrite snapshots in latest schema (best-effort) to complete migration.
        for st, needs_rewrite in loaded:
            if needs_rewrite:
                self._persist_snapshot_direct(self._snapshot_job(st))

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
            phase="validate",
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
        self._persist_snapshot_direct(snap)
        return snap

    def get(self, job_id: str) -> JobStatus | None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return None
            return self._snapshot_job(st)

    def list(self) -> list[JobStatus]:
        with self._lock:
            items = list(self._jobs.values())
        items.sort(key=lambda s: s.created_at, reverse=True)
        return [self._snapshot_job(s) for s in items]

    def update(self, job_id: str, **kwargs) -> None:
        flush_now = False
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
            self._mark_dirty_locked(job_id)
            flush_now = st.state in {"done", "error", "cancelled"}
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
                ChunkStatus(index=i, state="pending", llm_model=llm_model) for i in range(total_chunks)
            ]
            self._mark_dirty_locked(job_id)
        self._flush_job(job_id, require_dirty=False)

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
            should_persist = False
            for k, v in kwargs.items():
                setattr(cs, k, v)
            if "state" in kwargs and cs.state != prev_state:
                should_persist = True
                if prev_state == "done" and st.done_chunks > 0:
                    st.done_chunks -= 1
                if cs.state == "done":
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
                cs.retries += inc
                cs.last_error_code = last_error_code
                cs.last_error_message = last_error_message
            self._mark_dirty_locked(job_id)

    def add_stat(self, job_id: str, key: str, inc: int = 1) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if st is None:
                return
            st.stats[key] = st.stats.get(key, 0) + inc
        # Stats are best-effort diagnostics; avoid persisting on every increment for performance.

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
            self._mark_dirty_locked(job_id)
        self._flush_job(job_id, require_dirty=False)
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
            self._mark_dirty_locked(job_id)
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
            self._mark_dirty_locked(job_id)
        return True

    def delete(self, job_id: str) -> bool:
        path: Path | None = None
        existed: bool
        with self._persist_lock:
            with self._lock:
                existed = job_id in self._jobs
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

    def is_paused(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._paused


GLOBAL_JOBS = JobStore()
