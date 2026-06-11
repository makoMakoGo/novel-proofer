from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from novel_proofer.states import ExecutionState, JobCommand


class StopReason(StrEnum):
    PAUSE = "pause"
    DELETE = "delete"


class ExecutionAlreadyActive(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionSnapshot:
    job_id: str
    attempt_id: str
    command: str
    state: str
    stop_requested: str | None


@dataclass
class _ExecutionEntry:
    job_id: str
    attempt_id: str
    command: str
    state: str
    stop_requested: str | None = None
    done_callbacks: list[Callable[[], Any]] = field(default_factory=list)


class ExecutionRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_job: dict[str, _ExecutionEntry] = {}
        self._by_attempt: dict[str, _ExecutionEntry] = {}

    def begin(self, job_id: str, command: JobCommand | str) -> ExecutionSnapshot:
        jid = _normalize_job_id(job_id)
        cmd = JobCommand(command).value
        attempt_id = uuid.uuid4().hex
        entry = _ExecutionEntry(
            job_id=jid,
            attempt_id=attempt_id,
            command=cmd,
            state=ExecutionState.QUEUED.value,
        )
        with self._lock:
            current = self._by_job.get(jid)
            if current is not None:
                raise ExecutionAlreadyActive(
                    f"job_id '{jid}' already has active execution {current.attempt_id} ({current.command})"
                )
            self._by_job[jid] = entry
            self._by_attempt[attempt_id] = entry
            return _snapshot(entry)

    def mark_running(self, attempt_id: str) -> None:
        aid = _normalize_attempt_id(attempt_id)
        with self._lock:
            entry = self._by_attempt.get(aid)
            if entry is None:
                return
            entry.state = ExecutionState.RUNNING.value

    def request_stop(self, job_id: str, reason: StopReason | str) -> bool:
        jid = _normalize_job_id(job_id)
        stop_reason = StopReason(reason).value
        with self._lock:
            entry = self._by_job.get(jid)
            if entry is None:
                return False
            if entry.stop_requested == StopReason.DELETE.value:
                return True
            entry.stop_requested = stop_reason
            return True

    def stop_requested(self, job_id: str) -> bool:
        return self.stop_reason(job_id) is not None

    def stop_reason(self, job_id: str) -> str | None:
        jid = _normalize_job_id(job_id)
        with self._lock:
            entry = self._by_job.get(jid)
            return None if entry is None else entry.stop_requested

    def get(self, job_id: str) -> ExecutionSnapshot | None:
        jid = _normalize_job_id(job_id)
        with self._lock:
            entry = self._by_job.get(jid)
            return None if entry is None else _snapshot(entry)

    def add_done_callback(self, job_id: str, cb: Callable[[], Any]) -> bool:
        jid = _normalize_job_id(job_id)
        with self._lock:
            entry = self._by_job.get(jid)
            if entry is None:
                return False
            entry.done_callbacks.append(cb)
            return True

    def finish(self, attempt_id: str) -> list[Callable[[], Any]]:
        aid = _normalize_attempt_id(attempt_id)
        with self._lock:
            entry = self._by_attempt.pop(aid, None)
            if entry is None:
                return []
            current = self._by_job.get(entry.job_id)
            if current is entry:
                self._by_job.pop(entry.job_id, None)
            callbacks = list(entry.done_callbacks)
            entry.done_callbacks.clear()
            return callbacks

    def clear(self) -> None:
        with self._lock:
            self._by_job.clear()
            self._by_attempt.clear()


def _normalize_job_id(job_id: str) -> str:
    jid = str(job_id or "").strip()
    if not jid:
        raise ValueError("job_id is required")
    return jid


def _normalize_attempt_id(attempt_id: str) -> str:
    aid = str(attempt_id or "").strip()
    if not aid:
        raise ValueError("attempt_id is required")
    return aid


def _snapshot(entry: _ExecutionEntry) -> ExecutionSnapshot:
    return ExecutionSnapshot(
        job_id=entry.job_id,
        attempt_id=entry.attempt_id,
        command=entry.command,
        state=entry.state,
        stop_requested=entry.stop_requested,
    )


GLOBAL_EXECUTIONS = ExecutionRegistry()
