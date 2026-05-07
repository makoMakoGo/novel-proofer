from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from novel_proofer.states import ChunkState, JobPhase, JobState


class WorkflowInvariantError(ValueError):
    pass


class ResumeTarget(StrEnum):
    VALIDATE = "validate"
    PROCESS = "process"


class ProcessingFinalState(StrEnum):
    ERROR = "error"
    READY_TO_MERGE = "ready_to_merge"


@dataclass(frozen=True)
class WorkflowGuard:
    allowed: bool
    reason: str | None = None

    @classmethod
    def allow(cls) -> WorkflowGuard:
        return cls(True)

    @classmethod
    def reject(cls, reason: str) -> WorkflowGuard:
        return cls(False, reason)


@dataclass(frozen=True)
class ResumeGuard(WorkflowGuard):
    target: ResumeTarget | None = None

    @classmethod
    def allow_target(cls, target: ResumeTarget) -> ResumeGuard:
        return cls(True, target=target)

    @classmethod
    def reject(cls, reason: str) -> ResumeGuard:
        return cls(False, reason=reason)


def _state_value(state: JobState | str) -> str:
    return JobState(state).value


def _phase_value(phase: JobPhase | str) -> str:
    return JobPhase(phase).value


def _chunk_states(chunks: Iterable[ChunkState | str]) -> list[ChunkState]:
    return [ChunkState(chunk) for chunk in chunks]


def is_in_flight_job_state(state: JobState | str) -> bool:
    return JobState(state) in {JobState.QUEUED, JobState.RUNNING}


def can_pause(state: JobState | str, phase: JobPhase | str) -> WorkflowGuard:
    state_value = _state_value(state)
    phase_value = _phase_value(phase)
    if phase_value != JobPhase.PROCESS:
        return WorkflowGuard.reject(f"cannot pause job in phase={phase_value}")
    if not is_in_flight_job_state(state_value):
        return WorkflowGuard.reject(f"cannot pause job in state={state_value}")
    return WorkflowGuard.allow()


def can_resume(state: JobState | str, phase: JobPhase | str) -> ResumeGuard:
    state_value = _state_value(state)
    phase_value = _phase_value(phase)
    if state_value == JobState.RUNNING:
        return ResumeGuard.reject("job is running")
    if state_value == JobState.CANCELLED:
        return ResumeGuard.reject("job is cancelled")
    if state_value != JobState.PAUSED:
        return ResumeGuard.reject("job is not paused")
    if phase_value == JobPhase.MERGE:
        return ResumeGuard.reject("job is ready to merge")
    if phase_value == JobPhase.DONE:
        return ResumeGuard.reject("job is already done")
    if phase_value == JobPhase.VALIDATE:
        return ResumeGuard.allow_target(ResumeTarget.VALIDATE)
    if phase_value == JobPhase.PROCESS:
        return ResumeGuard.allow_target(ResumeTarget.PROCESS)
    return ResumeGuard.reject(f"cannot resume job in phase={phase_value}")


def can_retry_failed(state: JobState | str, chunks: Iterable[ChunkState | str]) -> WorkflowGuard:
    state_value = _state_value(state)
    if state_value == JobState.CANCELLED:
        return WorkflowGuard.reject("job is cancelled")
    if state_value == JobState.RUNNING:
        return WorkflowGuard.reject("job is running")
    if state_value != JobState.ERROR:
        return WorkflowGuard.reject(f"job is not in error state (state={state_value})")
    if not any(chunk == ChunkState.ERROR for chunk in _chunk_states(chunks)):
        return WorkflowGuard.reject("no failed chunks to retry")
    return WorkflowGuard.allow()


def can_merge(state: JobState | str, phase: JobPhase | str, chunks: Iterable[ChunkState | str]) -> WorkflowGuard:
    state_value = _state_value(state)
    phase_value = _phase_value(phase)
    chunk_states = _chunk_states(chunks)
    if state_value == JobState.CANCELLED:
        return WorkflowGuard.reject("job is cancelled")
    if state_value == JobState.RUNNING:
        return WorkflowGuard.reject("job is running")
    if state_value != JobState.PAUSED:
        return WorkflowGuard.reject(f"job is not paused (state={state_value})")
    if phase_value != JobPhase.MERGE:
        return WorkflowGuard.reject(f"job is not ready to merge (phase={phase_value})")
    if not chunk_states or any(chunk != ChunkState.DONE for chunk in chunk_states):
        return WorkflowGuard.reject("job is not ready to merge (chunks incomplete)")
    return WorkflowGuard.allow()


def processing_final_state(chunks: Iterable[ChunkState | str]) -> ProcessingFinalState:
    chunk_states = _chunk_states(chunks)
    if any(chunk == ChunkState.ERROR for chunk in chunk_states):
        return ProcessingFinalState.ERROR
    return ProcessingFinalState.READY_TO_MERGE


def validate_job_phase_invariants(
    state: JobState | str,
    phase: JobPhase | str,
    chunks: Iterable[ChunkState | str],
) -> None:
    state_value = JobState(state)
    phase_value = JobPhase(phase)
    chunk_states = _chunk_states(chunks)
    if state_value == JobState.DONE and phase_value != JobPhase.DONE:
        raise WorkflowInvariantError("job.phase must be 'done' when job.state is 'done'")
    if phase_value == JobPhase.DONE and state_value != JobState.DONE:
        raise WorkflowInvariantError("job.state must be 'done' when job.phase is 'done'")
    if phase_value == JobPhase.MERGE and any(chunk != ChunkState.DONE for chunk in chunk_states):
        raise WorkflowInvariantError("job.phase 'merge' requires every chunk to be done")
    if state_value == JobState.DONE and any(chunk != ChunkState.DONE for chunk in chunk_states):
        raise WorkflowInvariantError("job.state 'done' requires every chunk to be done")
