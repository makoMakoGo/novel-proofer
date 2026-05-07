from __future__ import annotations

import pytest

from novel_proofer.states import ChunkState, JobPhase, JobState
from novel_proofer.workflow import (
    ProcessingFinalState,
    ResumeTarget,
    WorkflowInvariantError,
    can_merge,
    can_pause,
    can_resume,
    can_retry_failed,
    processing_final_state,
    validate_job_phase_invariants,
)


def test_pause_guard_is_process_only_and_in_flight_only() -> None:
    assert can_pause(JobState.RUNNING, JobPhase.PROCESS).allowed is True
    assert can_pause(JobState.QUEUED, JobPhase.PROCESS).allowed is True

    validate_guard = can_pause(JobState.RUNNING, JobPhase.VALIDATE)
    assert validate_guard.allowed is False
    assert validate_guard.reason == "cannot pause job in phase=validate"

    paused_guard = can_pause(JobState.PAUSED, JobPhase.PROCESS)
    assert paused_guard.allowed is False
    assert paused_guard.reason == "cannot pause job in state=paused"


def test_resume_guard_selects_validate_or_process_target() -> None:
    validate_guard = can_resume(JobState.PAUSED, JobPhase.VALIDATE)
    assert validate_guard.allowed is True
    assert validate_guard.target == ResumeTarget.VALIDATE

    process_guard = can_resume(JobState.PAUSED, JobPhase.PROCESS)
    assert process_guard.allowed is True
    assert process_guard.target == ResumeTarget.PROCESS

    merge_guard = can_resume(JobState.PAUSED, JobPhase.MERGE)
    assert merge_guard.allowed is False
    assert merge_guard.reason == "job is ready to merge"

    running_guard = can_resume(JobState.RUNNING, JobPhase.PROCESS)
    assert running_guard.allowed is False
    assert running_guard.reason == "job is running"


def test_retry_guard_requires_error_state_with_failed_chunks() -> None:
    ok = can_retry_failed(JobState.ERROR, [ChunkState.DONE, ChunkState.ERROR])
    assert ok.allowed is True

    no_failed = can_retry_failed(JobState.ERROR, [ChunkState.DONE])
    assert no_failed.allowed is False
    assert no_failed.reason == "no failed chunks to retry"

    paused = can_retry_failed(JobState.PAUSED, [ChunkState.ERROR])
    assert paused.allowed is False
    assert paused.reason == "job is not in error state (state=paused)"


def test_processing_final_state_depends_only_on_chunk_states() -> None:
    failed = processing_final_state([ChunkState.DONE, ChunkState.ERROR])
    assert failed == ProcessingFinalState.ERROR

    ready = processing_final_state([ChunkState.DONE, ChunkState.DONE])
    assert ready == ProcessingFinalState.READY_TO_MERGE


def test_merge_guard_requires_paused_merge_phase_and_complete_chunks() -> None:
    ok = can_merge(JobState.PAUSED, JobPhase.MERGE, [ChunkState.DONE, ChunkState.DONE])
    assert ok.allowed is True

    running = can_merge(JobState.RUNNING, JobPhase.MERGE, [ChunkState.DONE])
    assert running.allowed is False
    assert running.reason == "job is running"

    wrong_phase = can_merge(JobState.PAUSED, JobPhase.PROCESS, [ChunkState.DONE])
    assert wrong_phase.allowed is False
    assert wrong_phase.reason == "job is not ready to merge (phase=process)"

    incomplete = can_merge(JobState.PAUSED, JobPhase.MERGE, [ChunkState.DONE, ChunkState.PENDING])
    assert incomplete.allowed is False
    assert incomplete.reason == "job is not ready to merge (chunks incomplete)"


def test_persisted_phase_invariants_are_centralized() -> None:
    validate_job_phase_invariants(JobState.DONE, JobPhase.DONE, [ChunkState.DONE])
    validate_job_phase_invariants(JobState.PAUSED, JobPhase.MERGE, [ChunkState.DONE])

    with pytest.raises(WorkflowInvariantError, match=r"job\.phase must be 'done'"):
        validate_job_phase_invariants(JobState.DONE, JobPhase.MERGE, [ChunkState.DONE])

    with pytest.raises(WorkflowInvariantError, match="requires every chunk to be done"):
        validate_job_phase_invariants(JobState.PAUSED, JobPhase.MERGE, [ChunkState.DONE, ChunkState.ERROR])
