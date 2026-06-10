from __future__ import annotations

import pytest

from novel_proofer.states import ChunkState, JobCommand, JobPhase, JobState, WaitReason
from novel_proofer.workflow import (
    ProcessingFinalState,
    ResumeTarget,
    WorkflowContext,
    WorkflowEvent,
    WorkflowInvariantError,
    WorkflowRejectionCode,
    apply_event,
    available_commands,
    can_merge,
    can_pause,
    can_resume,
    can_retry_failed,
    create_validation_state,
    decide_command,
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


@pytest.mark.parametrize(
    ("context", "command", "next_state", "target"),
    [
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.VALIDATE,
                wait_reason=WaitReason.USER_PAUSED,
            ),
            JobCommand.VALIDATE,
            (JobState.QUEUED, JobPhase.VALIDATE, None),
            ResumeTarget.VALIDATE,
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.PROCESS,
                wait_reason=WaitReason.READY_TO_PROCESS,
            ),
            JobCommand.PROCESS,
            (JobState.QUEUED, JobPhase.PROCESS, None),
            ResumeTarget.PROCESS,
        ),
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.PROCESS),
            JobCommand.PAUSE,
            (JobState.PAUSED, JobPhase.PROCESS, WaitReason.USER_PAUSED),
            None,
        ),
        (
            WorkflowContext.from_values(
                state=JobState.ERROR,
                phase=JobPhase.PROCESS,
                chunks=[ChunkState.DONE, ChunkState.ERROR],
            ),
            JobCommand.RETRY_FAILED,
            (JobState.QUEUED, JobPhase.PROCESS, None),
            None,
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.MERGE,
                wait_reason=WaitReason.READY_TO_MERGE,
                chunks=[ChunkState.DONE, ChunkState.DONE],
            ),
            JobCommand.MERGE,
            (JobState.QUEUED, JobPhase.MERGE, None),
            None,
        ),
        (
            WorkflowContext.from_values(state=JobState.DONE, phase=JobPhase.DONE, chunks=[ChunkState.DONE]),
            JobCommand.DOWNLOAD,
            (JobState.DONE, JobPhase.DONE, None),
            None,
        ),
    ],
)
def test_command_decisions_return_explicit_next_state_and_target(
    context: WorkflowContext,
    command: JobCommand,
    next_state: tuple[JobState, JobPhase, WaitReason | None],
    target: ResumeTarget | None,
) -> None:
    decision = decide_command(context, command)

    assert decision.allowed is True
    assert decision.rejection is None
    assert decision.target == target
    assert decision.next_state is not None
    assert (decision.next_state.state, decision.next_state.phase, decision.next_state.wait_reason) == next_state


@pytest.mark.parametrize(
    ("context", "command", "code", "reason"),
    [
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.VALIDATE),
            JobCommand.PAUSE,
            WorkflowRejectionCode.INVALID_PHASE,
            "cannot pause job in phase=validate",
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.MERGE,
                wait_reason=WaitReason.READY_TO_MERGE,
                chunks=[ChunkState.DONE],
            ),
            JobCommand.PROCESS,
            WorkflowRejectionCode.INVALID_PHASE,
            "job is ready to merge",
        ),
        (
            WorkflowContext.from_values(state=JobState.ERROR, phase=JobPhase.PROCESS, chunks=[ChunkState.DONE]),
            JobCommand.RETRY_FAILED,
            WorkflowRejectionCode.NO_FAILED_CHUNKS,
            "no failed chunks to retry",
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.MERGE,
                wait_reason=WaitReason.READY_TO_MERGE,
                chunks=[ChunkState.DONE, ChunkState.PENDING],
            ),
            JobCommand.MERGE,
            WorkflowRejectionCode.CHUNKS_INCOMPLETE,
            "job is not ready to merge (chunks incomplete)",
        ),
        (
            WorkflowContext.from_values(state=JobState.CANCELLED, phase=JobPhase.PROCESS),
            JobCommand.RESET,
            WorkflowRejectionCode.CANCELLED,
            "job is cancelled",
        ),
    ],
)
def test_illegal_commands_return_typed_rejections(
    context: WorkflowContext,
    command: JobCommand,
    code: WorkflowRejectionCode,
    reason: str,
) -> None:
    decision = decide_command(context, command)

    assert decision.allowed is False
    assert decision.rejection is not None
    assert decision.rejection.code == code
    assert decision.rejection.message == reason


@pytest.mark.parametrize(
    ("context", "event", "next_state"),
    [
        (
            WorkflowContext.from_values(state=JobState.QUEUED, phase=JobPhase.VALIDATE),
            WorkflowEvent.CREATE_VALIDATION,
            (JobState.QUEUED, JobPhase.VALIDATE, None),
        ),
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.VALIDATE),
            WorkflowEvent.VALIDATION_COMPLETE,
            (JobState.PAUSED, JobPhase.PROCESS, WaitReason.READY_TO_PROCESS),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.PROCESS,
                wait_reason=WaitReason.READY_TO_PROCESS,
            ),
            WorkflowEvent.START_PROCESS,
            (JobState.QUEUED, JobPhase.PROCESS, None),
        ),
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.PROCESS),
            WorkflowEvent.USER_PAUSE,
            (JobState.PAUSED, JobPhase.PROCESS, WaitReason.USER_PAUSED),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.RUNNING,
                phase=JobPhase.PROCESS,
                chunks=[ChunkState.DONE, ChunkState.DONE],
            ),
            WorkflowEvent.PROCESS_COMPLETE,
            (JobState.PAUSED, JobPhase.MERGE, WaitReason.READY_TO_MERGE),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.RUNNING,
                phase=JobPhase.PROCESS,
                chunks=[ChunkState.DONE, ChunkState.ERROR],
            ),
            WorkflowEvent.PROCESS_FAILED,
            (JobState.ERROR, JobPhase.PROCESS, None),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.ERROR,
                phase=JobPhase.PROCESS,
                chunks=[ChunkState.ERROR],
            ),
            WorkflowEvent.RETRY_FAILED_CHUNKS,
            (JobState.QUEUED, JobPhase.PROCESS, None),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.QUEUED,
                phase=JobPhase.MERGE,
                chunks=[ChunkState.DONE],
            ),
            WorkflowEvent.MERGE_STARTED,
            (JobState.RUNNING, JobPhase.MERGE, None),
        ),
        (
            WorkflowContext.from_values(
                state=JobState.RUNNING,
                phase=JobPhase.MERGE,
                chunks=[ChunkState.DONE],
            ),
            WorkflowEvent.MERGE_COMPLETE,
            (JobState.DONE, JobPhase.DONE, None),
        ),
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.PROCESS),
            WorkflowEvent.RESTART_RECOVERY,
            (JobState.PAUSED, JobPhase.PROCESS, WaitReason.SERVER_RECOVERED),
        ),
        (
            WorkflowContext.from_values(state=JobState.QUEUED, phase=JobPhase.PROCESS),
            WorkflowEvent.RESET_DELETE,
            (JobState.CANCELLED, JobPhase.PROCESS, None),
        ),
    ],
)
def test_workflow_events_are_table_driven(
    context: WorkflowContext,
    event: WorkflowEvent,
    next_state: tuple[JobState, JobPhase, WaitReason | None],
) -> None:
    transition = apply_event(context, event)

    assert transition.allowed is True
    assert transition.rejection is None
    assert transition.next_state is not None
    assert (transition.next_state.state, transition.next_state.phase, transition.next_state.wait_reason) == next_state


@pytest.mark.parametrize(
    ("context", "event", "code", "reason"),
    [
        (
            WorkflowContext.from_values(
                state=JobState.RUNNING,
                phase=JobPhase.PROCESS,
                chunks=[ChunkState.DONE, ChunkState.PENDING],
            ),
            WorkflowEvent.PROCESS_COMPLETE,
            WorkflowRejectionCode.CHUNKS_INCOMPLETE,
            "process chunks are incomplete",
        ),
        (
            WorkflowContext.from_values(
                state=JobState.PAUSED,
                phase=JobPhase.PROCESS,
                wait_reason=WaitReason.USER_PAUSED,
            ),
            WorkflowEvent.RESTART_RECOVERY,
            WorkflowRejectionCode.INVALID_STATE,
            "job is not in-flight (state=paused)",
        ),
        (
            WorkflowContext.from_values(state=JobState.RUNNING, phase=JobPhase.PROCESS),
            WorkflowEvent.MERGE_STARTED,
            WorkflowRejectionCode.INVALID_PHASE,
            "merge cannot start in phase=process",
        ),
    ],
)
def test_illegal_events_return_typed_rejections(
    context: WorkflowContext,
    event: WorkflowEvent,
    code: WorkflowRejectionCode,
    reason: str,
) -> None:
    transition = apply_event(context, event)

    assert transition.allowed is False
    assert transition.rejection is not None
    assert transition.rejection.code == code
    assert transition.rejection.message == reason


@pytest.mark.parametrize("phase", list(JobPhase))
def test_pause_is_legal_only_for_in_flight_process_phase(phase: JobPhase) -> None:
    if phase == JobPhase.DONE:
        with pytest.raises(WorkflowInvariantError, match=r"job\.state must be 'done'"):
            WorkflowContext.from_values(state=JobState.RUNNING, phase=phase)
        return

    decision = decide_command(WorkflowContext.from_values(state=JobState.RUNNING, phase=phase), JobCommand.PAUSE)

    if phase == JobPhase.PROCESS:
        assert decision.allowed is True
    else:
        assert decision.allowed is False
        assert decision.rejection is not None
        assert decision.rejection.code == WorkflowRejectionCode.INVALID_PHASE


@pytest.mark.parametrize(
    "wait_reason",
    [WaitReason.READY_TO_PROCESS, WaitReason.USER_PAUSED, WaitReason.SERVER_RECOVERED],
)
def test_process_resume_allows_process_wait_reasons(wait_reason: WaitReason) -> None:
    context = WorkflowContext.from_values(state=JobState.PAUSED, phase=JobPhase.PROCESS, wait_reason=wait_reason)

    decision = decide_command(context, JobCommand.PROCESS)

    assert decision.allowed is True
    assert decision.target == ResumeTarget.PROCESS


def test_wait_reasons_are_phase_specific() -> None:
    with pytest.raises(WorkflowInvariantError, match="wait_reason=ready_to_merge is invalid for phase=process"):
        WorkflowContext.from_values(
            state=JobState.PAUSED,
            phase=JobPhase.PROCESS,
            wait_reason=WaitReason.READY_TO_MERGE,
        )


def test_available_commands_are_derived_from_workflow_decisions() -> None:
    context = WorkflowContext.from_values(
        state=JobState.PAUSED,
        phase=JobPhase.MERGE,
        wait_reason=WaitReason.READY_TO_MERGE,
        chunks=[ChunkState.DONE, ChunkState.DONE],
    )

    assert available_commands(context) == [JobCommand.MERGE, JobCommand.DETACH, JobCommand.RESET]


def test_create_validation_state_is_the_only_new_job_workflow_entry() -> None:
    state = create_validation_state()

    assert state.state == JobState.QUEUED
    assert state.phase == JobPhase.VALIDATE
    assert state.wait_reason is None
