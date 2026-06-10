from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from novel_proofer.states import ChunkState, JobCommand, JobPhase, JobState, WaitReason


class WorkflowInvariantError(ValueError):
    pass


class WorkflowTransitionError(ValueError):
    def __init__(self, message: str, *, code: WorkflowRejectionCode | None = None) -> None:
        super().__init__(message)
        self.code = code


class ResumeTarget(StrEnum):
    VALIDATE = "validate"
    PROCESS = "process"


class ProcessingFinalState(StrEnum):
    ERROR = "error"
    READY_TO_MERGE = "ready_to_merge"


class WorkflowEvent(StrEnum):
    CREATE_VALIDATION = "create_validation"
    VALIDATION_COMPLETE = "validation_complete"
    START_PROCESS = "start_process"
    USER_PAUSE = "user_pause"
    EXECUTION_STOPPED = "execution_stopped"
    PROCESS_COMPLETE = "process_complete"
    PROCESS_FAILED = "process_failed"
    RETRY_FAILED_CHUNKS = "retry_failed_chunks"
    MERGE_STARTED = "merge_started"
    MERGE_COMPLETE = "merge_complete"
    RESET_DELETE = "reset_delete"
    RESTART_RECOVERY = "restart_recovery"


class WorkflowRejectionCode(StrEnum):
    CANCELLED = "cancelled"
    CHUNKS_INCOMPLETE = "chunks_incomplete"
    DONE = "done"
    FAILED_CHUNKS = "failed_chunks"
    INVALID_PHASE = "invalid_phase"
    INVALID_STATE = "invalid_state"
    INVALID_WAIT_REASON = "invalid_wait_reason"
    JOB_RUNNING = "job_running"
    NO_FAILED_CHUNKS = "no_failed_chunks"
    NOT_PAUSED = "not_paused"


_WAIT_REASONS_BY_PHASE = {
    JobPhase.VALIDATE: {WaitReason.USER_PAUSED, WaitReason.SERVER_RECOVERED},
    JobPhase.PROCESS: {WaitReason.READY_TO_PROCESS, WaitReason.USER_PAUSED, WaitReason.SERVER_RECOVERED},
    JobPhase.MERGE: {WaitReason.READY_TO_MERGE, WaitReason.SERVER_RECOVERED},
}


@dataclass(frozen=True)
class WorkflowRejection:
    code: WorkflowRejectionCode
    message: str


@dataclass(frozen=True)
class WorkflowState:
    state: JobState
    phase: JobPhase
    wait_reason: WaitReason | None = None

    @classmethod
    def from_values(
        cls,
        state: JobState | str,
        phase: JobPhase | str,
        wait_reason: WaitReason | str | None = None,
    ) -> WorkflowState:
        reason = None if wait_reason is None else WaitReason(wait_reason)
        out = cls(JobState(state), JobPhase(phase), reason)
        out.validate()
        return out

    def validate(self) -> None:
        if self.state == JobState.PAUSED and self.wait_reason is None:
            raise WorkflowInvariantError("paused workflow state requires wait_reason")
        if self.state != JobState.PAUSED and self.wait_reason is not None:
            raise WorkflowInvariantError("workflow wait_reason must be None unless state is paused")
        if self.state == JobState.PAUSED and self.wait_reason not in _WAIT_REASONS_BY_PHASE.get(self.phase, set()):
            raise WorkflowInvariantError(
                f"wait_reason={self.wait_reason.value if self.wait_reason else None} is invalid for phase={self.phase.value}"
            )
        if self.state == JobState.DONE and self.phase != JobPhase.DONE:
            raise WorkflowInvariantError("job.phase must be 'done' when job.state is 'done'")
        if self.phase == JobPhase.DONE and self.state != JobState.DONE:
            raise WorkflowInvariantError("job.state must be 'done' when job.phase is 'done'")


@dataclass(frozen=True)
class ChunkSummary:
    total: int
    pending: int = 0
    processing: int = 0
    retrying: int = 0
    done: int = 0
    error: int = 0

    @classmethod
    def from_states(cls, chunks: Iterable[ChunkState | str]) -> ChunkSummary:
        counts = {state: 0 for state in ChunkState}
        for chunk in chunks:
            counts[ChunkState(chunk)] += 1
        return cls(
            total=sum(counts.values()),
            pending=counts[ChunkState.PENDING],
            processing=counts[ChunkState.PROCESSING],
            retrying=counts[ChunkState.RETRYING],
            done=counts[ChunkState.DONE],
            error=counts[ChunkState.ERROR],
        )

    @classmethod
    def from_counts(cls, *, total: int, counts: dict[str, int]) -> ChunkSummary:
        return cls(
            total=int(total),
            pending=int(counts.get(ChunkState.PENDING.value, 0) or counts.get(ChunkState.PENDING, 0) or 0),
            processing=int(counts.get(ChunkState.PROCESSING.value, 0) or counts.get(ChunkState.PROCESSING, 0) or 0),
            retrying=int(counts.get(ChunkState.RETRYING.value, 0) or counts.get(ChunkState.RETRYING, 0) or 0),
            done=int(counts.get(ChunkState.DONE.value, 0) or counts.get(ChunkState.DONE, 0) or 0),
            error=int(counts.get(ChunkState.ERROR.value, 0) or counts.get(ChunkState.ERROR, 0) or 0),
        )

    @property
    def all_done(self) -> bool:
        return self.total > 0 and self.done == self.total

    @property
    def has_failed(self) -> bool:
        return self.error > 0

    @property
    def has_in_flight_chunks(self) -> bool:
        return self.processing > 0 or self.retrying > 0

    def validate_for_phase(self, phase: JobPhase) -> None:
        if phase == JobPhase.MERGE and not self.all_done:
            raise WorkflowInvariantError("job.phase 'merge' requires every chunk to be done")
        if phase == JobPhase.DONE and not self.all_done:
            raise WorkflowInvariantError("job.state 'done' requires every chunk to be done")


@dataclass(frozen=True)
class WorkflowContext:
    workflow: WorkflowState
    chunks: ChunkSummary

    @classmethod
    def from_values(
        cls,
        *,
        state: JobState | str,
        phase: JobPhase | str,
        wait_reason: WaitReason | str | None = None,
        chunks: Iterable[ChunkState | str] = (),
    ) -> WorkflowContext:
        out = cls(WorkflowState.from_values(state, phase, wait_reason), ChunkSummary.from_states(chunks))
        out.validate()
        return out

    @classmethod
    def from_counts(
        cls,
        *,
        state: JobState | str,
        phase: JobPhase | str,
        wait_reason: WaitReason | str | None = None,
        total_chunks: int,
        chunk_counts: dict[str, int],
    ) -> WorkflowContext:
        out = cls(
            WorkflowState.from_values(state, phase, wait_reason),
            ChunkSummary.from_counts(total=int(total_chunks), counts=chunk_counts),
        )
        out.validate()
        return out

    def validate(self) -> None:
        self.workflow.validate()


@dataclass(frozen=True)
class WorkflowGuard:
    allowed: bool
    reason: str | None = None
    rejection: WorkflowRejection | None = None

    @classmethod
    def allow(cls) -> WorkflowGuard:
        return cls(True)

    @classmethod
    def reject(cls, reason: str, *, code: WorkflowRejectionCode = WorkflowRejectionCode.INVALID_STATE) -> WorkflowGuard:
        return cls(False, reason, WorkflowRejection(code, reason))


@dataclass(frozen=True)
class ResumeGuard(WorkflowGuard):
    target: ResumeTarget | None = None

    @classmethod
    def allow_target(cls, target: ResumeTarget) -> ResumeGuard:
        return cls(True, target=target)

    @classmethod
    def reject(cls, reason: str, *, code: WorkflowRejectionCode = WorkflowRejectionCode.INVALID_STATE) -> ResumeGuard:
        return cls(False, reason=reason, rejection=WorkflowRejection(code, reason))


@dataclass(frozen=True)
class CommandDecision:
    command: JobCommand
    allowed: bool
    next_state: WorkflowState | None = None
    target: ResumeTarget | None = None
    rejection: WorkflowRejection | None = None

    @property
    def reason(self) -> str | None:
        return None if self.rejection is None else self.rejection.message

    @classmethod
    def allow(
        cls,
        command: JobCommand,
        *,
        next_state: WorkflowState | None = None,
        target: ResumeTarget | None = None,
    ) -> CommandDecision:
        return cls(command=command, allowed=True, next_state=next_state, target=target)

    @classmethod
    def reject(cls, command: JobCommand, code: WorkflowRejectionCode, message: str) -> CommandDecision:
        return cls(command=command, allowed=False, rejection=WorkflowRejection(code, message))


@dataclass(frozen=True)
class EventTransition:
    event: WorkflowEvent
    allowed: bool
    next_state: WorkflowState | None = None
    rejection: WorkflowRejection | None = None

    @property
    def reason(self) -> str | None:
        return None if self.rejection is None else self.rejection.message

    @classmethod
    def allow(cls, event: WorkflowEvent, next_state: WorkflowState) -> EventTransition:
        return cls(event=event, allowed=True, next_state=next_state)

    @classmethod
    def reject(cls, event: WorkflowEvent, code: WorkflowRejectionCode, message: str) -> EventTransition:
        return cls(event=event, allowed=False, rejection=WorkflowRejection(code, message))


def _in_flight(state: JobState) -> bool:
    return state in {JobState.QUEUED, JobState.RUNNING}


def is_in_flight_job_state(state: JobState | str) -> bool:
    return _in_flight(JobState(state))


def create_validation_state() -> WorkflowState:
    return WorkflowState(JobState.QUEUED, JobPhase.VALIDATE)


def _default_wait_reason_for_guard(state: JobState | str, phase: JobPhase | str) -> WaitReason | None:
    if JobState(state) != JobState.PAUSED:
        return None
    phase_value = JobPhase(phase)
    if phase_value == JobPhase.PROCESS:
        return WaitReason.USER_PAUSED
    if phase_value == JobPhase.MERGE:
        return WaitReason.READY_TO_MERGE
    if phase_value == JobPhase.VALIDATE:
        return WaitReason.USER_PAUSED
    return None


def _reject(command: JobCommand, code: WorkflowRejectionCode, message: str) -> CommandDecision:
    return CommandDecision.reject(command, code, message)


def decide_command(context: WorkflowContext, command: JobCommand | str) -> CommandDecision:
    cmd = JobCommand(command)
    state = context.workflow.state
    phase = context.workflow.phase

    if cmd == JobCommand.VALIDATE:
        if state == JobState.CANCELLED:
            return _reject(cmd, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state == JobState.RUNNING:
            return _reject(cmd, WorkflowRejectionCode.JOB_RUNNING, "job is running")
        if state != JobState.PAUSED:
            return _reject(cmd, WorkflowRejectionCode.NOT_PAUSED, "job is not paused")
        if phase != JobPhase.VALIDATE:
            return _reject(cmd, WorkflowRejectionCode.INVALID_PHASE, f"cannot validate job in phase={phase.value}")
        return CommandDecision.allow(
            cmd,
            next_state=WorkflowState(JobState.QUEUED, JobPhase.VALIDATE),
            target=ResumeTarget.VALIDATE,
        )

    if cmd == JobCommand.PROCESS:
        if state == JobState.CANCELLED:
            return _reject(cmd, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state == JobState.RUNNING:
            return _reject(cmd, WorkflowRejectionCode.JOB_RUNNING, "job is running")
        if state != JobState.PAUSED:
            return _reject(cmd, WorkflowRejectionCode.NOT_PAUSED, "job is not paused")
        if phase == JobPhase.MERGE:
            return _reject(cmd, WorkflowRejectionCode.INVALID_PHASE, "job is ready to merge")
        if phase == JobPhase.DONE:
            return _reject(cmd, WorkflowRejectionCode.DONE, "job is already done")
        if phase != JobPhase.PROCESS:
            return _reject(cmd, WorkflowRejectionCode.INVALID_PHASE, f"cannot process job in phase={phase.value}")
        return CommandDecision.allow(
            cmd,
            next_state=WorkflowState(JobState.QUEUED, JobPhase.PROCESS),
            target=ResumeTarget.PROCESS,
        )

    if cmd == JobCommand.PAUSE:
        if phase != JobPhase.PROCESS:
            return _reject(cmd, WorkflowRejectionCode.INVALID_PHASE, f"cannot pause job in phase={phase.value}")
        if not _in_flight(state):
            return _reject(cmd, WorkflowRejectionCode.INVALID_STATE, f"cannot pause job in state={state.value}")
        return CommandDecision.allow(
            cmd,
            next_state=WorkflowState(JobState.PAUSED, JobPhase.PROCESS, WaitReason.USER_PAUSED),
        )

    if cmd == JobCommand.RETRY_FAILED:
        if state == JobState.CANCELLED:
            return _reject(cmd, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state == JobState.RUNNING:
            return _reject(cmd, WorkflowRejectionCode.JOB_RUNNING, "job is running")
        if state != JobState.ERROR:
            return _reject(cmd, WorkflowRejectionCode.INVALID_STATE, f"job is not in error state (state={state.value})")
        if not context.chunks.has_failed:
            return _reject(cmd, WorkflowRejectionCode.NO_FAILED_CHUNKS, "no failed chunks to retry")
        return CommandDecision.allow(cmd, next_state=WorkflowState(JobState.QUEUED, JobPhase.PROCESS))

    if cmd == JobCommand.MERGE:
        if state == JobState.CANCELLED:
            return _reject(cmd, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state == JobState.RUNNING:
            return _reject(cmd, WorkflowRejectionCode.JOB_RUNNING, "job is running")
        if state != JobState.PAUSED:
            return _reject(cmd, WorkflowRejectionCode.NOT_PAUSED, f"job is not paused (state={state.value})")
        if phase != JobPhase.MERGE:
            return _reject(cmd, WorkflowRejectionCode.INVALID_PHASE, f"job is not ready to merge (phase={phase.value})")
        if not context.chunks.all_done:
            return _reject(
                cmd, WorkflowRejectionCode.CHUNKS_INCOMPLETE, "job is not ready to merge (chunks incomplete)"
            )
        return CommandDecision.allow(cmd, next_state=WorkflowState(JobState.QUEUED, JobPhase.MERGE))

    if cmd == JobCommand.DETACH:
        if state in {JobState.QUEUED, JobState.RUNNING, JobState.CANCELLED}:
            return _reject(cmd, WorkflowRejectionCode.INVALID_STATE, f"cannot detach job in state={state.value}")
        return CommandDecision.allow(cmd, next_state=context.workflow)

    if cmd == JobCommand.RESET:
        if state == JobState.CANCELLED:
            return _reject(cmd, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state in {JobState.DONE, JobState.ERROR}:
            return CommandDecision.allow(cmd, next_state=context.workflow)
        return CommandDecision.allow(cmd, next_state=WorkflowState(JobState.CANCELLED, phase))

    if cmd == JobCommand.DOWNLOAD:
        if state != JobState.DONE:
            return _reject(cmd, WorkflowRejectionCode.INVALID_STATE, f"cannot download job in state={state.value}")
        return CommandDecision.allow(cmd, next_state=context.workflow)

    raise WorkflowTransitionError(f"unhandled workflow command: {cmd.value}")


def require_command(context: WorkflowContext, command: JobCommand | str) -> CommandDecision:
    decision = decide_command(context, command)
    if not decision.allowed:
        assert decision.rejection is not None
        raise WorkflowTransitionError(decision.rejection.message, code=decision.rejection.code)
    return decision


def resume_decision(context: WorkflowContext) -> CommandDecision:
    if context.workflow.phase == JobPhase.VALIDATE:
        return decide_command(context, JobCommand.VALIDATE)
    if context.workflow.phase == JobPhase.PROCESS:
        return decide_command(context, JobCommand.PROCESS)
    if context.workflow.phase == JobPhase.MERGE:
        return CommandDecision.reject(JobCommand.PROCESS, WorkflowRejectionCode.INVALID_PHASE, "job is ready to merge")
    if context.workflow.phase == JobPhase.DONE:
        return CommandDecision.reject(JobCommand.PROCESS, WorkflowRejectionCode.DONE, "job is already done")
    raise WorkflowTransitionError(f"unhandled resume phase: {context.workflow.phase.value}")


def available_commands(context: WorkflowContext) -> list[JobCommand]:
    return [command for command in JobCommand if decide_command(context, command).allowed]


def _event_reject(event: WorkflowEvent, code: WorkflowRejectionCode, message: str) -> EventTransition:
    return EventTransition.reject(event, code, message)


def apply_event(context: WorkflowContext, event: WorkflowEvent | str) -> EventTransition:
    evt = WorkflowEvent(event)
    state = context.workflow.state
    phase = context.workflow.phase

    if evt == WorkflowEvent.CREATE_VALIDATION:
        return EventTransition.allow(evt, create_validation_state())

    if evt == WorkflowEvent.VALIDATION_COMPLETE:
        if phase != JobPhase.VALIDATE:
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_PHASE, f"validation cannot complete in phase={phase.value}"
            )
        if not _in_flight(state):
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_STATE, f"validation cannot complete in state={state.value}"
            )
        return EventTransition.allow(evt, WorkflowState(JobState.PAUSED, JobPhase.PROCESS, WaitReason.READY_TO_PROCESS))

    if evt == WorkflowEvent.START_PROCESS:
        decision = decide_command(context, JobCommand.PROCESS)
        if not decision.allowed:
            assert decision.rejection is not None
            return EventTransition.reject(evt, decision.rejection.code, decision.rejection.message)
        assert decision.next_state is not None
        return EventTransition.allow(evt, decision.next_state)

    if evt == WorkflowEvent.USER_PAUSE:
        decision = decide_command(context, JobCommand.PAUSE)
        if not decision.allowed:
            assert decision.rejection is not None
            return EventTransition.reject(evt, decision.rejection.code, decision.rejection.message)
        assert decision.next_state is not None
        return EventTransition.allow(evt, decision.next_state)

    if evt == WorkflowEvent.EXECUTION_STOPPED:
        if not _in_flight(state):
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_STATE, f"execution is not active (state={state.value})"
            )
        if phase == JobPhase.DONE:
            return _event_reject(evt, WorkflowRejectionCode.DONE, "job is already done")
        return EventTransition.allow(evt, WorkflowState(JobState.PAUSED, phase, WaitReason.USER_PAUSED))

    if evt == WorkflowEvent.PROCESS_COMPLETE:
        if phase != JobPhase.PROCESS:
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_PHASE, f"process cannot complete in phase={phase.value}"
            )
        if context.chunks.has_failed:
            return _event_reject(evt, WorkflowRejectionCode.FAILED_CHUNKS, "process has failed chunks")
        if not context.chunks.all_done:
            return _event_reject(evt, WorkflowRejectionCode.CHUNKS_INCOMPLETE, "process chunks are incomplete")
        return EventTransition.allow(evt, WorkflowState(JobState.PAUSED, JobPhase.MERGE, WaitReason.READY_TO_MERGE))

    if evt == WorkflowEvent.PROCESS_FAILED:
        if phase != JobPhase.PROCESS:
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_PHASE, f"process cannot fail in phase={phase.value}"
            )
        if not context.chunks.has_failed:
            return _event_reject(evt, WorkflowRejectionCode.NO_FAILED_CHUNKS, "process has no failed chunks")
        return EventTransition.allow(evt, WorkflowState(JobState.ERROR, JobPhase.PROCESS))

    if evt == WorkflowEvent.RETRY_FAILED_CHUNKS:
        decision = decide_command(context, JobCommand.RETRY_FAILED)
        if not decision.allowed:
            assert decision.rejection is not None
            return EventTransition.reject(evt, decision.rejection.code, decision.rejection.message)
        assert decision.next_state is not None
        return EventTransition.allow(evt, decision.next_state)

    if evt == WorkflowEvent.MERGE_STARTED:
        if phase != JobPhase.MERGE:
            return _event_reject(evt, WorkflowRejectionCode.INVALID_PHASE, f"merge cannot start in phase={phase.value}")
        if state == JobState.CANCELLED:
            return _event_reject(evt, WorkflowRejectionCode.CANCELLED, "job is cancelled")
        if state not in {JobState.PAUSED, JobState.QUEUED, JobState.RUNNING}:
            return _event_reject(evt, WorkflowRejectionCode.INVALID_STATE, f"merge cannot start in state={state.value}")
        if not context.chunks.all_done:
            return _event_reject(evt, WorkflowRejectionCode.CHUNKS_INCOMPLETE, "merge chunks are incomplete")
        return EventTransition.allow(evt, WorkflowState(JobState.RUNNING, JobPhase.MERGE))

    if evt == WorkflowEvent.MERGE_COMPLETE:
        if phase != JobPhase.MERGE:
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_PHASE, f"merge cannot complete in phase={phase.value}"
            )
        if not context.chunks.all_done:
            return _event_reject(evt, WorkflowRejectionCode.CHUNKS_INCOMPLETE, "merge chunks are incomplete")
        return EventTransition.allow(evt, WorkflowState(JobState.DONE, JobPhase.DONE))

    if evt == WorkflowEvent.RESET_DELETE:
        decision = decide_command(context, JobCommand.RESET)
        if not decision.allowed:
            assert decision.rejection is not None
            return EventTransition.reject(evt, decision.rejection.code, decision.rejection.message)
        assert decision.next_state is not None
        return EventTransition.allow(evt, decision.next_state)

    if evt == WorkflowEvent.RESTART_RECOVERY:
        if not _in_flight(state):
            return _event_reject(
                evt, WorkflowRejectionCode.INVALID_STATE, f"job is not in-flight (state={state.value})"
            )
        if phase == JobPhase.DONE:
            return _event_reject(evt, WorkflowRejectionCode.DONE, "job is already done")
        return EventTransition.allow(evt, WorkflowState(JobState.PAUSED, phase, WaitReason.SERVER_RECOVERED))

    raise WorkflowTransitionError(f"unhandled workflow event: {evt.value}")


def require_event(context: WorkflowContext, event: WorkflowEvent | str) -> EventTransition:
    transition = apply_event(context, event)
    if not transition.allowed:
        assert transition.rejection is not None
        raise WorkflowTransitionError(transition.rejection.message, code=transition.rejection.code)
    return transition


def can_pause(state: JobState | str, phase: JobPhase | str) -> WorkflowGuard:
    context = WorkflowContext.from_values(
        state=state, phase=phase, wait_reason=_default_wait_reason_for_guard(state, phase), chunks=()
    )
    decision = decide_command(context, JobCommand.PAUSE)
    if decision.allowed:
        return WorkflowGuard.allow()
    assert decision.rejection is not None
    return WorkflowGuard.reject(decision.rejection.message, code=decision.rejection.code)


def can_resume(state: JobState | str, phase: JobPhase | str) -> ResumeGuard:
    context = WorkflowContext.from_values(
        state=state, phase=phase, wait_reason=_default_wait_reason_for_guard(state, phase), chunks=()
    )
    decision = resume_decision(context)
    if decision.allowed:
        assert decision.target is not None
        return ResumeGuard.allow_target(decision.target)
    assert decision.rejection is not None
    return ResumeGuard.reject(decision.rejection.message, code=decision.rejection.code)


def can_retry_failed(state: JobState | str, chunks: Iterable[ChunkState | str]) -> WorkflowGuard:
    context = WorkflowContext.from_values(
        state=state,
        phase=JobPhase.PROCESS,
        wait_reason=_default_wait_reason_for_guard(state, JobPhase.PROCESS),
        chunks=chunks,
    )
    decision = decide_command(context, JobCommand.RETRY_FAILED)
    if decision.allowed:
        return WorkflowGuard.allow()
    assert decision.rejection is not None
    return WorkflowGuard.reject(decision.rejection.message, code=decision.rejection.code)


def can_merge(state: JobState | str, phase: JobPhase | str, chunks: Iterable[ChunkState | str]) -> WorkflowGuard:
    context = WorkflowContext.from_values(
        state=state, phase=phase, wait_reason=_default_wait_reason_for_guard(state, phase), chunks=chunks
    )
    decision = decide_command(context, JobCommand.MERGE)
    if decision.allowed:
        return WorkflowGuard.allow()
    assert decision.rejection is not None
    return WorkflowGuard.reject(decision.rejection.message, code=decision.rejection.code)


def processing_final_state(chunks: Iterable[ChunkState | str]) -> ProcessingFinalState:
    chunk_states = [ChunkState(chunk) for chunk in chunks]
    if any(chunk == ChunkState.ERROR for chunk in chunk_states):
        return ProcessingFinalState.ERROR
    return ProcessingFinalState.READY_TO_MERGE


def validate_job_phase_invariants(
    state: JobState | str,
    phase: JobPhase | str,
    chunks: Iterable[ChunkState | str],
) -> None:
    context = WorkflowContext.from_values(
        state=state, phase=phase, wait_reason=_default_wait_reason_for_guard(state, phase), chunks=chunks
    )
    context.validate()
    context.chunks.validate_for_phase(context.workflow.phase)
