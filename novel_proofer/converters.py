from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from novel_proofer.dotenv_store import LLMDefaults
from novel_proofer.executions import ExecutionSnapshot, StopReason
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import ChunkStatus, JobStatus
from novel_proofer.llm.config import LLMConfig
from novel_proofer.models import (
    ChunkOut,
    ErrorEnvelope,
    FormatOptions,
    JobOptions,
    JobOut,
    JobProgress,
    JobSummaryOut,
    LLMOptions,
    LLMSettings,
)
from novel_proofer.paths import _rel_debug_dir, _rel_output_path
from novel_proofer.states import ExecutionState, JobCommand, JobPhase, JobState, TerminalState, WaitReason
from novel_proofer.workflow import available_commands
from novel_proofer.workflow_context import workflow_context_for_job

_INTERNAL_ERROR_MESSAGE = "internal server error"

_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass(frozen=True)
class _JobSnapshotFields:
    workflow_phase: str
    execution_state: str
    wait_reason: str | None
    terminal_state: str | None
    available_commands: list[str]


def _error_code_for_status(status_code: int) -> str:
    if status_code == 404:
        return "not_found"
    if status_code == 409:
        return "conflict"
    if status_code in {400, 413, 422}:
        return "bad_request"
    return "internal_error"


def _error(status_code: int, message: str, *, request_id: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": ErrorEnvelope(
                code=_error_code_for_status(status_code), message=message, request_id=request_id
            ).model_dump()
        },
    )


def _request_id_from_request(request: Request) -> str:
    existing = getattr(getattr(request, "state", object()), "request_id", None)
    if isinstance(existing, str) and existing:
        return existing

    incoming = str(request.headers.get("x-request-id", "") or "").strip()
    request_id = incoming if incoming and _REQUEST_ID_RE.fullmatch(incoming) else uuid.uuid4().hex

    request.state.request_id = request_id
    return request_id


def _job_to_out(st: JobStatus, execution: ExecutionSnapshot | None) -> JobOut:
    pct = 0
    if st.total_chunks > 0:
        pct = int((st.done_chunks / st.total_chunks) * 100)

    output_path = None
    if st.state == JobState.DONE and st.output_path:
        output_path = _rel_output_path(Path(st.output_path))

    fmt = st.format
    snapshot = _job_snapshot_fields(st, execution)
    return JobOut(
        id=st.job_id,
        workflow_phase=snapshot.workflow_phase,
        execution_state=snapshot.execution_state,
        wait_reason=snapshot.wait_reason,
        terminal_state=snapshot.terminal_state,
        available_commands=snapshot.available_commands,
        created_at=st.created_at,
        started_at=st.started_at,
        finished_at=st.finished_at,
        input_filename=st.input_filename,
        output_filename=st.output_filename,
        output_path=output_path,
        debug_dir=_rel_debug_dir(st.job_id),
        progress=JobProgress(total_chunks=st.total_chunks, done_chunks=st.done_chunks, percent=pct),
        format=FormatOptions(
            max_chunk_chars=fmt.max_chunk_chars,
            paragraph_indent=fmt.paragraph_indent,
            indent_with_fullwidth_space=fmt.indent_with_fullwidth_space,
            normalize_blank_lines=fmt.normalize_blank_lines,
            trim_trailing_spaces=fmt.trim_trailing_spaces,
            normalize_ellipsis=fmt.normalize_ellipsis,
            normalize_em_dash=fmt.normalize_em_dash,
            normalize_cjk_punctuation=fmt.normalize_cjk_punctuation,
            fix_cjk_punct_spacing=fmt.fix_cjk_punct_spacing,
            normalize_quotes=fmt.normalize_quotes,
        ),
        last_error_code=st.last_error_code,
        last_retry_count=st.last_retry_count,
        llm_model=st.last_llm_model,
        stats=dict(st.stats),
        error=st.error,
        cleanup_debug_dir=st.cleanup_debug_dir,
    )


def _job_summary_to_out(st: JobStatus, execution: ExecutionSnapshot | None) -> JobSummaryOut:
    pct = 0
    if st.total_chunks > 0:
        pct = int((st.done_chunks / st.total_chunks) * 100)

    snapshot = _job_snapshot_fields(st, execution)
    return JobSummaryOut(
        id=st.job_id,
        workflow_phase=snapshot.workflow_phase,
        execution_state=snapshot.execution_state,
        wait_reason=snapshot.wait_reason,
        terminal_state=snapshot.terminal_state,
        available_commands=snapshot.available_commands,
        created_at=st.created_at,
        input_filename=st.input_filename,
        output_filename=st.output_filename,
        progress=JobProgress(total_chunks=st.total_chunks, done_chunks=st.done_chunks, percent=pct),
        last_error_code=st.last_error_code,
        llm_model=st.last_llm_model,
    )


def _available_commands(st: JobStatus, execution: ExecutionSnapshot | None) -> list[str]:
    commands = [command.value for command in available_commands(workflow_context_for_job(st))]
    if execution is None:
        if JobState(st.state) in {JobState.QUEUED, JobState.RUNNING}:
            return [command for command in commands if command == JobCommand.RESET.value]
        return commands
    if execution.stop_requested == StopReason.DELETE.value:
        return []
    if execution.stop_requested == StopReason.PAUSE.value:
        return [command for command in commands if command == JobCommand.RESET.value]
    active_commands = {JobCommand.PAUSE.value, JobCommand.RESET.value}
    return [command for command in commands if command in active_commands]


def _terminal_state_for(state: JobState) -> TerminalState | None:
    if state in {JobState.DONE, JobState.ERROR, JobState.CANCELLED}:
        return TerminalState(state.value)
    return None


def _job_snapshot_fields(st: JobStatus, execution: ExecutionSnapshot | None) -> _JobSnapshotFields:
    state = JobState(st.state)
    phase = JobPhase(st.phase)
    terminal_state = _terminal_state_for(state)
    active_execution = None if terminal_state is not None else execution
    execution_state = ExecutionState.IDLE if active_execution is None else ExecutionState(active_execution.state)
    wait_reason: str | None = None

    if state == JobState.PAUSED:
        if st.wait_reason is None:
            raise ValueError("paused job snapshot requires wait_reason")
        wait_reason = WaitReason(st.wait_reason).value

    return _JobSnapshotFields(
        workflow_phase=phase.value,
        execution_state=execution_state.value,
        wait_reason=wait_reason,
        terminal_state=terminal_state.value if terminal_state is not None else None,
        available_commands=_available_commands(st, active_execution),
    )


def _chunk_to_out(cs: ChunkStatus) -> ChunkOut:
    return ChunkOut(
        index=cs.index,
        state=cs.state,
        started_at=cs.started_at,
        finished_at=cs.finished_at,
        retries=cs.retries,
        llm_model=cs.llm_model,
        input_chars=cs.input_chars,
        output_chars=cs.output_chars,
        last_error_code=cs.last_error_code,
        last_error_message=cs.last_error_message,
    )


def _llm_from_options(opts: LLMOptions) -> LLMConfig:
    return LLMConfig(
        base_url=str(opts.base_url or "").strip(),
        api_key=str(opts.api_key or "").strip(),
        model=str(opts.model or "").strip(),
        temperature=float(opts.temperature),
        timeout_seconds=float(opts.timeout_seconds),
        max_concurrency=int(opts.max_concurrency),
        extra_params=opts.extra_params,
    )


def _format_from_options(opts: FormatOptions) -> FormatConfig:
    return FormatConfig(
        max_chunk_chars=int(opts.max_chunk_chars),
        paragraph_indent=bool(opts.paragraph_indent),
        indent_with_fullwidth_space=bool(opts.indent_with_fullwidth_space),
        normalize_blank_lines=bool(opts.normalize_blank_lines),
        trim_trailing_spaces=bool(opts.trim_trailing_spaces),
        normalize_ellipsis=bool(opts.normalize_ellipsis),
        normalize_em_dash=bool(opts.normalize_em_dash),
        normalize_cjk_punctuation=bool(opts.normalize_cjk_punctuation),
        fix_cjk_punct_spacing=bool(opts.fix_cjk_punct_spacing),
        normalize_quotes=bool(opts.normalize_quotes),
    )


def _llm_settings_from_defaults(d: LLMDefaults) -> LLMSettings:
    return LLMSettings(
        base_url=d.base_url,
        api_key=d.api_key,
        model=d.model,
        temperature=d.temperature,
        timeout_seconds=d.timeout_seconds,
        max_concurrency=d.max_concurrency,
        extra_params=d.extra_params,
    )


def _parse_options_json(options: str) -> JobOptions:
    try:
        data = json.loads(options)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"options must be valid JSON: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="options must be a JSON object")
    try:
        return JobOptions.model_validate(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid options: {e}") from e
