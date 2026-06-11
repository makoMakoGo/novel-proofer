from __future__ import annotations

import re
from dataclasses import dataclass, fields
from typing import Any

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.states import ChunkState, JobPhase, JobState, WaitReason
from novel_proofer.workflow import WorkflowContext, WorkflowInvariantError

JOB_RECORD_VERSION = 4

_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
_JOB_PHASES = {phase.value for phase in JobPhase}
_JOB_STATES = {state.value for state in JobState}
_WAIT_REASONS = {reason.value for reason in WaitReason}
_CHUNK_STATE_VALUES = {state.value for state in ChunkState}
_CHUNK_STATES = (
    ChunkState.PENDING,
    ChunkState.PROCESSING,
    ChunkState.RETRYING,
    ChunkState.DONE,
    ChunkState.ERROR,
)
_FORMAT_DEFAULTS = FormatConfig()


@dataclass(frozen=True)
class ChunkRecord:
    index: int
    state: str
    started_at: float | None
    finished_at: float | None
    retries: int
    last_error_code: int | None
    last_error_message: str | None
    llm_model: str | None
    input_chars: int | None
    output_chars: int | None


@dataclass(frozen=True)
class WorkflowRecord:
    state: str
    phase: str
    wait_reason: str | None


@dataclass(frozen=True)
class TimestampRecord:
    created_at: float
    started_at: float | None
    finished_at: float | None


@dataclass(frozen=True)
class ArtifactRecord:
    input_filename: str
    output_filename: str
    output_path: str | None
    work_dir: str | None
    cleanup_debug_dir: bool


@dataclass(frozen=True)
class LLMRecord:
    last_error_code: int | None
    last_retry_count: int
    last_llm_model: str | None


@dataclass(frozen=True)
class ChunkSetRecord:
    total: int
    done: int
    counts: dict[str, int]
    items: list[ChunkRecord]


@dataclass(frozen=True)
class DiagnosticsRecord:
    stats: dict[str, int]
    error: str | None


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    workflow: WorkflowRecord
    timestamps: TimestampRecord
    artifacts: ArtifactRecord
    format: FormatConfig
    llm: LLMRecord
    chunks: ChunkSetRecord
    diagnostics: DiagnosticsRecord


def new_chunk_counts() -> dict[str, int]:
    return {state.value: 0 for state in _CHUNK_STATES}


def compute_chunk_counts(chunks: list[ChunkRecord]) -> dict[str, int]:
    out = new_chunk_counts()
    for chunk in chunks:
        out[chunk.state] = out.get(chunk.state, 0) + 1
    return out


def job_record_to_payload(record: JobRecord) -> dict[str, Any]:
    return {
        "version": JOB_RECORD_VERSION,
        "job_record": {
            "job_id": record.job_id,
            "workflow": {
                "state": record.workflow.state,
                "phase": record.workflow.phase,
                "wait_reason": record.workflow.wait_reason,
            },
            "timestamps": {
                "created_at": record.timestamps.created_at,
                "started_at": record.timestamps.started_at,
                "finished_at": record.timestamps.finished_at,
            },
            "artifacts": {
                "input_filename": record.artifacts.input_filename,
                "output_filename": record.artifacts.output_filename,
                "output_path": record.artifacts.output_path,
                "work_dir": record.artifacts.work_dir,
                "cleanup_debug_dir": record.artifacts.cleanup_debug_dir,
            },
            "format": _format_config_to_dict(record.format),
            "llm": {
                "last_error_code": record.llm.last_error_code,
                "last_retry_count": record.llm.last_retry_count,
                "last_llm_model": record.llm.last_llm_model,
            },
            "chunks": {
                "total": record.chunks.total,
                "done": record.chunks.done,
                "counts": dict(record.chunks.counts),
                "items": [_chunk_to_dict(chunk) for chunk in record.chunks.items],
            },
            "diagnostics": {
                "stats": dict(record.diagnostics.stats),
                "error": record.diagnostics.error,
            },
        },
    }


def job_record_from_payload(raw_payload: object) -> JobRecord:
    payload = _require_dict(raw_payload, context="job record file")
    _reject_unknown_fields(payload, {"version", "job_record"}, context="job record file")
    version = _parse_int(
        _require_field(payload, "version", context="job record file"),
        context="job record file.version",
    )
    if version != JOB_RECORD_VERSION:
        raise ValueError(f"unsupported job record version {version}; expected {JOB_RECORD_VERSION}")

    record = _require_dict(_require_field(payload, "job_record", context="job record file"), context="job_record")
    _reject_unknown_fields(
        record,
        {"job_id", "workflow", "timestamps", "artifacts", "format", "llm", "chunks", "diagnostics"},
        context="job_record",
    )

    job_id = _parse_str(_require_field(record, "job_id", context="job_record"), context="job_record.job_id")
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("job_record.job_id must be a 32-character lowercase hex string")

    workflow = _workflow_from_dict(_require_field(record, "workflow", context="job_record"))
    timestamps = _timestamps_from_dict(_require_field(record, "timestamps", context="job_record"))
    artifacts = _artifacts_from_dict(_require_field(record, "artifacts", context="job_record"))
    llm = _llm_from_dict(_require_field(record, "llm", context="job_record"))
    chunks = _chunks_from_dict(_require_field(record, "chunks", context="job_record"))
    diagnostics = _diagnostics_from_dict(_require_field(record, "diagnostics", context="job_record"))

    try:
        context = WorkflowContext.from_values(
            state=workflow.state,
            phase=workflow.phase,
            wait_reason=workflow.wait_reason,
            chunks=[chunk.state for chunk in chunks.items],
        )
        context.chunks.validate_for_phase(context.workflow.phase)
    except WorkflowInvariantError as e:
        raise ValueError(str(e)) from e

    return JobRecord(
        job_id=job_id,
        workflow=workflow,
        timestamps=timestamps,
        artifacts=artifacts,
        format=_format_config_from_dict(_require_field(record, "format", context="job_record")),
        llm=llm,
        chunks=chunks,
        diagnostics=diagnostics,
    )


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
    return int(raw)


def _parse_float(raw: Any, *, context: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"{context} must be a number")
    return float(raw)


def _parse_optional_float(raw: Any, *, context: str) -> float | None:
    if raw is None:
        return None
    return _parse_float(raw, context=context)


def _parse_bool(raw: Any, *, context: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{context} must be a boolean")
    return raw


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


def _parse_str(raw: Any, *, context: str, allow_empty: bool = False) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{context} must be a string")
    if not allow_empty and not raw.strip():
        raise ValueError(f"{context} must not be empty")
    return raw


def _parse_optional_str(raw: Any, *, context: str) -> str | None:
    if raw is None:
        return None
    return _parse_str(raw, context=context, allow_empty=True)


def _parse_enum(raw: Any, *, context: str, allowed: set[str]) -> str:
    value = _parse_str(raw, context=context)
    if value not in allowed:
        raise ValueError(f"{context} must be one of {sorted(allowed)!r}")
    return value


def _parse_stats(raw: object) -> dict[str, int]:
    stats = _require_dict(raw, context="job_record.diagnostics.stats")
    out: dict[str, int] = {}
    for key, value in stats.items():
        if not isinstance(key, str) or not key:
            raise ValueError("job_record.diagnostics.stats keys must be non-empty strings")
        out[key] = _parse_non_negative_int(value, context=f"job_record.diagnostics.stats[{key!r}]")
    return out


def _format_config_to_dict(config: FormatConfig) -> dict[str, Any]:
    return {field.name: getattr(config, field.name) for field in fields(FormatConfig)}


def _format_config_from_dict(raw: object) -> FormatConfig:
    config = _require_dict(raw, context="job_record.format")
    allowed = {field.name for field in fields(FormatConfig)}
    _reject_unknown_fields(config, allowed, context="job_record.format")
    kwargs: dict[str, Any] = {}
    for field in fields(FormatConfig):
        value = _require_field(config, field.name, context="job_record.format")
        default = getattr(_FORMAT_DEFAULTS, field.name)
        if isinstance(default, bool):
            kwargs[field.name] = _parse_bool(value, context=f"job_record.format.{field.name}")
            continue
        if isinstance(default, int):
            kwargs[field.name] = _parse_int(value, context=f"job_record.format.{field.name}")
            continue
        kwargs[field.name] = value
    return FormatConfig(**kwargs)


def _chunk_to_dict(chunk: ChunkRecord) -> dict[str, Any]:
    return {
        "index": chunk.index,
        "state": chunk.state,
        "started_at": chunk.started_at,
        "finished_at": chunk.finished_at,
        "retries": chunk.retries,
        "last_error_code": chunk.last_error_code,
        "last_error_message": chunk.last_error_message,
        "llm_model": chunk.llm_model,
        "input_chars": chunk.input_chars,
        "output_chars": chunk.output_chars,
    }


def _chunk_from_dict(raw_item: object) -> ChunkRecord:
    raw = _require_dict(raw_item, context="chunk")
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
    return ChunkRecord(
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


def _workflow_from_dict(raw_value: object) -> WorkflowRecord:
    raw = _require_dict(raw_value, context="job_record.workflow")
    _reject_unknown_fields(raw, {"state", "phase", "wait_reason"}, context="job_record.workflow")
    state = _parse_enum(
        _require_field(raw, "state", context="job_record.workflow"),
        context="job_record.workflow.state",
        allowed=_JOB_STATES,
    )
    phase = _parse_enum(
        _require_field(raw, "phase", context="job_record.workflow"),
        context="job_record.workflow.phase",
        allowed=_JOB_PHASES,
    )
    wait_reason = _parse_optional_str(raw.get("wait_reason"), context="job_record.workflow.wait_reason")
    if wait_reason is not None and wait_reason not in _WAIT_REASONS:
        raise ValueError(f"job_record.workflow.wait_reason must be one of {sorted(_WAIT_REASONS)!r}")
    return WorkflowRecord(state=state, phase=phase, wait_reason=wait_reason)


def _timestamps_from_dict(raw_value: object) -> TimestampRecord:
    raw = _require_dict(raw_value, context="job_record.timestamps")
    _reject_unknown_fields(raw, {"created_at", "started_at", "finished_at"}, context="job_record.timestamps")
    return TimestampRecord(
        created_at=_parse_float(
            _require_field(raw, "created_at", context="job_record.timestamps"),
            context="job_record.timestamps.created_at",
        ),
        started_at=_parse_optional_float(raw.get("started_at"), context="job_record.timestamps.started_at"),
        finished_at=_parse_optional_float(raw.get("finished_at"), context="job_record.timestamps.finished_at"),
    )


def _artifacts_from_dict(raw_value: object) -> ArtifactRecord:
    raw = _require_dict(raw_value, context="job_record.artifacts")
    _reject_unknown_fields(
        raw,
        {"input_filename", "output_filename", "output_path", "work_dir", "cleanup_debug_dir"},
        context="job_record.artifacts",
    )
    return ArtifactRecord(
        input_filename=_parse_str(
            _require_field(raw, "input_filename", context="job_record.artifacts"),
            context="job_record.artifacts.input_filename",
        ),
        output_filename=_parse_str(
            _require_field(raw, "output_filename", context="job_record.artifacts"),
            context="job_record.artifacts.output_filename",
        ),
        output_path=_parse_optional_str(raw.get("output_path"), context="job_record.artifacts.output_path"),
        work_dir=_parse_optional_str(raw.get("work_dir"), context="job_record.artifacts.work_dir"),
        cleanup_debug_dir=_parse_bool(
            _require_field(raw, "cleanup_debug_dir", context="job_record.artifacts"),
            context="job_record.artifacts.cleanup_debug_dir",
        ),
    )


def _llm_from_dict(raw_value: object) -> LLMRecord:
    raw = _require_dict(raw_value, context="job_record.llm")
    _reject_unknown_fields(
        raw,
        {"last_error_code", "last_retry_count", "last_llm_model"},
        context="job_record.llm",
    )
    return LLMRecord(
        last_error_code=_parse_optional_int(raw.get("last_error_code"), context="job_record.llm.last_error_code"),
        last_retry_count=_parse_non_negative_int(
            _require_field(raw, "last_retry_count", context="job_record.llm"),
            context="job_record.llm.last_retry_count",
        ),
        last_llm_model=_parse_optional_str(raw.get("last_llm_model"), context="job_record.llm.last_llm_model"),
    )


def _parse_chunk_counts(raw_value: object, chunks: list[ChunkRecord]) -> dict[str, int]:
    counts = _require_dict(raw_value, context="job_record.chunks.counts")
    expected = {state.value for state in _CHUNK_STATES}
    extra = sorted(set(counts) - expected)
    if extra:
        raise ValueError(f"job_record.chunks.counts contains unknown fields: {', '.join(extra)}")
    missing = sorted(expected - set(counts))
    if missing:
        raise ValueError(f"job_record.chunks.counts missing fields: {', '.join(missing)}")
    out = {
        key: _parse_non_negative_int(value, context=f"job_record.chunks.counts[{key!r}]")
        for key, value in counts.items()
    }
    computed = compute_chunk_counts(chunks)
    if out != computed:
        raise ValueError("job_record.chunks.counts does not match chunk items")
    return out


def _chunks_from_dict(raw_value: object) -> ChunkSetRecord:
    raw = _require_dict(raw_value, context="job_record.chunks")
    _reject_unknown_fields(raw, {"total", "done", "counts", "items"}, context="job_record.chunks")

    raw_items = _require_field(raw, "items", context="job_record.chunks")
    if not isinstance(raw_items, list):
        raise ValueError("job_record.chunks.items must be a list")
    chunks = [_chunk_from_dict(item) for item in raw_items]
    for expected_index, chunk in enumerate(chunks):
        if chunk.index != expected_index:
            raise ValueError(
                f"chunk index mismatch at position {expected_index}: expected {expected_index}, got {chunk.index}"
            )

    total = _parse_non_negative_int(
        _require_field(raw, "total", context="job_record.chunks"),
        context="job_record.chunks.total",
    )
    done = _parse_non_negative_int(
        _require_field(raw, "done", context="job_record.chunks"),
        context="job_record.chunks.done",
    )
    counted_done = sum(1 for chunk in chunks if chunk.state == ChunkState.DONE)
    if total != len(chunks):
        raise ValueError("job_record.chunks.total does not match chunk items length")
    if done != counted_done:
        raise ValueError("job_record.chunks.done does not match chunk items")

    return ChunkSetRecord(
        total=total,
        done=done,
        counts=_parse_chunk_counts(_require_field(raw, "counts", context="job_record.chunks"), chunks),
        items=chunks,
    )


def _diagnostics_from_dict(raw_value: object) -> DiagnosticsRecord:
    raw = _require_dict(raw_value, context="job_record.diagnostics")
    _reject_unknown_fields(raw, {"stats", "error"}, context="job_record.diagnostics")
    return DiagnosticsRecord(
        stats=_parse_stats(_require_field(raw, "stats", context="job_record.diagnostics")),
        error=_parse_optional_str(raw.get("error"), context="job_record.diagnostics.error"),
    )
