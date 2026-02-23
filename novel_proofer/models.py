from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ErrorEnvelope(BaseModel):
    code: str
    message: str
    request_id: str | None = None


class LLMOptions(BaseModel):
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    temperature: float = 0.0
    timeout_seconds: float = 180.0
    max_concurrency: int = 20
    extra_params: dict[str, Any] | None = None


class LLMSettings(BaseModel):
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    temperature: float | None = None
    timeout_seconds: float | None = None
    max_concurrency: int | None = None
    extra_params: dict[str, Any] | None = None


class LLMSettingsResponse(BaseModel):
    llm: LLMSettings


class LLMSettingsPutRequest(BaseModel):
    llm: LLMSettings = Field(default_factory=LLMSettings)


class FormatOptions(BaseModel):
    max_chunk_chars: int = Field(default=2_000, ge=200, le=4_000)
    paragraph_indent: bool = True
    indent_with_fullwidth_space: bool = True
    normalize_blank_lines: bool = True
    trim_trailing_spaces: bool = True
    normalize_ellipsis: bool = True
    normalize_em_dash: bool = True
    normalize_cjk_punctuation: bool = True
    fix_cjk_punct_spacing: bool = True
    normalize_quotes: bool = False


class OutputOptions(BaseModel):
    suffix: str = "_rev"
    cleanup_debug_dir: bool = True


class JobOptions(BaseModel):
    format: FormatOptions = Field(default_factory=FormatOptions)
    llm: LLMOptions = Field(default_factory=LLMOptions)
    output: OutputOptions = Field(default_factory=OutputOptions)


class JobProgress(BaseModel):
    total_chunks: int
    done_chunks: int
    percent: int


class JobOut(BaseModel):
    id: str
    state: str
    phase: str
    created_at: float
    started_at: float | None
    finished_at: float | None
    input_filename: str
    output_filename: str
    output_path: str | None
    debug_dir: str
    progress: JobProgress
    format: FormatOptions
    last_error_code: int | None = None
    last_retry_count: int = 0
    llm_model: str | None = None
    stats: dict[str, int] = Field(default_factory=dict)
    error: str | None = None
    cleanup_debug_dir: bool = True


class ChunkOut(BaseModel):
    index: int
    state: str
    started_at: float | None = None
    finished_at: float | None = None
    retries: int = 0
    llm_model: str | None = None
    input_chars: int | None = None
    output_chars: int | None = None
    last_error_code: int | None = None
    last_error_message: str | None = None


class JobGetResponse(BaseModel):
    job: JobOut
    chunks: list[ChunkOut] | None = None
    chunk_counts: dict[str, int] | None = None
    has_more: bool | None = None


class JobCreateResponse(BaseModel):
    job: JobOut


class JobActionResponse(BaseModel):
    ok: bool
    job: JobOut | None = None


class RetryFailedRequest(BaseModel):
    llm: LLMOptions | None = None


class MergeRequest(BaseModel):
    cleanup_debug_dir: bool | None = None


class JobSummaryOut(BaseModel):
    id: str
    state: str
    phase: str
    created_at: float
    input_filename: str
    output_filename: str
    progress: JobProgress
    last_error_code: int | None = None
    llm_model: str | None = None


class JobListResponse(BaseModel):
    jobs: list[JobSummaryOut]


class InputStatsOut(BaseModel):
    job_id: str
    input_chars: int


class PurgeAllRequest(BaseModel):
    exclude: list[str] = Field(default_factory=list)


class PurgeAllResponse(BaseModel):
    ok: bool
    purged: int
