from __future__ import annotations

import json
import os
import re
import shutil
import threading
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS, ChunkStatus, JobStatus
from novel_proofer.llm.config import LLMConfig
from novel_proofer.runner import resume_paused_job, retry_failed_chunks, run_job


WORKDIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = WORKDIR / "templates"

OUTPUT_DIR = WORKDIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
JOBS_DIR = OUTPUT_DIR / ".jobs"
JOBS_DIR.mkdir(exist_ok=True)

_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

_filename_strip_re = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff\u3400-\u4dbf\u3000-\u303f\uFF00-\uFFEF._ -]+")

MAX_UPLOAD_BYTES = 200 * 1024 * 1024


def _safe_filename(name: str) -> str:
    base = os.path.basename(name or "")
    base = base.replace("\\", "_").replace("/", "_").strip()
    if not base:
        return "input.txt"
    base = _filename_strip_re.sub("_", base)
    return base[:200]


def _derive_output_filename(input_name: str, suffix: str) -> str:
    input_name = _safe_filename(input_name)
    suffix = (suffix or "").strip()
    if not suffix:
        suffix = "_rev"

    p = Path(input_name)
    stem = p.stem or "output"
    ext = p.suffix if p.suffix else ".txt"

    out = f"{stem}{suffix}{ext}"
    return _safe_filename(out)


def _decode_text(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def _rel_output_path(output_abs: Path) -> str:
    # Keep UI hints stable and avoid leaking local absolute paths.
    return f"output/{output_abs.name}"


def _rel_debug_dir(job_id: str) -> str:
    return f"output/.jobs/{job_id}/"


def _cleanup_job_dir(job_id: str) -> bool:
    """Delete output/.jobs/<job_id>/ directory (best-effort, safe-guarded)."""

    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")

    root = JOBS_DIR.resolve()
    target = (JOBS_DIR / job_id).resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    shutil.rmtree(target)
    return True


class ErrorEnvelope(BaseModel):
    code: str
    message: str


def _error_code_for_status(status_code: int) -> str:
    if status_code == 404:
        return "not_found"
    if status_code == 409:
        return "conflict"
    if status_code in {400, 413, 422}:
        return "bad_request"
    return "internal_error"


def _error(status_code: int, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": ErrorEnvelope(code=_error_code_for_status(status_code), message=message).model_dump()},
    )


class LLMOptions(BaseModel):
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    temperature: float = 0.0
    timeout_seconds: float = 180.0
    max_concurrency: int = 20
    extra_params: dict[str, Any] | None = None
    filter_think_tags: bool = True


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
    created_at: float
    started_at: float | None
    finished_at: float | None
    input_filename: str
    output_filename: str
    output_path: str | None
    debug_dir: str
    progress: JobProgress
    last_error_code: int | None = None
    last_retry_count: int = 0
    stats: dict[str, int] = Field(default_factory=dict)
    error: str | None = None
    cleanup_debug_dir: bool = True


class ChunkOut(BaseModel):
    index: int
    state: str
    started_at: float | None = None
    finished_at: float | None = None
    retries: int = 0
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


def _job_to_out(st: JobStatus) -> JobOut:
    pct = 0
    if st.total_chunks > 0:
        pct = int((st.done_chunks / st.total_chunks) * 100)

    output_path = None
    if st.output_path:
        output_path = _rel_output_path(Path(st.output_path))

    return JobOut(
        id=st.job_id,
        state=st.state,
        created_at=st.created_at,
        started_at=st.started_at,
        finished_at=st.finished_at,
        input_filename=st.input_filename,
        output_filename=st.output_filename,
        output_path=output_path,
        debug_dir=_rel_debug_dir(st.job_id),
        progress=JobProgress(total_chunks=st.total_chunks, done_chunks=st.done_chunks, percent=pct),
        last_error_code=st.last_error_code,
        last_retry_count=st.last_retry_count,
        stats=dict(st.stats),
        error=st.error,
        cleanup_debug_dir=bool(getattr(st, "cleanup_debug_dir", True)),
    )


def _chunk_to_out(cs: ChunkStatus) -> ChunkOut:
    return ChunkOut(
        index=cs.index,
        state=cs.state,
        started_at=cs.started_at,
        finished_at=cs.finished_at,
        retries=cs.retries,
        input_chars=cs.input_chars,
        output_chars=cs.output_chars,
        last_error_code=cs.last_error_code,
        last_error_message=cs.last_error_message,
    )


async def _read_upload_limited(upload: UploadFile, limit: int) -> bytes:
    total = 0
    parts: list[bytes] = []
    while True:
        chunk = await upload.read(1024 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise HTTPException(status_code=413, detail=f"file too large (> {limit} bytes)")
        parts.append(chunk)
    return b"".join(parts)


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


def _llm_from_options(opts: LLMOptions) -> LLMConfig:
    return LLMConfig(
        base_url=str(opts.base_url or "").strip(),
        api_key=str(opts.api_key or "").strip(),
        model=str(opts.model or "").strip(),
        temperature=float(opts.temperature),
        timeout_seconds=float(opts.timeout_seconds),
        max_concurrency=int(opts.max_concurrency),
        extra_params=opts.extra_params,
        filter_think_tags=bool(opts.filter_think_tags),
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


app = FastAPI()


@app.exception_handler(HTTPException)
async def _http_exception_handler(_request: Request, exc: HTTPException):  # noqa: ANN001
    return _error(int(exc.status_code), str(exc.detail))


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(_request: Request, exc: RequestValidationError):  # noqa: ANN001
    msg = "bad request"
    try:
        errors = exc.errors()
        if errors:
            msg = errors[0].get("msg") or msg
    except Exception:
        pass
    return _error(400, msg)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(_request: Request, exc: Exception):  # noqa: ANN001
    return _error(500, str(exc))


@app.get("/", include_in_schema=False)
async def index():
    path = TEMPLATES_DIR / "index.html"
    if not path.exists():
        raise HTTPException(status_code=500, detail="missing templates/index.html")
    return FileResponse(path, media_type="text/html; charset=utf-8")


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.post("/api/v1/jobs", response_model=JobCreateResponse, status_code=201)
async def create_job(file: UploadFile = File(...), options: str = Form(...)):
    if file is None:
        raise HTTPException(status_code=400, detail="file is required")
    opts = _parse_options_json(options)

    raw = await _read_upload_limited(file, MAX_UPLOAD_BYTES)
    input_text = _decode_text(raw)

    out_name = _derive_output_filename(file.filename or "input.txt", opts.output.suffix)

    job = GLOBAL_JOBS.create(file.filename or "input.txt", out_name, total_chunks=0)
    output_abs = OUTPUT_DIR / f"{job.job_id}_{out_name}"
    work_dir = JOBS_DIR / job.job_id
    GLOBAL_JOBS.update(
        job.job_id,
        output_filename=output_abs.name,
        output_path=str(output_abs),
        work_dir=str(work_dir),
        cleanup_debug_dir=bool(opts.output.cleanup_debug_dir),
    )

    fmt = _format_from_options(opts.format)
    llm = _llm_from_options(opts.llm)

    t = threading.Thread(
        target=run_job,
        args=(job.job_id, input_text, fmt, llm),
        daemon=True,
    )
    t.start()

    st = GLOBAL_JOBS.get(job.job_id)
    if st is None:
        raise HTTPException(status_code=500, detail="job store error")
    return JobCreateResponse(job=_job_to_out(st))


@app.get("/api/v1/jobs/{job_id}", response_model=JobGetResponse)
async def get_job(
    job_id: str,
    *,
    chunks: int = Query(1, ge=0, le=1),
    chunk_state: str = Query("all"),
    limit: int = Query(0, ge=0),
    offset: int = Query(0, ge=0),
):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")

    payload = JobGetResponse(job=_job_to_out(st))
    if chunks != 1:
        return payload

    allowed_filters = {"all", "pending", "processing", "retrying", "done", "error"}
    chunk_state = str(chunk_state or "all").strip().lower()
    if chunk_state not in allowed_filters:
        chunk_state = "all"

    chunk_counts: dict[str, int] = {}
    matched = 0
    has_more = False
    out_chunks: list[ChunkOut] = []
    for c in st.chunk_statuses:
        chunk_counts[c.state] = chunk_counts.get(c.state, 0) + 1

        if chunk_state != "all" and c.state != chunk_state:
            continue

        if matched < offset:
            matched += 1
            continue

        if limit > 0 and len(out_chunks) >= limit:
            has_more = True
            matched += 1
            continue

        out_chunks.append(_chunk_to_out(c))
        matched += 1

    payload.chunks = out_chunks
    payload.chunk_counts = chunk_counts
    payload.has_more = has_more
    return payload


@app.post("/api/v1/jobs/{job_id}/cancel", response_model=JobActionResponse)
async def cancel_job(job_id: str):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    GLOBAL_JOBS.cancel(job_id)
    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/pause", response_model=JobActionResponse)
async def pause_job(job_id: str):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not GLOBAL_JOBS.pause(job_id):
        raise HTTPException(status_code=409, detail=f"cannot pause job in state={st.state}")
    st2 = GLOBAL_JOBS.get(job_id) or st
    return JobActionResponse(ok=True, job=_job_to_out(st2))


@app.post("/api/v1/jobs/{job_id}/resume", response_model=JobActionResponse)
async def resume_job(job_id: str, body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state == "running":
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == "cancelled":
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state != "paused":
        raise HTTPException(status_code=409, detail="job is not paused")
    if not GLOBAL_JOBS.resume(job_id):
        raise HTTPException(status_code=409, detail="failed to resume job")

    llm = _llm_from_options(body.llm or LLMOptions())
    t = threading.Thread(
        target=resume_paused_job,
        args=(job_id, llm),
        daemon=True,
    )
    t.start()
    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/retry-failed", response_model=JobActionResponse)
async def retry_failed(job_id: str, body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state == "running":
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == "cancelled":
        raise HTTPException(status_code=409, detail="job is cancelled")

    llm = _llm_from_options(body.llm or LLMOptions())
    t = threading.Thread(
        target=retry_failed_chunks,
        args=(job_id, llm),
        daemon=True,
    )
    t.start()
    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/cleanup-debug", response_model=JobActionResponse)
async def cleanup_debug(job_id: str):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == "cancelled":
        raise HTTPException(status_code=409, detail="job is cancelled")

    try:
        _cleanup_job_dir(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    deleted = GLOBAL_JOBS.delete(job_id)
    return JobActionResponse(ok=True, job=_job_to_out(st) if deleted else None)
