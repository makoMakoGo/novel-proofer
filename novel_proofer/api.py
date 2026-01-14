from __future__ import annotations

import codecs
import json
import logging
import os
import re
import shutil
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from novel_proofer.background import shutdown as shutdown_background
from novel_proofer.background import submit as submit_background_job
from novel_proofer.dotenv_store import (
    LLMDefaults,
    llm_env_updates_from_defaults_patch,
    read_llm_defaults,
    update_llm_defaults,
)
from novel_proofer.dotenv_store import (
    dotenv_path as dotenv_path,
)
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS, ChunkStatus, JobStatus
from novel_proofer.llm.config import LLMConfig
from novel_proofer.logging_setup import ensure_file_logging
from novel_proofer.runner import merge_outputs, resume_paused_job, retry_failed_chunks, run_job

logger = logging.getLogger(__name__)

WORKDIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = WORKDIR / "templates"
IMAGES_DIR = WORKDIR / "images"

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


def _input_cache_root() -> Path:
    return OUTPUT_DIR / ".inputs"


def _input_cache_path(job_id: str) -> Path:
    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")
    return _input_cache_root() / f"{job_id}.txt"


def _write_input_cache(job_id: str, text: str) -> None:
    p = _input_cache_path(job_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _input_upload_tmp_path(job_id: str) -> Path:
    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")
    return _input_cache_root() / f"{job_id}.upload.tmp"


async def _save_upload_limited_to_file(upload: UploadFile, *, limit: int, dst: Path) -> int:
    total = 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("wb") as f:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise HTTPException(status_code=413, detail=f"file too large (> {limit} bytes)")
            f.write(chunk)
    return total


def _transcode_bytes_file_to_utf8_text(
    src: Path,
    dst: Path,
    *,
    encoding: str,
    errors: str,
) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + f".{uuid.uuid4().hex}.tmp")
    decoder = codecs.getincrementaldecoder(encoding)(errors=errors)
    with src.open("rb") as fin, tmp.open("w", encoding="utf-8") as fout:
        while True:
            b = fin.read(1024 * 1024)
            if not b:
                break
            fout.write(decoder.decode(b))
        fout.write(decoder.decode(b"", final=True))
    tmp.replace(dst)


async def _write_input_cache_from_upload(job_id: str, upload: UploadFile, *, limit: int) -> None:
    """Write decoded input cache (utf-8) without keeping the whole upload in memory."""

    tmp_upload = _input_upload_tmp_path(job_id)
    dst = _input_cache_path(job_id)
    try:
        await _save_upload_limited_to_file(upload, limit=limit, dst=tmp_upload)

        # Try strict decoders first to avoid silently garbling text.
        for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
            try:
                _transcode_bytes_file_to_utf8_text(tmp_upload, dst, encoding=enc, errors="strict")
                return
            except UnicodeDecodeError:
                continue

        # Final fallback: keep going even with malformed bytes.
        _transcode_bytes_file_to_utf8_text(tmp_upload, dst, encoding="utf-8", errors="replace")
    finally:
        try:
            if tmp_upload.exists():
                tmp_upload.unlink()
        except Exception:
            logger.exception("failed to cleanup temp upload: %s", tmp_upload)


def _copy_input_cache(src_job_id: str, dst_job_id: str) -> None:
    src = _input_cache_path(src_job_id)
    if not src.exists():
        raise FileNotFoundError(str(src))
    dst = _input_cache_path(dst_job_id)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def _cleanup_input_cache(job_id: str) -> bool:
    """Delete output/.inputs/<job_id>.txt (best-effort, safe-guarded)."""

    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")

    root = _input_cache_root().resolve()
    target = _input_cache_path(job_id).resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    target.unlink()
    return True


def _jobs_state_root() -> Path:
    return OUTPUT_DIR / ".state" / "jobs"


def _cleanup_job_state(job_id: str) -> bool:
    """Delete output/.state/jobs/<job_id>.json (best-effort, safe-guarded)."""

    job_id = (job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")

    root = _jobs_state_root().resolve()
    target = (root / f"{job_id}.json").resolve()
    if target == root or root not in target.parents:
        raise ValueError("invalid job_id")

    if not target.exists():
        return False

    target.unlink()
    return True


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


def _job_to_out(st: JobStatus) -> JobOut:
    pct = 0
    if st.total_chunks > 0:
        pct = int((st.done_chunks / st.total_chunks) * 100)

    output_path = None
    if st.state == "done" and st.output_path:
        output_path = _rel_output_path(Path(st.output_path))

    return JobOut(
        id=st.job_id,
        state=st.state,
        phase=str(getattr(st, "phase", "validate")),
        created_at=st.created_at,
        started_at=st.started_at,
        finished_at=st.finished_at,
        input_filename=st.input_filename,
        output_filename=st.output_filename,
        output_path=output_path,
        debug_dir=_rel_debug_dir(st.job_id),
        progress=JobProgress(total_chunks=st.total_chunks, done_chunks=st.done_chunks, percent=pct),
        format=FormatOptions(
            max_chunk_chars=int(getattr(getattr(st, "format", None), "max_chunk_chars", 2000)),
            paragraph_indent=bool(getattr(getattr(st, "format", None), "paragraph_indent", True)),
            indent_with_fullwidth_space=bool(getattr(getattr(st, "format", None), "indent_with_fullwidth_space", True)),
            normalize_blank_lines=bool(getattr(getattr(st, "format", None), "normalize_blank_lines", True)),
            trim_trailing_spaces=bool(getattr(getattr(st, "format", None), "trim_trailing_spaces", True)),
            normalize_ellipsis=bool(getattr(getattr(st, "format", None), "normalize_ellipsis", True)),
            normalize_em_dash=bool(getattr(getattr(st, "format", None), "normalize_em_dash", True)),
            normalize_cjk_punctuation=bool(getattr(getattr(st, "format", None), "normalize_cjk_punctuation", True)),
            fix_cjk_punct_spacing=bool(getattr(getattr(st, "format", None), "fix_cjk_punct_spacing", True)),
            normalize_quotes=bool(getattr(getattr(st, "format", None), "normalize_quotes", False)),
        ),
        last_error_code=st.last_error_code,
        last_retry_count=st.last_retry_count,
        llm_model=getattr(st, "last_llm_model", None),
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
        llm_model=getattr(cs, "llm_model", None),
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


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    # These globals are monkeypatched in tests; use the current values at startup time.
    OUTPUT_DIR.mkdir(exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    _input_cache_root().mkdir(parents=True, exist_ok=True)
    _jobs_state_root().mkdir(parents=True, exist_ok=True)

    log_file = ensure_file_logging(log_dir=OUTPUT_DIR / "logs")
    logger.info("file logging enabled: %s", log_file)

    GLOBAL_JOBS.configure_persistence(persist_dir=_jobs_state_root())
    loaded = GLOBAL_JOBS.load_persisted_jobs()
    if loaded:
        logger.info("loaded %s persisted jobs", loaded)

    yield

    shutdown_background(wait=False)


app = FastAPI(lifespan=_lifespan)

# Mount static files for images
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")


@app.exception_handler(HTTPException)
async def _http_exception_handler(_request: Request, exc: HTTPException):
    return _error(int(exc.status_code), str(exc.detail))


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(_request: Request, exc: RequestValidationError):
    msg = "bad request"
    try:
        errors = exc.errors()
        if errors:
            msg = errors[0].get("msg") or msg
    except Exception:
        pass
    return _error(400, msg)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(_request: Request, exc: Exception):
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


@app.get("/api/v1/settings/llm", response_model=LLMSettingsResponse)
async def get_llm_settings():
    path = dotenv_path(workdir=WORKDIR)
    try:
        defaults = read_llm_defaults(path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return LLMSettingsResponse(llm=_llm_settings_from_defaults(defaults))


@app.put("/api/v1/settings/llm", response_model=LLMSettingsResponse)
async def put_llm_settings(body: LLMSettingsPutRequest = Body(...)):
    path = dotenv_path(workdir=WORKDIR)
    patch = LLMDefaults(**body.llm.model_dump())
    updates = llm_env_updates_from_defaults_patch(patch, fields_set=set(body.llm.model_fields_set))
    try:
        if updates:
            update_llm_defaults(path, updates=updates)
        defaults = read_llm_defaults(path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return LLMSettingsResponse(llm=_llm_settings_from_defaults(defaults))


@app.post("/api/v1/jobs", response_model=JobCreateResponse, status_code=201)
async def create_job(file: UploadFile = File(...), options: str = Form(...)):
    if file is None:
        raise HTTPException(status_code=400, detail="file is required")
    opts = _parse_options_json(options)

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

    try:
        await _write_input_cache_from_upload(job.job_id, file, limit=MAX_UPLOAD_BYTES)
    except Exception as e:
        GLOBAL_JOBS.delete(job.job_id)
        raise HTTPException(status_code=500, detail=f"failed to cache input: {e}") from e

    fmt = _format_from_options(opts.format)
    llm = _llm_from_options(opts.llm)

    GLOBAL_JOBS.update(job.job_id, phase="validate", format=fmt, last_llm_model=llm.model)
    submit_background_job(job.job_id, run_job, job.job_id, _input_cache_path(job.job_id), fmt, llm)

    st = GLOBAL_JOBS.get(job.job_id)
    if st is None:
        raise HTTPException(status_code=500, detail="job store error")
    return JobCreateResponse(job=_job_to_out(st))


@app.post("/api/v1/jobs/{job_id}/rerun-all", response_model=JobCreateResponse, status_code=201)
async def rerun_all(job_id: str, options: JobOptions = Body(...)):
    st0 = GLOBAL_JOBS.get(job_id)
    if st0 is None:
        raise HTTPException(status_code=404, detail="job not found")

    try:
        # Use filesystem copy to avoid pulling the whole input into memory.
        src_cache = _input_cache_path(job_id)
        if not src_cache.exists():
            raise FileNotFoundError(str(src_cache))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="job input cache not found") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    out_name = _derive_output_filename(st0.input_filename or "input.txt", options.output.suffix)

    job = GLOBAL_JOBS.create(st0.input_filename or "input.txt", out_name, total_chunks=0)
    output_abs = OUTPUT_DIR / f"{job.job_id}_{out_name}"
    work_dir = JOBS_DIR / job.job_id
    GLOBAL_JOBS.update(
        job.job_id,
        output_filename=output_abs.name,
        output_path=str(output_abs),
        work_dir=str(work_dir),
        cleanup_debug_dir=bool(options.output.cleanup_debug_dir),
    )

    try:
        _copy_input_cache(job_id, job.job_id)
    except Exception as e:
        GLOBAL_JOBS.delete(job.job_id)
        raise HTTPException(status_code=500, detail=f"failed to cache input: {e}") from e

    fmt = _format_from_options(options.format)
    llm = _llm_from_options(options.llm)

    GLOBAL_JOBS.update(job.job_id, phase="validate", format=fmt, last_llm_model=llm.model)
    submit_background_job(job.job_id, run_job, job.job_id, _input_cache_path(job.job_id), fmt, llm)

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

    allowed_filters = {"all", "pending", "processing", "retrying", "done", "error", "active"}
    chunk_state = str(chunk_state or "all").strip().lower()
    if chunk_state not in allowed_filters:
        chunk_state = "all"

    chunk_counts: dict[str, int] = {}
    matched = 0
    has_more = False
    out_chunks: list[ChunkOut] = []
    for c in st.chunk_statuses:
        chunk_counts[c.state] = chunk_counts.get(c.state, 0) + 1

        if chunk_state == "active":
            if c.state not in {"processing", "retrying"}:
                continue
        elif chunk_state != "all" and c.state != chunk_state:
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


@app.get("/api/v1/jobs", response_model=JobListResponse)
async def list_jobs(
    *,
    state: str = Query(""),
    phase: str = Query(""),
    limit: int = Query(50, ge=0, le=500),
    offset: int = Query(0, ge=0),
    include_cancelled: int = Query(0, ge=0, le=1),
):
    wanted_states = {s.strip().lower() for s in str(state or "").split(",") if s.strip()}
    wanted_phases = {s.strip().lower() for s in str(phase or "").split(",") if s.strip()}

    jobs = GLOBAL_JOBS.list()
    out: list[JobSummaryOut] = []
    for st in jobs:
        if not include_cancelled and st.state == "cancelled":
            continue
        if wanted_states and st.state.lower() not in wanted_states:
            continue
        st_phase = str(getattr(st, "phase", "") or "").lower()
        if wanted_phases and st_phase not in wanted_phases:
            continue
        out.append(
            JobSummaryOut(
                id=st.job_id,
                state=st.state,
                phase=st_phase or "validate",
                created_at=st.created_at,
                input_filename=st.input_filename,
                output_filename=st.output_filename,
                progress=JobProgress(
                    total_chunks=int(st.total_chunks or 0),
                    done_chunks=int(st.done_chunks or 0),
                    percent=int((st.done_chunks / st.total_chunks) * 100) if st.total_chunks else 0,
                ),
                last_error_code=st.last_error_code,
                llm_model=getattr(st, "last_llm_model", None),
            )
        )

    if offset:
        out = out[offset:]
    if limit:
        out = out[:limit]

    return JobListResponse(jobs=out)


@app.post("/api/v1/jobs/{job_id}/cancel", response_model=JobActionResponse)
async def cancel_job(job_id: str):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    # Recoverable cancel: pause the job (if in-flight) so UI can detach safely and later load/resume.
    if st.state in {"queued", "running"} and not GLOBAL_JOBS.pause(job_id):
        raise HTTPException(status_code=409, detail="failed to pause job")
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
    if getattr(st, "phase", "") == "merge":
        raise HTTPException(status_code=409, detail="job is ready to merge")
    if getattr(st, "phase", "") == "done":
        raise HTTPException(status_code=409, detail="job is already done")
    if not GLOBAL_JOBS.resume(job_id):
        raise HTTPException(status_code=409, detail="failed to resume job")

    llm = _llm_from_options(body.llm or LLMOptions())
    prev_llm_model = getattr(st, "last_llm_model", None)
    GLOBAL_JOBS.update(job_id, last_llm_model=llm.model)
    try:
        if getattr(st, "phase", "") == "validate":
            fmt = getattr(st, "format", FormatConfig())
            submit_background_job(job_id, run_job, job_id, _input_cache_path(job_id), fmt, llm)
        else:
            submit_background_job(job_id, resume_paused_job, job_id, llm)
    except ValueError as e:
        # If the previous runner is still exiting, revert to paused and ask the client to retry later.
        GLOBAL_JOBS.pause(job_id)
        GLOBAL_JOBS.update(job_id, last_llm_model=prev_llm_model)
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        GLOBAL_JOBS.pause(job_id)
        GLOBAL_JOBS.update(job_id, last_llm_model=prev_llm_model)
        raise HTTPException(status_code=500, detail=str(e)) from e
    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/retry-failed", response_model=JobActionResponse)
async def retry_failed(job_id: str, body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if GLOBAL_JOBS.is_cancelled(job_id):
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state == "running":
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == "cancelled":
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state != "error":
        raise HTTPException(status_code=409, detail=f"job is not in error state (state={st.state})")

    failed = [c.index for c in st.chunk_statuses if c.state == "error"]
    if not failed:
        raise HTTPException(status_code=409, detail="no failed chunks to retry")

    llm = _llm_from_options(body.llm or LLMOptions())
    prev_llm_model = getattr(st, "last_llm_model", None)

    # Important: flip the visible job/chunk states before starting the worker thread, otherwise
    # clients may poll and immediately "bounce" back to the error UI state.
    GLOBAL_JOBS.update(job_id, state="queued", phase="process", finished_at=None, error=None, last_llm_model=llm.model)
    for i in failed:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state="pending",
            started_at=None,
            finished_at=None,
        )

    try:
        submit_background_job(job_id, retry_failed_chunks, job_id, llm)
    except ValueError as e:
        # If the previous runner is still exiting, revert to the error state and ask the client to retry later.
        GLOBAL_JOBS.update(
            job_id, state="error", finished_at=st.finished_at, error=st.error, last_llm_model=prev_llm_model
        )
        for i in failed:
            GLOBAL_JOBS.update_chunk(job_id, i, state="error")
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        GLOBAL_JOBS.update(
            job_id, state="error", finished_at=st.finished_at, error=st.error, last_llm_model=prev_llm_model
        )
        for i in failed:
            GLOBAL_JOBS.update_chunk(job_id, i, state="error")
        raise HTTPException(status_code=500, detail=str(e)) from e
    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/merge", response_model=JobActionResponse)
async def merge_job(job_id: str, body: MergeRequest = Body(default_factory=MergeRequest)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if GLOBAL_JOBS.is_cancelled(job_id) or st.state == "cancelled":
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state == "running":
        raise HTTPException(status_code=409, detail="job is running")
    if st.state != "paused":
        raise HTTPException(status_code=409, detail=f"job is not paused (state={st.state})")
    if getattr(st, "phase", "") != "merge":
        raise HTTPException(status_code=409, detail=f"job is not ready to merge (phase={getattr(st, 'phase', None)})")
    if not st.chunk_statuses or any(c.state != "done" for c in st.chunk_statuses):
        raise HTTPException(status_code=409, detail="job is not ready to merge (chunks incomplete)")

    if not GLOBAL_JOBS.resume(job_id):
        raise HTTPException(status_code=409, detail="failed to start merge")

    try:
        submit_background_job(job_id, merge_outputs, job_id, cleanup_debug_dir=body.cleanup_debug_dir)
    except ValueError as e:
        GLOBAL_JOBS.pause(job_id)
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        GLOBAL_JOBS.pause(job_id)
        raise HTTPException(status_code=500, detail=str(e)) from e

    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/reset", response_model=JobActionResponse)
async def reset_job(job_id: str):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")

    # Signal cancellation first so any in-flight LLM calls stop launching new work.
    GLOBAL_JOBS.cancel(job_id)

    def _cleanup_and_delete() -> None:
        try:
            _cleanup_job_dir(job_id)
            _cleanup_input_cache(job_id)
            _cleanup_job_state(job_id)
        except Exception:
            logger.exception("reset cleanup failed: job_id=%s", job_id)
        with suppress(Exception):
            GLOBAL_JOBS.delete(job_id)

    try:
        from novel_proofer.background import add_done_callback as add_done_callback  # local import to avoid cycles

        add_done_callback(job_id, _cleanup_and_delete)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return JobActionResponse(ok=True, job=None)


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
        _cleanup_input_cache(job_id)
        _cleanup_job_state(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    deleted = GLOBAL_JOBS.delete(job_id)
    return JobActionResponse(ok=True, job=_job_to_out(st) if deleted else None)
