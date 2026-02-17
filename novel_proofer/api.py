from __future__ import annotations

import codecs
import json
import logging
import os
import re
import shutil
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
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
from novel_proofer.states import ChunkState, JobPhase, JobState

logger = logging.getLogger(__name__)

WORKDIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = WORKDIR / "templates"
IMAGES_DIR = WORKDIR / "images"

OUTPUT_DIR = WORKDIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
JOBS_DIR = OUTPUT_DIR / ".jobs"
JOBS_DIR.mkdir(exist_ok=True)

_JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_INTERNAL_ERROR_MESSAGE = "internal server error"


def _validate_job_id(job_id: str) -> str:
    job_id = str(job_id or "").strip().lower()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job_id")
    return job_id


def _job_id_dep(job_id: str) -> str:
    try:
        return _validate_job_id(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


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
    job_id = _validate_job_id(job_id)
    return _input_cache_root() / f"{job_id}.txt"


def _write_input_cache(job_id: str, text: str) -> None:
    p = _input_cache_path(job_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _input_upload_tmp_path(job_id: str) -> Path:
    job_id = _validate_job_id(job_id)
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

    job_id = _validate_job_id(job_id)

    root = _input_cache_root().resolve()
    target = (_input_cache_root() / f"{job_id}.txt").resolve()
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

    job_id = _validate_job_id(job_id)

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

    job_id = _validate_job_id(job_id)

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
    request_id: str | None = None


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


def _count_non_whitespace_chars_from_utf8_file(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            n += sum(1 for ch in chunk if not ch.isspace())
    return n


def _job_to_out(st: JobStatus) -> JobOut:
    pct = 0
    if st.total_chunks > 0:
        pct = int((st.done_chunks / st.total_chunks) * 100)

    output_path = None
    if st.state == JobState.DONE and st.output_path:
        output_path = _rel_output_path(Path(st.output_path))

    fmt = st.format
    return JobOut(
        id=st.job_id,
        state=st.state,
        phase=st.phase,
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


class _JobCommandService:
    @staticmethod
    def prepare_new_job(
        *,
        input_filename: str,
        suffix: str,
        cleanup_debug_dir: bool,
    ) -> str:
        source_name = input_filename or "input.txt"
        out_name = _derive_output_filename(source_name, suffix)
        job = GLOBAL_JOBS.create(source_name, out_name, total_chunks=0)
        output_abs = OUTPUT_DIR / f"{job.job_id}_{out_name}"
        work_dir = JOBS_DIR / job.job_id
        GLOBAL_JOBS.update(
            job.job_id,
            output_filename=output_abs.name,
            output_path=str(output_abs),
            work_dir=str(work_dir),
            cleanup_debug_dir=bool(cleanup_debug_dir),
        )
        return job.job_id

    @staticmethod
    def get_job_or_500(job_id: str) -> JobStatus:
        st = GLOBAL_JOBS.get(job_id)
        if st is None:
            raise HTTPException(status_code=500, detail="job store error")
        return st

    @staticmethod
    def cleanup_failed_new_job(job_id: str) -> None:
        with suppress(Exception):
            _cleanup_job_dir(job_id)
        with suppress(Exception):
            _cleanup_input_cache(job_id)
        with suppress(Exception):
            _cleanup_job_state(job_id)
        with suppress(Exception):
            GLOBAL_JOBS.delete(job_id)

    @staticmethod
    def submit_background(
        *,
        job_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        on_submit_failure: Callable[[], None] | None = None,
    ) -> None:
        try:
            submit_background_job(job_id, fn, *args, **(kwargs or {}))
        except ValueError as e:
            if on_submit_failure is not None:
                on_submit_failure()
            raise HTTPException(status_code=409, detail=str(e)) from e
        except Exception as e:
            if on_submit_failure is not None:
                on_submit_failure()
            raise HTTPException(status_code=500, detail=str(e)) from e

    @staticmethod
    def queue_validate_run(
        *,
        job_id: str,
        fmt: FormatConfig,
        llm: LLMConfig,
        on_submit_failure: Callable[[], None] | None = None,
    ) -> None:
        GLOBAL_JOBS.update(job_id, phase=JobPhase.VALIDATE, format=fmt, last_llm_model=llm.model)
        _JobCommandService.submit_background(
            job_id=job_id,
            fn=run_job,
            args=(job_id, _input_cache_path(job_id), fmt, llm),
            on_submit_failure=on_submit_failure,
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

    GLOBAL_JOBS.shutdown_persistence(wait=False)
    shutdown_background(wait=False)


app = FastAPI(lifespan=_lifespan)


@app.middleware("http")
async def _request_id_middleware(request: Request, call_next):
    request_id = _request_id_from_request(request)
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# Mount static files for images
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")
app.mount("/static", StaticFiles(directory=str(TEMPLATES_DIR / "static")), name="static")


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    request_id = _request_id_from_request(request)
    status_code = int(exc.status_code)
    message = str(exc.detail)
    if status_code >= 500:
        logger.error("http error response: request_id=%s status=%s detail=%s", request_id, status_code, message)
        message = _INTERNAL_ERROR_MESSAGE
    return _error(status_code, message, request_id=request_id)


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    request_id = _request_id_from_request(request)
    msg = "bad request"
    try:
        errors = exc.errors()
        if errors:
            msg = errors[0].get("msg") or msg
    except Exception:
        pass
    return _error(400, msg, request_id=request_id)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    request_id = _request_id_from_request(request)
    logger.exception("unhandled exception: request_id=%s", request_id)
    return _error(500, _INTERNAL_ERROR_MESSAGE, request_id=request_id)


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
    job_id = _JobCommandService.prepare_new_job(
        input_filename=file.filename or "input.txt",
        suffix=opts.output.suffix,
        cleanup_debug_dir=bool(opts.output.cleanup_debug_dir),
    )

    try:
        await _write_input_cache_from_upload(job_id, file, limit=MAX_UPLOAD_BYTES)
    except Exception as e:
        _JobCommandService.cleanup_failed_new_job(job_id)
        raise HTTPException(status_code=500, detail=f"failed to cache input: {e}") from e

    fmt = _format_from_options(opts.format)
    llm = _llm_from_options(opts.llm)

    _JobCommandService.queue_validate_run(
        job_id=job_id,
        fmt=fmt,
        llm=llm,
        on_submit_failure=lambda: _JobCommandService.cleanup_failed_new_job(job_id),
    )

    st = _JobCommandService.get_job_or_500(job_id)
    return JobCreateResponse(job=_job_to_out(st))


@app.post("/api/v1/jobs/{job_id}/rerun-all", response_model=JobCreateResponse, status_code=201)
async def rerun_all(job_id: str = Depends(_job_id_dep), options: JobOptions = Body(...)):
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

    new_job_id = _JobCommandService.prepare_new_job(
        input_filename=st0.input_filename or "input.txt",
        suffix=options.output.suffix,
        cleanup_debug_dir=bool(options.output.cleanup_debug_dir),
    )

    try:
        _copy_input_cache(job_id, new_job_id)
    except Exception as e:
        _JobCommandService.cleanup_failed_new_job(new_job_id)
        raise HTTPException(status_code=500, detail=f"failed to cache input: {e}") from e

    fmt = _format_from_options(options.format)
    llm = _llm_from_options(options.llm)

    _JobCommandService.queue_validate_run(
        job_id=new_job_id,
        fmt=fmt,
        llm=llm,
        on_submit_failure=lambda: _JobCommandService.cleanup_failed_new_job(new_job_id),
    )

    st = _JobCommandService.get_job_or_500(new_job_id)
    return JobCreateResponse(job=_job_to_out(st))


@app.get("/api/v1/jobs/{job_id}", response_model=JobGetResponse)
async def get_job(
    job_id: str = Depends(_job_id_dep),
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

    allowed_filters = {
        "all",
        ChunkState.PENDING,
        ChunkState.PROCESSING,
        ChunkState.RETRYING,
        ChunkState.DONE,
        ChunkState.ERROR,
        "active",
    }
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
            if c.state not in {ChunkState.PROCESSING, ChunkState.RETRYING}:
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


@app.get("/api/v1/jobs/{job_id}/input-stats", response_model=InputStatsOut)
async def get_job_input_stats(job_id: str = Depends(_job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")

    try:
        p = _input_cache_path(job_id)
        resolved = p.resolve()
        root = _input_cache_root().resolve()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    if resolved != root and root not in resolved.parents:
        raise HTTPException(status_code=400, detail="invalid input cache path")

    try:
        if resolved.exists():
            chars = _count_non_whitespace_chars_from_utf8_file(resolved)
        else:
            work_dir = st.work_dir or str(JOBS_DIR / st.job_id)

            try:
                job_root = JOBS_DIR.resolve()
                resolved_work_dir = Path(work_dir).resolve()
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e)) from e

            if resolved_work_dir != job_root and job_root not in resolved_work_dir.parents:
                raise HTTPException(status_code=400, detail="invalid job work_dir")

            pre_dir = resolved_work_dir / "pre"
            if not pre_dir.exists():
                raise HTTPException(status_code=404, detail="job input cache not found")

            chars = 0
            for fp in sorted(pre_dir.glob("*.txt")):
                chars += _count_non_whitespace_chars_from_utf8_file(fp)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e)) from e

    return InputStatsOut(job_id=st.job_id, input_chars=int(chars))


@app.get("/api/v1/jobs/{job_id}/download")
async def download_job_output(job_id: str = Depends(_job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state != JobState.DONE:
        raise HTTPException(status_code=409, detail="job is not done")
    if not st.output_path:
        raise HTTPException(status_code=404, detail="job output missing")

    out_path = Path(st.output_path)
    try:
        resolved = out_path.resolve()
        out_root = OUTPUT_DIR.resolve()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    if resolved != out_root and out_root not in resolved.parents:
        raise HTTPException(status_code=400, detail="invalid output path")
    if not resolved.exists():
        raise HTTPException(status_code=404, detail="output file not found")

    filename = st.output_filename or resolved.name
    return FileResponse(str(resolved), filename=filename, media_type="text/plain; charset=utf-8")


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
        if not include_cancelled and st.state == JobState.CANCELLED:
            continue
        if wanted_states and st.state.lower() not in wanted_states:
            continue
        st_phase = st.phase.lower()
        if wanted_phases and st_phase not in wanted_phases:
            continue
        out.append(
            JobSummaryOut(
                id=st.job_id,
                state=st.state,
                phase=st_phase or JobPhase.VALIDATE,
                created_at=st.created_at,
                input_filename=st.input_filename,
                output_filename=st.output_filename,
                progress=JobProgress(
                    total_chunks=int(st.total_chunks or 0),
                    done_chunks=int(st.done_chunks or 0),
                    percent=int((st.done_chunks / st.total_chunks) * 100) if st.total_chunks else 0,
                ),
                last_error_code=st.last_error_code,
                llm_model=st.last_llm_model,
            )
        )

    if offset:
        out = out[offset:]
    if limit:
        out = out[:limit]

    return JobListResponse(jobs=out)


@app.post("/api/v1/jobs/{job_id}/pause", response_model=JobActionResponse)
async def pause_job(job_id: str = Depends(_job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    phase = st.phase.strip().lower()
    if phase != JobPhase.PROCESS:
        raise HTTPException(status_code=409, detail=f"cannot pause job in phase={phase or None}")
    if not GLOBAL_JOBS.pause(job_id):
        raise HTTPException(status_code=409, detail=f"cannot pause job in state={st.state}")
    st2 = GLOBAL_JOBS.get(job_id) or st
    return JobActionResponse(ok=True, job=_job_to_out(st2))


@app.post("/api/v1/jobs/{job_id}/resume", response_model=JobActionResponse)
async def resume_job(
    job_id: str = Depends(_job_id_dep), body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)
):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state == JobState.RUNNING:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == JobState.CANCELLED:
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state != JobState.PAUSED:
        raise HTTPException(status_code=409, detail="job is not paused")
    if st.phase == JobPhase.MERGE:
        raise HTTPException(status_code=409, detail="job is ready to merge")
    if st.phase == JobPhase.DONE:
        raise HTTPException(status_code=409, detail="job is already done")
    if not GLOBAL_JOBS.resume(job_id):
        raise HTTPException(status_code=409, detail="failed to resume job")

    llm = _llm_from_options(body.llm or LLMOptions())
    prev_llm_model = st.last_llm_model
    GLOBAL_JOBS.update(job_id, last_llm_model=llm.model)
    phase = st.phase
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    if phase == JobPhase.VALIDATE:
        fmt = st.format
        fn = run_job
        args = (job_id, _input_cache_path(job_id), fmt, llm)
    else:
        fn = resume_paused_job
        args = (job_id, llm)

    def _rollback_resume_state() -> None:
        GLOBAL_JOBS.pause(job_id)
        GLOBAL_JOBS.update(job_id, last_llm_model=prev_llm_model)

    _JobCommandService.submit_background(
        job_id=job_id,
        fn=fn,
        args=args,
        on_submit_failure=_rollback_resume_state,
    )

    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/retry-failed", response_model=JobActionResponse)
async def retry_failed(
    job_id: str = Depends(_job_id_dep), body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)
):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if GLOBAL_JOBS.is_cancelled(job_id):
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state == JobState.RUNNING:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == JobState.CANCELLED:
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state != JobState.ERROR:
        raise HTTPException(status_code=409, detail=f"job is not in error state (state={st.state})")

    failed = [c.index for c in st.chunk_statuses if c.state == ChunkState.ERROR]
    if not failed:
        raise HTTPException(status_code=409, detail="no failed chunks to retry")

    llm = _llm_from_options(body.llm or LLMOptions())
    prev_llm_model = st.last_llm_model

    # Important: flip the visible job/chunk states before starting the worker thread, otherwise
    # clients may poll and immediately "bounce" back to the error UI state.
    GLOBAL_JOBS.update(
        job_id, state=JobState.QUEUED, phase=JobPhase.PROCESS, finished_at=None, error=None, last_llm_model=llm.model
    )
    for i in failed:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state=ChunkState.PENDING,
            started_at=None,
            finished_at=None,
        )

    def _rollback_retry_state() -> None:
        GLOBAL_JOBS.update(
            job_id, state=JobState.ERROR, finished_at=st.finished_at, error=st.error, last_llm_model=prev_llm_model
        )
        for i in failed:
            GLOBAL_JOBS.update_chunk(job_id, i, state=ChunkState.ERROR)

    _JobCommandService.submit_background(
        job_id=job_id,
        fn=retry_failed_chunks,
        args=(job_id, llm),
        on_submit_failure=_rollback_retry_state,
    )

    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/merge", response_model=JobActionResponse)
async def merge_job(job_id: str = Depends(_job_id_dep), body: MergeRequest = Body(default_factory=MergeRequest)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if GLOBAL_JOBS.is_cancelled(job_id) or st.state == JobState.CANCELLED:
        raise HTTPException(status_code=409, detail="job is cancelled")
    if st.state == JobState.RUNNING:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state != JobState.PAUSED:
        raise HTTPException(status_code=409, detail=f"job is not paused (state={st.state})")
    if st.phase != JobPhase.MERGE:
        raise HTTPException(status_code=409, detail=f"job is not ready to merge (phase={st.phase})")
    if not st.chunk_statuses or any(c.state != ChunkState.DONE for c in st.chunk_statuses):
        raise HTTPException(status_code=409, detail="job is not ready to merge (chunks incomplete)")

    if not GLOBAL_JOBS.resume(job_id):
        raise HTTPException(status_code=409, detail="failed to start merge")

    def _rollback_merge_state() -> None:
        GLOBAL_JOBS.pause(job_id)

    _JobCommandService.submit_background(
        job_id=job_id,
        fn=merge_outputs,
        args=(job_id,),
        kwargs={"cleanup_debug_dir": body.cleanup_debug_dir},
        on_submit_failure=_rollback_merge_state,
    )

    return JobActionResponse(ok=True, job=_job_to_out(GLOBAL_JOBS.get(job_id) or st))


@app.post("/api/v1/jobs/{job_id}/reset", response_model=JobActionResponse)
async def reset_job(job_id: str = Depends(_job_id_dep)):
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
async def cleanup_debug(job_id: str = Depends(_job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state in {JobState.QUEUED, JobState.RUNNING}:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == JobState.CANCELLED:
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
