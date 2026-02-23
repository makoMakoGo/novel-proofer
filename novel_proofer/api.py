from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from novel_proofer import paths
from novel_proofer.background import shutdown as shutdown_background
from novel_proofer.background import submit as submit_background_job
from novel_proofer.converters import (
    _INTERNAL_ERROR_MESSAGE,
    _chunk_to_out,
    _error,
    _format_from_options,
    _job_to_out,
    _llm_from_options,
    _llm_settings_from_defaults,
    _parse_options_json,
    _request_id_from_request,
)
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
from novel_proofer.jobs import GLOBAL_JOBS, JobStatus
from novel_proofer.llm.config import LLMConfig
from novel_proofer.logging_setup import ensure_file_logging
from novel_proofer.models import (
    InputStatsOut,
    JobActionResponse,
    JobCreateResponse,
    JobGetResponse,
    JobListResponse,
    JobOptions,
    JobProgress,
    JobSummaryOut,
    LLMOptions,
    LLMSettingsPutRequest,
    LLMSettingsResponse,
    MergeRequest,
    PurgeAllRequest,
    PurgeAllResponse,
    RetryFailedRequest,
)
from novel_proofer.runner import merge_outputs, resume_paused_job, retry_failed_chunks, run_job
from novel_proofer.states import ChunkState, JobPhase, JobState

logger = logging.getLogger(__name__)


class _JobCommandService:
    @staticmethod
    def prepare_new_job(
        *,
        input_filename: str,
        suffix: str,
        cleanup_debug_dir: bool,
    ) -> str:
        source_name = input_filename or "input.txt"
        out_name = paths._derive_output_filename(source_name, suffix)
        job = GLOBAL_JOBS.create(source_name, out_name, total_chunks=0)
        output_abs = paths.OUTPUT_DIR / f"{job.job_id}_{out_name}"
        work_dir = paths.JOBS_DIR / job.job_id
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
            paths._cleanup_job_dir(job_id)
        with suppress(Exception):
            paths._cleanup_input_cache(job_id)
        with suppress(Exception):
            paths._cleanup_job_state(job_id)
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
            args=(job_id, paths._input_cache_path(job_id), fmt, llm),
            on_submit_failure=on_submit_failure,
        )


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    paths.OUTPUT_DIR.mkdir(exist_ok=True)
    paths.JOBS_DIR.mkdir(parents=True, exist_ok=True)
    paths._input_cache_root().mkdir(parents=True, exist_ok=True)
    paths._jobs_state_root().mkdir(parents=True, exist_ok=True)

    log_file = ensure_file_logging(log_dir=paths.OUTPUT_DIR / "logs")
    logger.info("file logging enabled: %s", log_file)

    GLOBAL_JOBS.configure_persistence(persist_dir=paths._jobs_state_root())
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


app.mount("/images", StaticFiles(directory=str(paths.IMAGES_DIR)), name="images")
app.mount("/static", StaticFiles(directory=str(paths.TEMPLATES_DIR / "static")), name="static")


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
    path = paths.TEMPLATES_DIR / "index.html"
    if not path.exists():
        raise HTTPException(status_code=500, detail="missing templates/index.html")
    return FileResponse(path, media_type="text/html; charset=utf-8")


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/api/v1/settings/llm", response_model=LLMSettingsResponse)
async def get_llm_settings():
    path = dotenv_path(workdir=paths.WORKDIR)
    try:
        defaults = read_llm_defaults(path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return LLMSettingsResponse(llm=_llm_settings_from_defaults(defaults))


@app.put("/api/v1/settings/llm", response_model=LLMSettingsResponse)
async def put_llm_settings(body: LLMSettingsPutRequest = Body(...)):
    path = dotenv_path(workdir=paths.WORKDIR)
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
        await paths._write_input_cache_from_upload(job_id, file, limit=paths.MAX_UPLOAD_BYTES)
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
async def rerun_all(job_id: str = Depends(paths._job_id_dep), options: JobOptions = Body(...)):
    st0 = GLOBAL_JOBS.get(job_id)
    if st0 is None:
        raise HTTPException(status_code=404, detail="job not found")

    try:
        src_cache = paths._input_cache_path(job_id)
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
        paths._copy_input_cache(job_id, new_job_id)
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
    job_id: str = Depends(paths._job_id_dep),
    *,
    chunks: int = Query(1, ge=0, le=1),
    chunk_state: str = Query("all"),
    limit: int = Query(0, ge=0),
    offset: int = Query(0, ge=0),
):
    st = GLOBAL_JOBS.get_summary(job_id)
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

    page = GLOBAL_JOBS.get_chunks_page(job_id, chunk_state=chunk_state, limit=limit, offset=offset)
    if page is None:
        raise HTTPException(status_code=404, detail="job not found")

    chunk_items, chunk_counts, has_more = page
    payload.chunks = [_chunk_to_out(c) for c in chunk_items]
    payload.chunk_counts = chunk_counts
    payload.has_more = bool(has_more)
    return payload


@app.get("/api/v1/jobs/{job_id}/input-stats", response_model=InputStatsOut)
async def get_job_input_stats(job_id: str = Depends(paths._job_id_dep)):
    st = GLOBAL_JOBS.get_summary(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")

    try:
        p = paths._input_cache_path(job_id)
        resolved = p.resolve()
        root = paths._input_cache_root().resolve()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    if resolved != root and root not in resolved.parents:
        raise HTTPException(status_code=400, detail="invalid input cache path")

    try:
        if resolved.exists():
            chars = paths._count_non_whitespace_chars_from_utf8_file(resolved)
        else:
            work_dir = st.work_dir or str(paths.JOBS_DIR / st.job_id)

            try:
                job_root = paths.JOBS_DIR.resolve()
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
                chars += paths._count_non_whitespace_chars_from_utf8_file(fp)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e)) from e

    return InputStatsOut(job_id=st.job_id, input_chars=int(chars))


@app.get("/api/v1/jobs/{job_id}/download")
async def download_job_output(job_id: str = Depends(paths._job_id_dep)):
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
        out_root = paths.OUTPUT_DIR.resolve()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    if resolved != out_root and out_root not in resolved.parents:
        raise HTTPException(status_code=400, detail="invalid output path")
    if not resolved.exists():
        raise HTTPException(status_code=404, detail="output file not found")

    filename = st.output_filename or resolved.name
    return FileResponse(str(resolved), filename=filename, media_type="text/plain; charset=utf-8")


@app.post("/api/v1/jobs/purge-all", response_model=PurgeAllResponse)
async def purge_all_jobs(body: PurgeAllRequest = Body(default_factory=PurgeAllRequest)):
    """Cancel and delete every known job (except excluded), then wipe leftover disk artifacts."""

    from novel_proofer.background import add_done_callback

    exclude_set = set(body.exclude)
    summaries = GLOBAL_JOBS.list_summaries()
    purged = 0

    for st in summaries:
        jid = st.job_id
        if jid in exclude_set:
            continue
        try:
            GLOBAL_JOBS.cancel(jid)

            def _cleanup(job_id: str = jid) -> None:
                with suppress(Exception):
                    paths._cleanup_job_dir(job_id)
                with suppress(Exception):
                    paths._cleanup_input_cache(job_id)
                with suppress(Exception):
                    paths._cleanup_job_state(job_id)
                with suppress(Exception):
                    GLOBAL_JOBS.delete(job_id)

            add_done_callback(jid, _cleanup)
            purged += 1
        except Exception:
            logger.exception("purge-all: failed to process job_id=%s", jid)

    return PurgeAllResponse(ok=True, purged=purged)


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

    jobs = GLOBAL_JOBS.list_summaries()
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
async def pause_job(job_id: str = Depends(paths._job_id_dep)):
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
    job_id: str = Depends(paths._job_id_dep), body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)
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
        args = (job_id, paths._input_cache_path(job_id), fmt, llm)
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
    job_id: str = Depends(paths._job_id_dep), body: RetryFailedRequest = Body(default_factory=RetryFailedRequest)
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
async def merge_job(job_id: str = Depends(paths._job_id_dep), body: MergeRequest = Body(default_factory=MergeRequest)):
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
async def reset_job(job_id: str = Depends(paths._job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")

    GLOBAL_JOBS.cancel(job_id)

    def _cleanup_and_delete() -> None:
        try:
            paths._cleanup_job_dir(job_id)
            paths._cleanup_input_cache(job_id)
            paths._cleanup_job_state(job_id)
        except Exception:
            logger.exception("reset cleanup failed: job_id=%s", job_id)
        with suppress(Exception):
            GLOBAL_JOBS.delete(job_id)

    try:
        from novel_proofer.background import add_done_callback as add_done_callback

        add_done_callback(job_id, _cleanup_and_delete)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return JobActionResponse(ok=True, job=None)


@app.post("/api/v1/jobs/{job_id}/cleanup-debug", response_model=JobActionResponse)
async def cleanup_debug(job_id: str = Depends(paths._job_id_dep)):
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        raise HTTPException(status_code=404, detail="job not found")
    if st.state in {JobState.QUEUED, JobState.RUNNING}:
        raise HTTPException(status_code=409, detail="job is running")
    if st.state == JobState.CANCELLED:
        raise HTTPException(status_code=409, detail="job is cancelled")

    try:
        paths._cleanup_job_dir(job_id)
        paths._cleanup_input_cache(job_id)
        paths._cleanup_job_state(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    deleted = GLOBAL_JOBS.delete(job_id)
    return JobActionResponse(ok=True, job=_job_to_out(st) if deleted else None)
