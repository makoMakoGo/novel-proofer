from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import time
import traceback
import uuid
from pathlib import Path

from novel_proofer.formatting.chunking import chunk_by_lines
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.jobs import GLOBAL_JOBS
# (Some editors may flag unresolved imports; runtime is OK.)
from novel_proofer.llm.client import LLMError, call_llm_text_resilient_with_meta
from novel_proofer.llm.config import LLMConfig

_DEFAULT_SPLIT_MIN_CHARS = 1_500


def _merge_stats(dst: dict[str, int], src: dict[str, int]) -> None:
    for k, v in src.items():
        dst[k] = dst.get(k, 0) + v


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp name to avoid cross-thread collisions.
    tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _best_effort_cleanup_work_dir(job_id: str, work_dir: Path) -> None:
    try:
        if work_dir.exists():
            shutil.rmtree(work_dir)
            GLOBAL_JOBS.add_stat(job_id, "cleanup_work_dir", 1)
    except Exception:
        GLOBAL_JOBS.add_stat(job_id, "cleanup_work_dir_error", 1)


def _chunk_pre_path(work_dir: Path, index: int) -> Path:
    return work_dir / "pre" / f"{index:06d}.txt"


def _chunk_out_path(work_dir: Path, index: int) -> Path:
    return work_dir / "out" / f"{index:06d}.txt"


def _merge_chunk_outputs(work_dir: Path, total_chunks: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp name to avoid cross-thread collisions.
    tmp = out_path.with_suffix(out_path.suffix + f".{uuid.uuid4().hex}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        for i in range(total_chunks):
            f.write(_chunk_out_path(work_dir, i).read_text(encoding="utf-8"))
    tmp.replace(out_path)


def _split_text_in_half(text: str) -> tuple[str, str]:
    n = len(text)
    if n <= 1:
        return text, ""

    mid = n // 2

    # Prefer splitting at paragraph boundary, but cut AFTER the boundary
    # so the right part doesn't start with blank lines.
    left = text.rfind("\n\n", 0, mid)
    right = text.find("\n\n", mid)

    if left != -1:
        pivot = left + 2
    elif right != -1:
        pivot = right + 2
    else:
        pivot = mid

    pivot = max(1, min(n - 1, pivot))
    return text[:pivot], text[pivot:]


def _llm_process_chunk(cfg: LLMConfig, chunk: str, *, job_id: str | None = None, chunk_index: int | None = None) -> str:
    # If a chunk is too large and errors, we will split it.
    stack: list[str] = [chunk]
    out_parts: list[str] = []

    while stack:
        if job_id is not None and GLOBAL_JOBS.is_cancelled(job_id):
            return ""

        piece = stack.pop()
        if not piece:
            continue

        retry_count = 0

        def on_retry(_retry_index: int, last_code: int | None, last_msg: str | None) -> None:
            nonlocal retry_count
            if job_id is None or chunk_index is None:
                return
            retry_count += 1
            GLOBAL_JOBS.update_chunk(job_id, chunk_index, state="retrying")
            GLOBAL_JOBS.add_retry(job_id, chunk_index, 1, last_code, last_msg)

        try:
            should_stop_fn = None
            if job_id is not None:
                def _should_stop() -> bool:
                    return GLOBAL_JOBS.is_cancelled(job_id)
                should_stop_fn = _should_stop
            out, retries, last_code, last_msg = call_llm_text_resilient_with_meta(
                cfg,
                piece,
                should_stop=should_stop_fn,
                on_retry=on_retry,
            )
            if job_id is not None and chunk_index is not None and retries > retry_count:
                GLOBAL_JOBS.add_retry(job_id, chunk_index, retries - retry_count, last_code, last_msg)

            if job_id is not None and GLOBAL_JOBS.is_cancelled(job_id):
                return ""

            in_trim = len(piece.strip())
            out_trim = len(out.strip())
            if in_trim > 0 and out_trim == 0:
                raise LLMError(
                    "LLM output empty; likely token-limit/stream-parse/think-filter issue",
                    status_code=None,
                )
            if in_trim >= 200 and out_trim < max(200, int(in_trim * 0.2)):
                raise LLMError(
                    f"LLM output too short (in={in_trim}, out={out_trim}); likely token-limit/stream-parse issue",
                    status_code=None,
                )

            out_parts.append(out)
            continue
        except LLMError as e:
            if job_id is not None and GLOBAL_JOBS.is_cancelled(job_id):
                return ""

            # Automatic split on transient/timeouts or 504/503 etc.
            can_split = len(piece) > max(1000, _DEFAULT_SPLIT_MIN_CHARS)
            retryable = (e.status_code in {502, 503, 504, 408, 429, 500} or e.status_code is None)
            if can_split and retryable:
                a, b = _split_text_in_half(piece)
                if job_id is not None and chunk_index is not None:
                    GLOBAL_JOBS.add_split(job_id, chunk_index, 1)

                # Ensure we actually make progress; otherwise fallback to strict mid split.
                if not a or not b or len(a) >= len(piece) or len(b) >= len(piece):
                    mid = len(piece) // 2
                    if mid <= 1 or mid >= len(piece) - 1:
                        raise
                    a, b = piece[:mid], piece[mid:]

                # process a then b in order
                stack.append(b)
                stack.append(a)
                continue
            raise

    return "".join(out_parts)


def _count_done_chunks(job_id: str) -> int:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return 0
    return sum(1 for c in st.chunk_statuses if c.state == "done")


def _llm_request_snapshot(cfg: LLMConfig, input_text: str) -> dict:
    provider = (cfg.provider or "").strip().lower()

    if provider == "gemini":
        url = ""
        if cfg.base_url and cfg.model:
            url = cfg.base_url.rstrip("/") + f"/v1beta/models/{cfg.model}:streamGenerateContent?alt=sse"

        payload: dict = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": cfg.system_prompt + "\n\n" + input_text},
                    ],
                }
            ],
            "generationConfig": {"temperature": cfg.temperature},
        }
        if cfg.extra_params:
            payload.update(cfg.extra_params)

        return {"provider": provider, "url": url, "payload": payload}

    # Default to OpenAI-compatible.
    if provider != "openai_compatible":
        provider = "openai_compatible"

    url = ""
    if cfg.base_url:
        url = cfg.base_url.rstrip("/") + "/v1/chat/completions"

    payload = {
        "model": cfg.model,
        "temperature": cfg.temperature,
        "stream": True,
        "messages": [
            {"role": "system", "content": cfg.system_prompt},
            {"role": "user", "content": input_text},
        ],
    }
    if cfg.extra_params:
        payload.update(cfg.extra_params)

    return {"provider": provider, "url": url, "payload": payload}


def _chunk_req_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "req" / f"{index:06d}_{ts_ms}.json"


def _chunk_resp_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "resp" / f"{index:06d}_{ts_ms}.txt"


def _chunk_err_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "error" / f"{index:06d}_{ts_ms}.json"


def _llm_worker(job_id: str, index: int, work_dir: Path, llm: LLMConfig) -> None:
    if GLOBAL_JOBS.is_cancelled(job_id):
        return

    GLOBAL_JOBS.update_chunk(
        job_id,
        index,
        state="processing",
        started_at=time.time(),
        finished_at=None,
        last_error_code=None,
        last_error_message=None,
        input_chars=None,
        output_chars=None,
    )

    ts_ms = int(time.time() * 1000)

    try:
        pre = _chunk_pre_path(work_dir, index).read_text(encoding="utf-8")
        _atomic_write_text(
            _chunk_req_path(work_dir, index, ts_ms),
            json.dumps(_llm_request_snapshot(llm, pre), ensure_ascii=False, indent=2) + "\n",
        )
        GLOBAL_JOBS.update_chunk(job_id, index, input_chars=len(pre), output_chars=None)
        out = _llm_process_chunk(llm, pre, job_id=job_id, chunk_index=index)
        if GLOBAL_JOBS.is_cancelled(job_id):
            return

        GLOBAL_JOBS.update_chunk(job_id, index, output_chars=len(out))

        _atomic_write_text(_chunk_resp_path(work_dir, index, ts_ms), out)
        _atomic_write_text(_chunk_out_path(work_dir, index), out)
        GLOBAL_JOBS.update_chunk(job_id, index, state="done", finished_at=time.time())
        GLOBAL_JOBS.add_stat(job_id, "llm_chunks", 1)
    except LLMError as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            return
        _atomic_write_text(
            _chunk_err_path(work_dir, index, ts_ms),
            json.dumps(
                {
                    "type": "LLMError",
                    "job_id": job_id,
                    "chunk_index": index,
                    "status_code": e.status_code,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
        )
        GLOBAL_JOBS.update_chunk(
            job_id,
            index,
            state="error",
            finished_at=time.time(),
            last_error_code=e.status_code,
            last_error_message=str(e),
        )
    except Exception as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            return
        _atomic_write_text(
            _chunk_err_path(work_dir, index, ts_ms),
            json.dumps(
                {
                    "type": type(e).__name__,
                    "job_id": job_id,
                    "chunk_index": index,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
        )
        GLOBAL_JOBS.update_chunk(
            job_id,
            index,
            state="error",
            finished_at=time.time(),
            last_error_message=str(e),
        )


def _run_llm_for_indices(job_id: str, indices: list[int], work_dir: Path, llm: LLMConfig) -> None:
    max_workers = max(1, int(llm.max_concurrency))
    GLOBAL_JOBS.update(job_id, state="running", started_at=time.time(), finished_at=None, error=None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        # Submit gradually so cancel can actually stop launching new work.
        pending_indices = list(indices)
        in_flight: dict[concurrent.futures.Future, int] = {}

        while pending_indices or in_flight:
            if GLOBAL_JOBS.is_cancelled(job_id):
                break

            # Fill up the worker pool.
            while pending_indices and len(in_flight) < max_workers and not GLOBAL_JOBS.is_cancelled(job_id):
                i = pending_indices.pop(0)
                fut = ex.submit(_llm_worker, job_id, i, work_dir, llm)
                in_flight[fut] = i

            if not in_flight:
                break

            done, _ = concurrent.futures.wait(in_flight.keys(), timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
            for f in done:
                in_flight.pop(f, None)
                try:
                    f.result()
                except Exception:
                    # Worker is responsible for updating chunk status.
                    pass
                GLOBAL_JOBS.update(job_id, done_chunks=_count_done_chunks(job_id))

        # If cancelled, do not keep queued chunks as 'processing'.
        if GLOBAL_JOBS.is_cancelled(job_id):
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state="pending")
            GLOBAL_JOBS.update(job_id, done_chunks=_count_done_chunks(job_id))


def run_job(job_id: str, input_text: str, fmt: FormatConfig, llm: LLMConfig) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job missing work_dir/output_path")
        return

    work_dir = Path(st.work_dir)
    out_path = Path(st.output_path)
    (work_dir / "pre").mkdir(parents=True, exist_ok=True)
    (work_dir / "out").mkdir(parents=True, exist_ok=True)

    GLOBAL_JOBS.update(job_id, state="running", started_at=time.time(), finished_at=None, error=None)

    try:
        chunks = chunk_by_lines(input_text, max_chars=max(2_000, int(fmt.max_chunk_chars)))
        total = len(chunks)
        GLOBAL_JOBS.init_chunks(job_id, total_chunks=total)

        local_stats: dict[str, int] = {}
        for i, c in enumerate(chunks):
            if GLOBAL_JOBS.is_cancelled(job_id):
                GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                return

            fixed, s = apply_rules(c, fmt)
            _atomic_write_text(_chunk_pre_path(work_dir, i), fixed)
            _merge_stats(local_stats, s)

        for k, v in local_stats.items():
            GLOBAL_JOBS.add_stat(job_id, k, v)

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        if llm.enabled:
            _run_llm_for_indices(job_id, list(range(total)), work_dir, llm)
        else:
            # Pure local mode: treat local output as final chunk output.
            for i in range(total):
                if GLOBAL_JOBS.is_cancelled(job_id):
                    GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                    return
                pre = _chunk_pre_path(work_dir, i).read_text(encoding="utf-8")
                _atomic_write_text(_chunk_out_path(work_dir, i), pre)
                GLOBAL_JOBS.update_chunk(job_id, i, state="done", finished_at=time.time())
            GLOBAL_JOBS.update(job_id, done_chunks=total)

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        cur = GLOBAL_JOBS.get(job_id)
        if cur is None:
            return

        has_error = any(c.state == "error" for c in cur.chunk_statuses)
        if has_error:
            GLOBAL_JOBS.update(
                job_id,
                state="error",
                finished_at=time.time(),
                error="some chunks failed; update LLM config and retry failed chunks",
                done_chunks=_count_done_chunks(job_id),
            )
            return

        _merge_chunk_outputs(work_dir, total, out_path)

        final_stats = dict(cur.stats)
        GLOBAL_JOBS.update(
            job_id,
            state="done",
            finished_at=time.time(),
            stats=final_stats,
            done_chunks=total,
        )
        _best_effort_cleanup_work_dir(job_id, work_dir)
    except Exception as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error=str(e))


def retry_failed_chunks(job_id: str, llm: LLMConfig) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job missing work_dir/output_path")
        return

    work_dir = Path(st.work_dir)
    out_path = Path(st.output_path)

    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job has no chunk statuses")
        return

    total = len(st.chunk_statuses)
    failed = [c.index for c in st.chunk_statuses if c.state == "error"]
    if not failed:
        has_output = out_path.exists()
        if has_output:
            GLOBAL_JOBS.update(job_id, state="done", finished_at=time.time(), done_chunks=total)
        return

    for i in failed:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state="pending",
            started_at=None,
            finished_at=None,
            last_error_code=None,
            last_error_message=None,
            input_chars=None,
            output_chars=None,
        )

    _run_llm_for_indices(job_id, failed, work_dir, llm)

    if GLOBAL_JOBS.is_cancelled(job_id):
        GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
        return

    cur = GLOBAL_JOBS.get(job_id)
    if cur is None:
        return

    has_error = any(c.state == "error" for c in cur.chunk_statuses)
    if has_error:
        GLOBAL_JOBS.update(
            job_id,
            state="error",
            finished_at=time.time(),
            error="some chunks still failed; update LLM config and retry again",
            done_chunks=_count_done_chunks(job_id),
        )
        return

    _merge_chunk_outputs(work_dir, total, out_path)
    GLOBAL_JOBS.update(
        job_id,
        state="done",
        finished_at=time.time(),
        done_chunks=total,
    )
    _best_effort_cleanup_work_dir(job_id, work_dir)
