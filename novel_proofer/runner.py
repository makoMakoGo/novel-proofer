from __future__ import annotations

import concurrent.futures
import json
import shutil
import time
import traceback
import uuid
from pathlib import Path

from novel_proofer.formatting.chunking import chunk_by_lines
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMError, call_llm_text_resilient_with_meta_and_raw
from novel_proofer.llm.config import LLMConfig


_JOB_DEBUG_README = """\
本目录为 novel-proofer 的单次任务调试产物，用于排查与重试。

目录说明：
- pre/   : 本地规则处理后的分片输入（发送给 LLM 的输入），文件名为分片 index（固定覆盖）。
- out/   : 分片最终输出（通过校验，参与合并），文件名为分片 index（固定覆盖）。
- req/   : 每次请求 LLM 的请求快照 JSON（含 provider/url/payload），文件名带时间戳（不会覆盖）。
- resp/  : LLM 响应留档（原始 raw / 过滤后 filtered），文件名带时间戳（不会覆盖）。
- error/ : 结构化错误详情 JSON（含 traceback 与关联文件名），文件名带时间戳（不会覆盖）。

文件命名：
- pre/{index}.txt、out/{index}.txt 会被覆盖（以当前最新为准）。
- req/resp/error 目录下文件名包含 {index}_{timestamp}，用于保留多次尝试历史。
"""


def _ensure_job_debug_readme(work_dir: Path) -> None:
    p = work_dir / "README.txt"
    if p.exists():
        return
    _atomic_write_text(p, _JOB_DEBUG_README)


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


def _should_cleanup_debug_dir(job_id: str) -> bool:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return True
    return bool(getattr(st, "cleanup_debug_dir", True))


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


def _finalize_job(job_id: str, work_dir: Path, out_path: Path, total: int, error_msg: str) -> bool:
    """Check job status after LLM processing and finalize output.

    Returns True if job completed successfully, False if there were errors.
    """
    if GLOBAL_JOBS.is_cancelled(job_id):
        GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
        return False

    cur = GLOBAL_JOBS.get(job_id)
    if cur is None:
        return False

    has_error = any(c.state == "error" for c in cur.chunk_statuses)
    if has_error:
        GLOBAL_JOBS.update(
            job_id,
            state="error",
            finished_at=time.time(),
            error=error_msg,
            done_chunks=_count_done_chunks(job_id),
        )
        return False

    _merge_chunk_outputs(work_dir, total, out_path)

    final_stats = dict(cur.stats)
    GLOBAL_JOBS.update(
        job_id,
        state="done",
        finished_at=time.time(),
        stats=final_stats,
        done_chunks=total,
    )
    if _should_cleanup_debug_dir(job_id):
        _best_effort_cleanup_work_dir(job_id, work_dir)
    else:
        GLOBAL_JOBS.add_stat(job_id, "cleanup_work_dir_skipped", 1)
    return True


def _validate_llm_output(input_text: str, output_text: str) -> None:
    in_len = len(input_text)
    out_len = len(output_text)
    out_trim = len(output_text.strip())
    if in_len > 0 and out_trim == 0:
        raise LLMError(
            "LLM output empty; likely token-limit/stream-parse/think-filter issue",
            status_code=None,
        )
    if in_len >= 200 and in_len > 0:
        ratio = out_len / in_len
        if ratio < 0.85:
            raise LLMError(
                f"LLM output too short (in={in_len}, out={out_len}, ratio={ratio:.2f} < 0.85); "
                "possible content filtering / token-limit / stream truncation",
                status_code=None,
            )
        if ratio > 1.15:
            raise LLMError(
                f"LLM output too long (in={in_len}, out={out_len}, ratio={ratio:.2f} > 1.15); "
                "possible repetition / hallucination",
                status_code=None,
            )


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


def _chunk_resp_raw_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "resp" / f"{index:06d}_{ts_ms}_raw.txt"


def _chunk_resp_filtered_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "resp" / f"{index:06d}_{ts_ms}_filtered.txt"


def _chunk_resp_stream_path(work_dir: Path, index: int, ts_ms: int) -> Path:
    return work_dir / "resp" / f"{index:06d}_{ts_ms}_stream.txt"


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

    raw_text: str | None = None
    filtered_text: str | None = None
    stream_debug: str | None = None

    try:
        pre = _chunk_pre_path(work_dir, index).read_text(encoding="utf-8")
        req_path = _chunk_req_path(work_dir, index, ts_ms)
        _atomic_write_text(req_path, json.dumps(_llm_request_snapshot(llm, pre), ensure_ascii=False, indent=2) + "\n")
        GLOBAL_JOBS.update_chunk(job_id, index, input_chars=len(pre), output_chars=None)

        retry_count = 0

        def on_retry(_retry_index: int, last_code: int | None, last_msg: str | None) -> None:
            nonlocal retry_count
            retry_count += 1
            GLOBAL_JOBS.update_chunk(job_id, index, state="retrying")
            GLOBAL_JOBS.add_retry(job_id, index, 1, last_code, last_msg)

        def _should_stop() -> bool:
            return GLOBAL_JOBS.is_cancelled(job_id)

        result, retries, last_code, last_msg = call_llm_text_resilient_with_meta_and_raw(
            llm,
            pre,
            should_stop=_should_stop,
            on_retry=on_retry,
        )
        raw_text = result.raw_text
        filtered_text = result.text
        stream_debug = result.stream_debug

        if retries > retry_count:
            GLOBAL_JOBS.add_retry(job_id, index, retries - retry_count, last_code, last_msg)

        if GLOBAL_JOBS.is_cancelled(job_id):
            return

        assert filtered_text is not None
        GLOBAL_JOBS.update_chunk(job_id, index, output_chars=len(filtered_text))

        _atomic_write_text(_chunk_resp_raw_path(work_dir, index, ts_ms), raw_text or "")
        _atomic_write_text(_chunk_resp_filtered_path(work_dir, index, ts_ms), filtered_text)
        if (raw_text or "").strip() == "" and stream_debug:
            _atomic_write_text(_chunk_resp_stream_path(work_dir, index, ts_ms), stream_debug)

        _validate_llm_output(pre, filtered_text)

        _atomic_write_text(_chunk_out_path(work_dir, index), filtered_text)
        GLOBAL_JOBS.update_chunk(job_id, index, state="done", finished_at=time.time())
        GLOBAL_JOBS.add_stat(job_id, "llm_chunks", 1)
    except LLMError as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            return
        req_rel = str(_chunk_req_path(work_dir, index, ts_ms).relative_to(work_dir)).replace("\\", "/")
        pre_rel = str(_chunk_pre_path(work_dir, index).relative_to(work_dir)).replace("\\", "/")
        resp_raw_rel = str(_chunk_resp_raw_path(work_dir, index, ts_ms).relative_to(work_dir)).replace("\\", "/")
        resp_filtered_rel = str(_chunk_resp_filtered_path(work_dir, index, ts_ms).relative_to(work_dir)).replace("\\", "/")
        resp_stream_rel = str(_chunk_resp_stream_path(work_dir, index, ts_ms).relative_to(work_dir)).replace("\\", "/")
        out_rel = str(_chunk_out_path(work_dir, index).relative_to(work_dir)).replace("\\", "/")
        _atomic_write_text(
            _chunk_err_path(work_dir, index, ts_ms),
            json.dumps(
                {
                    "type": "LLMError",
                    "job_id": job_id,
                    "chunk_index": index,
                    "status_code": e.status_code,
                    "message": str(e),
                    "files": {
                        "pre": pre_rel,
                        "req": req_rel,
                        "resp_raw": resp_raw_rel,
                        "resp_filtered": resp_filtered_rel,
                        "resp_stream": resp_stream_rel,
                        "out": out_rel,
                    },
                    "raw_excerpt": (raw_text or "")[:400],
                    "filtered_excerpt": (filtered_text or "")[:400],
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


def _run_llm_for_indices(job_id: str, indices: list[int], work_dir: Path, llm: LLMConfig) -> str:
    max_workers = max(1, int(llm.max_concurrency))
    GLOBAL_JOBS.update(job_id, state="running", started_at=time.time(), finished_at=None, error=None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        # Submit gradually so cancel can actually stop launching new work.
        pending_indices = list(indices)
        in_flight: dict[concurrent.futures.Future, int] = {}

        while pending_indices or in_flight:
            if GLOBAL_JOBS.is_cancelled(job_id):
                break
            paused = GLOBAL_JOBS.is_paused(job_id)
            if paused and not in_flight:
                break

            if not paused:
                # Fill up the worker pool.
                while (
                    pending_indices
                    and len(in_flight) < max_workers
                    and not GLOBAL_JOBS.is_cancelled(job_id)
                    and not GLOBAL_JOBS.is_paused(job_id)
                ):
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
            return "cancelled"

        if GLOBAL_JOBS.is_paused(job_id) and pending_indices:
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state="pending")
            GLOBAL_JOBS.update(job_id, done_chunks=_count_done_chunks(job_id))
            return "paused"

    return "done"


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
    _ensure_job_debug_readme(work_dir)

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
            outcome = _run_llm_for_indices(job_id, list(range(total)), work_dir, llm)
            if outcome == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
                GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                return
            if outcome == "paused" or GLOBAL_JOBS.is_paused(job_id):
                GLOBAL_JOBS.update(job_id, state="paused", finished_at=None)
                return
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

        _finalize_job(job_id, work_dir, out_path, total, "some chunks failed; update LLM config and retry failed chunks")
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
    _ensure_job_debug_readme(work_dir)

    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job has no chunk statuses")
        return

    total = len(st.chunk_statuses)
    failed = [c.index for c in st.chunk_statuses if c.state == "error"]
    if not failed:
        if out_path.exists():
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

    outcome = _run_llm_for_indices(job_id, failed, work_dir, llm)
    if outcome == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
        return
    if outcome == "paused" or GLOBAL_JOBS.is_paused(job_id):
        GLOBAL_JOBS.update(job_id, state="paused", finished_at=None)
        return
    _finalize_job(job_id, work_dir, out_path, total, "some chunks still failed; update LLM config and retry again")


def resume_paused_job(job_id: str, llm: LLMConfig) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job missing work_dir/output_path")
        return
    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error="job has no chunk statuses")
        return

    work_dir = Path(st.work_dir)
    out_path = Path(st.output_path)
    _ensure_job_debug_readme(work_dir)

    total = len(st.chunk_statuses)
    pending = [c.index for c in st.chunk_statuses if c.state not in {"done", "error"}]
    if not pending:
        _finalize_job(job_id, work_dir, out_path, total, "some chunks failed; update LLM config and retry failed chunks")
        return

    for i in pending:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state="pending",
            started_at=None,
            finished_at=None,
        )

    outcome = _run_llm_for_indices(job_id, pending, work_dir, llm)
    if outcome == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
        return
    if outcome == "paused" or GLOBAL_JOBS.is_paused(job_id):
        GLOBAL_JOBS.update(job_id, state="paused", finished_at=None)
        return

    _finalize_job(job_id, work_dir, out_path, total, "some chunks failed; update LLM config and retry failed chunks")
