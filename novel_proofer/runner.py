from __future__ import annotations

import concurrent.futures
import threading
import time
from pathlib import Path

from novel_proofer.formatting.chunking import chunk_by_lines
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.jobs import GLOBAL_JOBS
# (Some editors may flag unresolved imports; runtime is OK.)
from novel_proofer.llm.client import LLMError, call_llm_text_resilient_with_meta
from novel_proofer.llm.config import LLMConfig


def _merge_stats(dst: dict[str, int], src: dict[str, int]) -> None:
    for k, v in src.items():
        dst[k] = dst.get(k, 0) + v


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

        try:
            out, retries, last_code, last_msg = call_llm_text_resilient_with_meta(cfg, piece)
            if job_id is not None and chunk_index is not None and retries:
                GLOBAL_JOBS.add_retry(job_id, chunk_index, retries, last_code, last_msg)

            if job_id is not None and GLOBAL_JOBS.is_cancelled(job_id):
                return ""

            out_parts.append(out)
            continue
        except LLMError as e:
            if job_id is not None and chunk_index is not None:
                GLOBAL_JOBS.add_retry(job_id, chunk_index, 1, e.status_code, str(e))

            if job_id is not None and GLOBAL_JOBS.is_cancelled(job_id):
                return ""

            # Automatic split on transient/timeouts or 504/503 etc.
            can_split = len(piece) > max(1000, int(cfg.split_min_chars))
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


def run_job(
    job_id: str,
    input_text: str,
    output_path: Path,
    fmt: FormatConfig,
    llm: LLMConfig,
) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return

    if st.state == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
        return

    GLOBAL_JOBS.update(job_id, state="running", started_at=time.time())

    try:
        chunks = chunk_by_lines(input_text, max_chars=max(2_000, int(fmt.max_chunk_chars)))
        GLOBAL_JOBS.init_chunks(job_id, total_chunks=len(chunks))

        # 先跑本地规则（轻量，保守）
        pre_chunks: list[str] = []
        local_stats: dict[str, int] = {}
        for c in chunks:
            if GLOBAL_JOBS.is_cancelled(job_id):
                GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                return

            fixed, s = apply_rules(c, fmt)
            pre_chunks.append(fixed)
            _merge_stats(local_stats, s)

        # Put local stats into job stats early.
        for k, v in local_stats.items():
            GLOBAL_JOBS.add_stat(job_id, k, v)

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        if llm.enabled:
            max_workers = max(1, int(llm.max_concurrency))
            out_chunks: list[str] = [""] * len(pre_chunks)

            done_count = 0

            def worker(idx: int, text: str) -> bool:
                if GLOBAL_JOBS.is_cancelled(job_id):
                    return False

                GLOBAL_JOBS.update_chunk(job_id, idx, state="running", started_at=time.time())
                try:
                    out = _llm_process_chunk(llm, text, job_id=job_id, chunk_index=idx)
                    if GLOBAL_JOBS.is_cancelled(job_id):
                        return False

                    out_chunks[idx] = out
                    GLOBAL_JOBS.update_chunk(job_id, idx, state="done", finished_at=time.time())
                    GLOBAL_JOBS.add_stat(job_id, "llm_chunks", 1)
                    return True
                except Exception as e:
                    if GLOBAL_JOBS.is_cancelled(job_id):
                        return False
                    GLOBAL_JOBS.update_chunk(
                        job_id,
                        idx,
                        state="error",
                        finished_at=time.time(),
                        last_error_message=str(e),
                    )
                    raise

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                next_idx = 0
                futures: dict[concurrent.futures.Future[bool], int] = {}

                def submit_one(i: int) -> None:
                    futures[ex.submit(worker, i, pre_chunks[i])] = i

                while next_idx < len(pre_chunks) and len(futures) < max_workers and not GLOBAL_JOBS.is_cancelled(job_id):
                    submit_one(next_idx)
                    next_idx += 1

                while futures:
                    done, _ = concurrent.futures.wait(
                        futures,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )

                    for f in done:
                        idx = futures.pop(f)
                        try:
                            ok = f.result()
                        except Exception:
                            if GLOBAL_JOBS.is_cancelled(job_id):
                                ok = False
                            else:
                                raise

                        if ok:
                            done_count += 1
                            GLOBAL_JOBS.update(job_id, done_chunks=done_count)

                        if GLOBAL_JOBS.is_cancelled(job_id):
                            for pending in futures:
                                pending.cancel()
                            break

                        if next_idx < len(pre_chunks):
                            submit_one(next_idx)
                            next_idx += 1

                    if GLOBAL_JOBS.is_cancelled(job_id):
                        break

            if GLOBAL_JOBS.is_cancelled(job_id):
                GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time(), done_chunks=done_count)
                return

            final_text = "".join(out_chunks)
        else:
            final_text = "".join(pre_chunks)

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        output_path.write_text(final_text, encoding="utf-8")

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        cur = GLOBAL_JOBS.get(job_id)
        final_stats = dict(cur.stats) if cur is not None else dict(local_stats)

        GLOBAL_JOBS.update(
            job_id,
            state="done",
            finished_at=time.time(),
            output_path=str(output_path),
            stats=final_stats,
            done_chunks=len(chunks),
        )
    except Exception as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return
        GLOBAL_JOBS.update(job_id, state="error", finished_at=time.time(), error=str(e))
