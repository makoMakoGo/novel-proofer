from __future__ import annotations

import concurrent.futures
import shutil
import time
import uuid
from dataclasses import replace
from pathlib import Path

from novel_proofer.formatting.chunking import chunk_by_lines_with_first_chunk_max
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMError, call_llm_text_resilient_with_meta_and_raw
from novel_proofer.llm.config import FIRST_CHUNK_SYSTEM_PROMPT_PREFIX, LLMConfig


_JOB_DEBUG_README = """\
本目录为 novel-proofer 的单次任务调试产物。

目录说明：
- pre/  : 发送给 LLM 的分片输入
- out/  : 分片最终输出（通过校验）
- resp/ : LLM 原始响应
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


def _normalize_newlines(text: str) -> str:
    if "\r" not in text:
        return text
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _count_trailing_newlines(text: str) -> int:
    n = 0
    for ch in reversed(text):
        if ch != "\n":
            break
        n += 1
    return n


def _align_trailing_newlines(reference: str, text: str, *, max_newlines: int = 3) -> str:
    """Align trailing newlines in `text` to match `reference` (up to max_newlines).

    This helps keep paragraph/chapter boundaries stable when LLM output omits
    trailing blank lines/newlines at chunk boundaries.
    """

    ref = _normalize_newlines(reference)
    out = _normalize_newlines(text)
    want = min(_count_trailing_newlines(ref), max_newlines)
    have = _count_trailing_newlines(out)
    if have == want:
        return out
    base = out.rstrip("\n")
    return base + ("\n" * want)


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


def _chunk_path(work_dir: Path, subdir: str, index: int) -> Path:
    return work_dir / subdir / f"{index:06d}.txt"


def _iter_normalized_lines_for_merge(text: str) -> list[str]:
    """Normalize text into lines for final merge.

    - Normalizes CRLF/CR to LF.
    - Treats whitespace-only lines as blank lines.
    - Trims trailing whitespace on non-blank lines.
    - Preserves explicit blank lines (including multiple) inside the chunk.
    """

    text = _normalize_newlines(text)
    had_trailing_newline = text.endswith("\n")
    lines = text.split("\n")
    if had_trailing_newline and lines:
        # Drop the implicit last empty element created by split("\n") when text endswith "\n".
        lines.pop()

    out: list[str] = []
    for line in lines:
        if line.strip() == "":
            out.append("")
        else:
            out.append(line.rstrip())
    return out


def _merge_chunk_outputs(work_dir: Path, total_chunks: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp name to avoid cross-thread collisions.
    tmp = out_path.with_suffix(out_path.suffix + f".{uuid.uuid4().hex}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        prev_nonblank = False
        for i in range(total_chunks):
            chunk_text = _chunk_path(work_dir, "out", i).read_text(encoding="utf-8")
            lines = _iter_normalized_lines_for_merge(chunk_text)
            is_last_chunk = i == total_chunks - 1
            keep_final_newline = chunk_text.endswith("\n") or chunk_text.endswith("\r")
            last_line_idx = len(lines) - 1
            for j, line in enumerate(lines):
                if line == "":
                    f.write("\n")
                    prev_nonblank = False
                    continue

                if prev_nonblank:
                    # Ensure paragraph separation (blank line) between adjacent non-blank lines,
                    # especially across chunk boundaries.
                    f.write("\n")
                f.write(line)
                if not (is_last_chunk and not keep_final_newline and j == last_line_idx):
                    f.write("\n")
                prev_nonblank = True
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
            done_chunks=cur.done_chunks,
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


def _validate_llm_output(input_text: str, output_text: str, *, allow_shorter: bool = False) -> None:
    in_len = len(input_text)
    out_len = len(output_text)
    out_trim = len(output_text.strip())
    if in_len > 0 and out_trim == 0:
        raise LLMError(
            "LLM output empty",
            status_code=None,
        )
    if in_len >= 200 and in_len > 0:
        ratio = out_len / in_len
        if ratio < 0.85 and not allow_shorter:
            raise LLMError(
                f"LLM output too short (in={in_len}, out={out_len}, ratio={ratio:.2f} < 0.85)",
                status_code=None,
            )
        if ratio > 1.15:
            raise LLMError(
                f"LLM output too long (in={in_len}, out={out_len}, ratio={ratio:.2f} > 1.15)",
                status_code=None,
            )


def _is_whitespace_only(text: str) -> bool:
    return text.strip() == ""


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

    try:
        pre = _chunk_path(work_dir, "pre", index).read_text(encoding="utf-8")
        # Whitespace-only chunks are valid (e.g., paragraph separators). Skip LLM entirely to
        # avoid providers that emit no `content` for empty prompts.
        if _is_whitespace_only(pre):
            GLOBAL_JOBS.update_chunk(job_id, index, input_chars=len(pre), output_chars=len(pre))
            _atomic_write_text(_chunk_path(work_dir, "out", index), pre)
            GLOBAL_JOBS.update_chunk(job_id, index, state="done", finished_at=time.time())
            GLOBAL_JOBS.add_stat(job_id, "llm_skipped_blank_chunks", 1)
            return

        GLOBAL_JOBS.update_chunk(job_id, index, input_chars=len(pre), output_chars=None)

        retry_count = 0

        def on_retry(_retry_index: int, last_code: int | None, last_msg: str | None) -> None:
            nonlocal retry_count
            retry_count += 1
            GLOBAL_JOBS.update_chunk(job_id, index, state="retrying")
            GLOBAL_JOBS.add_retry(job_id, index, 1, last_code, last_msg)

        def _should_stop() -> bool:
            return GLOBAL_JOBS.is_cancelled(job_id)

        llm_cfg = llm
        if index == 0:
            llm_cfg = replace(llm, system_prompt=FIRST_CHUNK_SYSTEM_PROMPT_PREFIX + "\n\n" + llm.system_prompt)

        result, retries, last_code, last_msg = call_llm_text_resilient_with_meta_and_raw(
            llm_cfg,
            pre,
            should_stop=_should_stop,
            on_retry=on_retry,
        )
        raw_text = result.raw_text
        filtered_text = result.text

        if retries > retry_count:
            GLOBAL_JOBS.add_retry(job_id, index, retries - retry_count, last_code, last_msg)

        if GLOBAL_JOBS.is_cancelled(job_id):
            return

        assert filtered_text is not None
        GLOBAL_JOBS.update_chunk(job_id, index, output_chars=len(filtered_text))

        _atomic_write_text(_chunk_path(work_dir, "resp", index), raw_text or "")

        _validate_llm_output(pre, filtered_text, allow_shorter=(index == 0))

        final_text = _align_trailing_newlines(pre, filtered_text)
        if final_text != filtered_text:
            GLOBAL_JOBS.update_chunk(job_id, index, output_chars=len(final_text))

        _atomic_write_text(_chunk_path(work_dir, "out", index), final_text)
        GLOBAL_JOBS.update_chunk(job_id, index, state="done", finished_at=time.time())
        GLOBAL_JOBS.add_stat(job_id, "llm_chunks", 1)
    except LLMError as e:
        if GLOBAL_JOBS.is_cancelled(job_id):
            return
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

        # If cancelled, do not keep queued chunks as 'processing'.
        if GLOBAL_JOBS.is_cancelled(job_id):
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state="pending")
            return "cancelled"

        if GLOBAL_JOBS.is_paused(job_id) and pending_indices:
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state="pending")
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
        max_chars = int(fmt.max_chunk_chars)
        max_chars = max(200, min(4_000, max_chars))
        first_chunk_max_chars = min(4_000, max(max_chars, 2_000))
        chunks = chunk_by_lines_with_first_chunk_max(
            input_text,
            max_chars=max_chars,
            first_chunk_max_chars=first_chunk_max_chars,
        )
        total = len(chunks)
        GLOBAL_JOBS.init_chunks(job_id, total_chunks=total)

        local_stats: dict[str, int] = {}
        for i, c in enumerate(chunks):
            if GLOBAL_JOBS.is_cancelled(job_id):
                GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                return

            fixed, s = apply_rules(c, fmt)
            _atomic_write_text(_chunk_path(work_dir, "pre", i), fixed)
            _merge_stats(local_stats, s)

        for k, v in local_stats.items():
            GLOBAL_JOBS.add_stat(job_id, k, v)

        if GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return

        outcome = _run_llm_for_indices(job_id, list(range(total)), work_dir, llm)
        if outcome == "cancelled" or GLOBAL_JOBS.is_cancelled(job_id):
            GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
            return
        if outcome == "paused" or GLOBAL_JOBS.is_paused(job_id):
            GLOBAL_JOBS.update(job_id, state="paused", finished_at=None)
            return

        # Post-LLM deterministic pass: enforce local formatting invariants on outputs.
        post_stats: dict[str, int] = {}
        cur = GLOBAL_JOBS.get(job_id)
        if cur is not None:
            for cs in cur.chunk_statuses:
                if GLOBAL_JOBS.is_cancelled(job_id):
                    GLOBAL_JOBS.update(job_id, state="cancelled", finished_at=time.time())
                    return
                if cs.state != "done":
                    continue
                p = _chunk_path(work_dir, "out", cs.index)
                if not p.exists():
                    continue
                chunk_out = p.read_text(encoding="utf-8")
                fixed, s = apply_rules(chunk_out, fmt)
                if fixed != chunk_out:
                    _atomic_write_text(p, fixed)
                _merge_stats(post_stats, s)
        for k, v in post_stats.items():
            GLOBAL_JOBS.add_stat(job_id, f"post_{k}", v)

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
