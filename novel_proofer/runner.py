from __future__ import annotations

import concurrent.futures
import itertools
import logging
import os
import shutil
import time
from collections import deque
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

from novel_proofer import paths
from novel_proofer.env import env_truthy
from novel_proofer.executions import GLOBAL_EXECUTIONS, StopReason
from novel_proofer.formatting.chunking import iter_chunks_by_lines_with_first_chunk_max_from_file
from novel_proofer.formatting.config import FormatConfig, clamp_chunk_params
from novel_proofer.formatting.merge import merge_text_chunks_to_path
from novel_proofer.formatting.rules import apply_rules, is_chapter_title, is_separator_line
from novel_proofer.jobs import GLOBAL_JOBS, JobStatus
from novel_proofer.llm.client import LLMError, call_llm_text_resilient_with_meta_and_raw
from novel_proofer.llm.config import LLMConfig, build_first_chunk_config
from novel_proofer.states import ChunkState, JobPhase, JobState, WaitReason
from novel_proofer.workflow import (
    ProcessingFinalState,
    WorkflowEvent,
    WorkflowState,
    WorkflowTransitionError,
    processing_final_state,
    require_event,
)
from novel_proofer.workflow_context import workflow_context_for_job

logger = logging.getLogger(__name__)

_JOB_DEBUG_README = """\
本目录为 novel-proofer 的单次任务调试产物。

目录说明：
- pre/  : 保留目录结构；预处理文本本身以内存态和输入缓存为准
- out/  : 分片最终输出（通过校验）
- resp/ : LLM 原始响应
"""


def _workflow_event_state(st: JobStatus, event: WorkflowEvent) -> WorkflowState:
    result = require_event(workflow_context_for_job(st), event)
    assert result.next_state is not None
    return result.next_state


def _ensure_job_debug_readme(work_dir: Path) -> None:
    p = work_dir / "README.txt"
    if p.exists():
        return
    _atomic_write_text(p, _JOB_DEBUG_README)


_tmp_seq = itertools.count()


def _tmp_suffix() -> str:
    return f".{os.getpid()}_{next(_tmp_seq)}.tmp"


def _merge_stats(dst: dict[str, int], src: dict[str, int]) -> None:
    for k, v in src.items():
        dst[k] = dst.get(k, 0) + v


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp name to avoid cross-thread collisions.
    tmp = path.with_suffix(path.suffix + _tmp_suffix())
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


def _count_leading_blank_lines(text: str) -> int:
    text = _normalize_newlines(text)
    n = 0
    i = 0
    while True:
        j = text.find("\n", i)
        if j < 0:
            break
        line = text[i:j]
        if line.strip() != "":
            break
        n += 1
        i = j + 1
    return n


def _strip_leading_blank_lines(text: str) -> str:
    text = _normalize_newlines(text)
    i = 0
    while True:
        j = text.find("\n", i)
        if j < 0:
            return text
        line = text[i:j]
        if line.strip() != "":
            return text[i:]
        i = j + 1


def _align_leading_blank_lines(reference: str, text: str, *, max_newlines: int = 10) -> str:
    """Align leading blank lines in `text` to match `reference` (up to max_newlines)."""

    ref = _normalize_newlines(reference)
    out = _normalize_newlines(text)
    want = min(_count_leading_blank_lines(ref), max_newlines)
    have = _count_leading_blank_lines(out)
    if have == want:
        return out
    base = _strip_leading_blank_lines(out)
    return ("\n" * want) + base


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


def _cleanup_work_dir(job_id: str, work_dir: Path) -> None:
    if not work_dir.exists():
        return
    shutil.rmtree(work_dir)
    GLOBAL_JOBS.add_stat(job_id, "cleanup_work_dir", 1)


def _should_cleanup_debug_dir(job_id: str) -> bool:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return True
    return st.cleanup_debug_dir


def _chunk_path(work_dir: Path, subdir: str, index: int) -> Path:
    return work_dir / subdir / f"{index:06d}.txt"


def _merge_chunk_outputs(work_dir: Path, total_chunks: int, out_path: Path) -> None:
    def _iter_chunks() -> Iterator[tuple[str, bool]]:
        for i in range(total_chunks):
            p = _chunk_path(work_dir, "out", i)
            yield (p.read_text(encoding="utf-8"), i == total_chunks - 1)

    merge_text_chunks_to_path(_iter_chunks(), out_path)


def _post_merge_paragraph_indent_pass(out_path: Path, fmt: FormatConfig) -> None:
    if not fmt.paragraph_indent:
        return

    indent = ("\u3000" * 2) if fmt.indent_with_fullwidth_space else "  "
    tmp = out_path.with_suffix(out_path.suffix + _tmp_suffix())

    prev_blank = True
    try:
        with (
            out_path.open("r", encoding="utf-8", newline="") as src,
            tmp.open("w", encoding="utf-8", newline="") as dst,
        ):
            for raw in src:
                has_nl = raw.endswith("\n")
                line = raw[:-1] if has_nl else raw
                if line.endswith("\r"):
                    line = line[:-1]

                if line.strip() == "":
                    if has_nl:
                        dst.write("\n")
                    prev_blank = True
                    continue

                if is_chapter_title(line):
                    dst.write(line.lstrip())
                    if has_nl:
                        dst.write("\n")
                    prev_blank = False
                    continue

                if is_separator_line(line):
                    dst.write(line)
                    if has_nl:
                        dst.write("\n")
                    prev_blank = False
                    continue

                if prev_blank:
                    if line.startswith(indent):
                        out_line = line
                    else:
                        core = line.lstrip()
                        out_line = (indent + core) if (core and len(core) >= 2) else core
                else:
                    out_line = line.lstrip()

                dst.write(out_line)
                if has_nl:
                    dst.write("\n")
                prev_blank = False
        tmp.replace(out_path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except FileNotFoundError:
            pass
        except Exception:
            logger.exception("failed to cleanup temp merge file: %s", tmp)


def _finalize_processing(job_id: str, total: int, error_msg: str) -> bool:
    """Finalize job after PROCESS stage (no merge)."""

    if _reconcile_process_stop(job_id):
        return False

    cur = GLOBAL_JOBS.get(job_id)
    if cur is None:
        return False
    if _reconcile_process_stop(job_id):
        return False

    final_state = processing_final_state([c.state for c in cur.chunk_statuses])
    if final_state == ProcessingFinalState.ERROR:
        if _reconcile_process_stop(job_id):
            return False
        next_state = _workflow_event_state(cur, WorkflowEvent.PROCESS_FAILED)
        GLOBAL_JOBS.update(
            job_id,
            state=next_state.state,
            phase=next_state.phase,
            wait_reason=next_state.wait_reason,
            finished_at=time.time(),
            error=error_msg,
            done_chunks=cur.done_chunks,
        )
        return False

    # All chunks processed; wait for explicit merge.
    if _reconcile_process_stop(job_id):
        return False
    next_state = _workflow_event_state(cur, WorkflowEvent.PROCESS_COMPLETE)
    GLOBAL_JOBS.update(
        job_id,
        state=next_state.state,
        phase=next_state.phase,
        wait_reason=next_state.wait_reason,
        finished_at=None,
        error=None,
        done_chunks=total,
    )
    return True


def _post_llm_deterministic_pass(job_id: str, work_dir: Path) -> None:
    """Enforce local formatting invariants on per-chunk outputs."""

    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    fmt = st.format

    post_stats: dict[str, int] = {}
    for cs in st.chunk_statuses:
        if _stop_requested(job_id):
            return
        if cs.state != ChunkState.DONE:
            continue
        p = _chunk_path(work_dir, "out", cs.index)
        if not p.exists():
            continue
        chunk_out = p.read_text(encoding="utf-8")
        pre_fmt = replace(fmt, paragraph_indent=False)
        fixed, s = apply_rules(chunk_out, pre_fmt)
        if fixed != chunk_out:
            _atomic_write_text(p, fixed)
        _merge_stats(post_stats, s)

    for k, v in post_stats.items():
        GLOBAL_JOBS.add_stat(job_id, f"post_{k}", v)


_MIN_VALIDATE_LEN = 200
_SHORTEST_RATIO = 0.85
_LONGEST_RATIO = 1.15
_WORKER_WAIT_TIMEOUT_S = 0.5


def _stop_reason(job_id: str) -> str | None:
    return GLOBAL_EXECUTIONS.stop_reason(job_id)


def _stop_requested(job_id: str) -> bool:
    return _stop_reason(job_id) is not None


def _pause_requested(job_id: str) -> bool:
    return _stop_reason(job_id) == StopReason.PAUSE.value


def _delete_requested(job_id: str) -> bool:
    return _stop_reason(job_id) == StopReason.DELETE.value


def _mark_reset_requested(job_id: str, *, phase: JobPhase | str | None = None) -> None:
    GLOBAL_JOBS.mark_reset_requested(job_id, phase=phase)


def _mark_execution_stopped(job_id: str, *, phase: JobPhase | str) -> None:
    GLOBAL_JOBS.mark_execution_stopped(job_id, phase=phase, wait_reason=WaitReason.USER_PAUSED)


def _reconcile_process_stop(job_id: str) -> bool:
    if _delete_requested(job_id):
        _mark_reset_requested(job_id, phase=JobPhase.PROCESS)
        return True
    if _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
        return True
    return False


def _reconcile_merge_delete(job_id: str) -> bool:
    if _delete_requested(job_id):
        _mark_reset_requested(job_id, phase=JobPhase.MERGE)
        return True
    return False


def _reset_stopped_chunk(job_id: str, index: int) -> None:
    GLOBAL_JOBS.update_chunk(
        job_id,
        index,
        state=ChunkState.PENDING,
        started_at=None,
        finished_at=None,
    )


def _validate_llm_output(input_text: str, output_text: str, *, allow_shorter: bool = False) -> None:
    in_len = len(input_text)
    out_len = len(output_text)
    out_trim = len(output_text.strip())
    if in_len > 0 and out_trim == 0:
        raise LLMError(
            "LLM output empty",
            status_code=None,
        )
    if in_len >= _MIN_VALIDATE_LEN and in_len > 0:
        ratio = out_len / in_len
        if ratio < _SHORTEST_RATIO and not allow_shorter:
            raise LLMError(
                f"LLM output too short (in={in_len}, out={out_len}, ratio={ratio:.2f} < {_SHORTEST_RATIO})",
                status_code=None,
            )
        if ratio > _LONGEST_RATIO:
            raise LLMError(
                f"LLM output too long (in={in_len}, out={out_len}, ratio={ratio:.2f} > {_LONGEST_RATIO})",
                status_code=None,
            )


def _is_whitespace_only(text: str) -> bool:
    return text.strip() == ""


def _ensure_chunk_pre_texts(job_id: str, target_indices: list[int], input_path: Path, fmt: FormatConfig) -> None:
    missing = {int(index) for index in target_indices if GLOBAL_JOBS.get_chunk_pre_text(job_id, int(index)) is None}
    if not missing:
        return
    if not input_path.exists():
        raise FileNotFoundError("job input cache missing")

    max_chars, first_chunk_max_chars = clamp_chunk_params(fmt.max_chunk_chars)
    pre_fmt = replace(fmt, paragraph_indent=False)
    for index, chunk in enumerate(
        iter_chunks_by_lines_with_first_chunk_max_from_file(
            input_path,
            max_chars=max_chars,
            first_chunk_max_chars=first_chunk_max_chars,
        )
    ):
        if index not in missing:
            continue
        fixed, _stats = apply_rules(chunk, pre_fmt)
        GLOBAL_JOBS.set_chunk_pre_text(job_id, index, fixed)
        missing.remove(index)
        if not missing:
            return

    missing_text = ", ".join(str(index) for index in sorted(missing))
    raise ValueError(f"preprocessed chunk text missing for indices: {missing_text}")


def _llm_worker(job_id: str, index: int, work_dir: Path, llm: LLMConfig, *, write_llm_resp: bool) -> None:
    if _stop_requested(job_id):
        return

    resp_path = _chunk_path(work_dir, "resp", index)
    raw_text: str | None = None
    try:
        pre = GLOBAL_JOBS.get_chunk_pre_text(job_id, index)
        if pre is None:
            raise RuntimeError(f"preprocessed chunk text missing for index {index}")
        GLOBAL_JOBS.update_chunk(
            job_id,
            index,
            state=ChunkState.PROCESSING,
            started_at=time.time(),
            finished_at=None,
            last_error_code=None,
            last_error_message=None,
            llm_model=llm.model,
            input_chars=len(pre),
            output_chars=None,
        )
        # Whitespace-only chunks are valid (e.g., paragraph separators). Skip LLM entirely to
        # avoid providers that emit no `content` for empty prompts.
        if _is_whitespace_only(pre):
            if _stop_requested(job_id):
                _reset_stopped_chunk(job_id, index)
                return
            _atomic_write_text(_chunk_path(work_dir, "out", index), pre)
            GLOBAL_JOBS.update_chunk(
                job_id, index, state=ChunkState.DONE, finished_at=time.time(), output_chars=len(pre)
            )
            GLOBAL_JOBS.add_stat(job_id, "llm_skipped_blank_chunks", 1)
            return

        retry_count = 0

        def on_retry(_retry_index: int, last_code: int | None, last_msg: str | None) -> None:
            nonlocal retry_count
            retry_count += 1
            GLOBAL_JOBS.update_chunk(job_id, index, state=ChunkState.RETRYING)
            GLOBAL_JOBS.add_retry(job_id, index, 1, last_code, last_msg)

        def _should_stop() -> bool:
            return _stop_requested(job_id)

        llm_cfg = llm
        if index == 0:
            llm_cfg = build_first_chunk_config(llm)

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

        if _stop_requested(job_id):
            _reset_stopped_chunk(job_id, index)
            return

        assert filtered_text is not None

        if write_llm_resp:
            _atomic_write_text(resp_path, raw_text or "")

        _validate_llm_output(pre, filtered_text, allow_shorter=(index == 0))

        final_text = _align_leading_blank_lines(pre, filtered_text)
        final_text = _align_trailing_newlines(pre, final_text)
        _atomic_write_text(_chunk_path(work_dir, "out", index), final_text)
        GLOBAL_JOBS.update_chunk(
            job_id, index, state=ChunkState.DONE, finished_at=time.time(), output_chars=len(final_text)
        )
        # Free pre text memory once processed.
        GLOBAL_JOBS.pop_chunk_pre_text(job_id, index)
        GLOBAL_JOBS.add_stat(job_id, "llm_chunks", 1)
    except LLMError as e:
        if _stop_requested(job_id):
            _reset_stopped_chunk(job_id, index)
            return
        if raw_text is not None:
            _atomic_write_text(resp_path, raw_text or "")
        GLOBAL_JOBS.update_chunk(
            job_id,
            index,
            state=ChunkState.ERROR,
            finished_at=time.time(),
            last_error_code=e.status_code,
            last_error_message=str(e),
        )
    except Exception as e:
        if _stop_requested(job_id):
            _reset_stopped_chunk(job_id, index)
            return
        if raw_text is not None:
            _atomic_write_text(resp_path, raw_text or "")
        GLOBAL_JOBS.update_chunk(
            job_id,
            index,
            state=ChunkState.ERROR,
            finished_at=time.time(),
            last_error_message=str(e),
        )


def _run_llm_for_indices(job_id: str, indices: list[int], work_dir: Path, llm: LLMConfig) -> str:
    max_workers = int(llm.max_concurrency)
    if max_workers < 1:
        raise ValueError("LLM max_concurrency must be >= 1")
    write_llm_resp = env_truthy("NOVEL_PROOFER_LLM_WRITE_RESP")
    if not write_llm_resp:
        st = GLOBAL_JOBS.get_summary(job_id)
        if st is not None and not st.cleanup_debug_dir:
            write_llm_resp = True
    GLOBAL_JOBS.update(
        job_id,
        state=JobState.RUNNING,
        phase=JobPhase.PROCESS,
        started_at=time.time(),
        finished_at=None,
        error=None,
        last_llm_model=llm.model,
    )
    if _delete_requested(job_id):
        return "cancelled"
    if _pause_requested(job_id):
        return "paused"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        # Submit gradually so cancel can actually stop launching new work.
        pending_indices = deque(indices)
        in_flight: dict[concurrent.futures.Future[None], int] = {}

        while pending_indices or in_flight:
            if _delete_requested(job_id):
                break
            paused = _pause_requested(job_id)
            if paused and not in_flight:
                break

            if not paused:
                # Fill up the worker pool.
                while (
                    pending_indices
                    and len(in_flight) < max_workers
                    and not _delete_requested(job_id)
                    and not _pause_requested(job_id)
                ):
                    i = pending_indices.popleft()
                    fut = ex.submit(_llm_worker, job_id, i, work_dir, llm, write_llm_resp=write_llm_resp)
                    in_flight[fut] = i

            if not in_flight:
                break

            done, _ = concurrent.futures.wait(
                in_flight.keys(), timeout=_WORKER_WAIT_TIMEOUT_S, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in done:
                idx = in_flight.pop(f, None)
                try:
                    f.result()
                except Exception as e:
                    logger.exception("llm worker crashed: job_id=%s chunk=%s", job_id, idx)
                    if idx is not None and not _stop_requested(job_id):
                        GLOBAL_JOBS.update_chunk(
                            job_id,
                            idx,
                            state=ChunkState.ERROR,
                            finished_at=time.time(),
                            last_error_message=f"worker crashed: {e}",
                        )

        # If cancelled, do not keep queued chunks as 'processing'.
        if _delete_requested(job_id):
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state=ChunkState.PENDING)
            return "cancelled"

        if _pause_requested(job_id) and pending_indices:
            for i in pending_indices:
                GLOBAL_JOBS.update_chunk(job_id, i, state=ChunkState.PENDING)
            return "paused"

    return "done"


def run_job(job_id: str, input_path: Path, fmt: FormatConfig, llm: LLMConfig) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == JobState.CANCELLED or _delete_requested(job_id):
        return
    if _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=st.phase)
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(
            job_id, state=JobState.ERROR, finished_at=time.time(), error="job missing work_dir/output_path"
        )
        return

    work_dir = Path(st.work_dir)
    (work_dir / "pre").mkdir(parents=True, exist_ok=True)
    (work_dir / "out").mkdir(parents=True, exist_ok=True)
    (work_dir / "resp").mkdir(parents=True, exist_ok=True)
    _ensure_job_debug_readme(work_dir)

    GLOBAL_JOBS.update(
        job_id,
        state=JobState.RUNNING,
        phase=JobPhase.VALIDATE,
        format=fmt,
        started_at=time.time(),
        finished_at=None,
        error=None,
        last_llm_model=llm.model,
    )

    try:
        if not input_path.exists():
            GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error="job input cache missing")
            return

        max_chars, first_chunk_max_chars = clamp_chunk_params(fmt.max_chunk_chars)
        local_stats: dict[str, int] = {}
        total = 0
        # For performance, keep preprocessed chunk text in memory during active runs.
        # The pre/ directory remains as debug layout only, not as workflow truth.
        for i, c in enumerate(
            iter_chunks_by_lines_with_first_chunk_max_from_file(
                input_path,
                max_chars=max_chars,
                first_chunk_max_chars=first_chunk_max_chars,
            )
        ):
            if _delete_requested(job_id):
                _mark_reset_requested(job_id, phase=JobPhase.VALIDATE)
                return
            if _pause_requested(job_id):
                _mark_execution_stopped(job_id, phase=JobPhase.VALIDATE)
                return

            pre_fmt = replace(fmt, paragraph_indent=False)
            fixed, s = apply_rules(c, pre_fmt)
            GLOBAL_JOBS.set_chunk_pre_text(job_id, i, fixed)
            _merge_stats(local_stats, s)
            total = i + 1

        GLOBAL_JOBS.init_chunks(job_id, total_chunks=total, llm_model=llm.model)

        for k, v in local_stats.items():
            GLOBAL_JOBS.add_stat(job_id, k, v)

        if _delete_requested(job_id):
            _mark_reset_requested(job_id, phase=JobPhase.PROCESS)
            return
        if _pause_requested(job_id):
            _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
            return

        cur = GLOBAL_JOBS.get(job_id)
        if cur is None:
            return
        next_state = _workflow_event_state(cur, WorkflowEvent.VALIDATION_COMPLETE)
        GLOBAL_JOBS.update(
            job_id,
            state=next_state.state,
            phase=next_state.phase,
            wait_reason=next_state.wait_reason,
            finished_at=None,
            error=None,
        )
    except Exception as e:
        if _delete_requested(job_id):
            _mark_reset_requested(job_id)
            return
        if _pause_requested(job_id):
            cur = GLOBAL_JOBS.get(job_id)
            _mark_execution_stopped(job_id, phase=JobPhase.VALIDATE if cur is None else cur.phase)
            return
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error=str(e))


def retry_failed_chunks(job_id: str, llm: LLMConfig, target_indices: tuple[int, ...]) -> None:
    targets = tuple(int(index) for index in target_indices)
    if not targets:
        raise ValueError("retry target indices are required")

    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == JobState.CANCELLED or _delete_requested(job_id):
        return
    if _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(
            job_id, state=JobState.ERROR, finished_at=time.time(), error="job missing work_dir/output_path"
        )
        return

    work_dir = Path(st.work_dir)
    _ensure_job_debug_readme(work_dir)

    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error="job has no chunk statuses")
        return

    total = len(st.chunk_statuses)
    for index in targets:
        if index < 0 or index >= total:
            raise ValueError(f"retry target index out of range: {index}")

    GLOBAL_JOBS.update(
        job_id, state=JobState.QUEUED, phase=JobPhase.PROCESS, finished_at=None, error=None, last_llm_model=llm.model
    )
    for i in targets:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state=ChunkState.PENDING,
            started_at=None,
            finished_at=None,
            llm_model=llm.model,
            input_chars=None,
            output_chars=None,
        )

    try:
        _ensure_chunk_pre_texts(job_id, list(targets), paths._input_cache_path(job_id), st.format)
    except Exception as e:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error=str(e))
        return

    outcome = _run_llm_for_indices(job_id, list(targets), work_dir, llm)
    if outcome == "cancelled" or _delete_requested(job_id):
        _mark_reset_requested(job_id, phase=JobPhase.PROCESS)
        return
    if outcome == "paused" or _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
        return
    _post_llm_deterministic_pass(job_id, work_dir)
    if _reconcile_process_stop(job_id):
        return
    _finalize_processing(job_id, total, "some chunks still failed; update LLM config and retry again")


def resume_paused_job(job_id: str, llm: LLMConfig) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == JobState.CANCELLED or _delete_requested(job_id):
        return
    if _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(
            job_id, state=JobState.ERROR, finished_at=time.time(), error="job missing work_dir/output_path"
        )
        return
    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error="job has no chunk statuses")
        return

    work_dir = Path(st.work_dir)
    _ensure_job_debug_readme(work_dir)

    total = len(st.chunk_statuses)
    pending = [c.index for c in st.chunk_statuses if c.state not in {ChunkState.DONE, ChunkState.ERROR}]
    if not pending:
        _finalize_processing(job_id, total, "some chunks failed; update LLM config and retry failed chunks")
        return

    for i in pending:
        GLOBAL_JOBS.update_chunk(
            job_id,
            i,
            state=ChunkState.PENDING,
            started_at=None,
            finished_at=None,
            llm_model=llm.model,
        )

    try:
        _ensure_chunk_pre_texts(job_id, pending, paths._input_cache_path(job_id), st.format)
    except Exception as e:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error=str(e))
        return

    outcome = _run_llm_for_indices(job_id, pending, work_dir, llm)
    if outcome == "cancelled" or _delete_requested(job_id):
        _mark_reset_requested(job_id, phase=JobPhase.PROCESS)
        return
    if outcome == "paused" or _pause_requested(job_id):
        _mark_execution_stopped(job_id, phase=JobPhase.PROCESS)
        return

    _post_llm_deterministic_pass(job_id, work_dir)
    if _reconcile_process_stop(job_id):
        return
    _finalize_processing(job_id, total, "some chunks failed; update LLM config and retry failed chunks")


def merge_outputs(job_id: str, *, cleanup_debug_dir: bool | None = None) -> None:
    st = GLOBAL_JOBS.get(job_id)
    if st is None:
        return
    if st.state == JobState.CANCELLED or _delete_requested(job_id):
        return
    if not st.work_dir or not st.output_path:
        GLOBAL_JOBS.update(
            job_id, state=JobState.ERROR, finished_at=time.time(), error="job missing work_dir/output_path"
        )
        return
    if not st.chunk_statuses:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, finished_at=time.time(), error="job has no chunk statuses")
        return

    work_dir = Path(st.work_dir)
    out_path = Path(st.output_path)
    total = len(st.chunk_statuses)

    try:
        next_state = _workflow_event_state(st, WorkflowEvent.MERGE_STARTED)
    except WorkflowTransitionError as e:
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, phase=st.phase, finished_at=time.time(), error=str(e))
        return
    GLOBAL_JOBS.update(
        job_id,
        state=next_state.state,
        phase=next_state.phase,
        wait_reason=next_state.wait_reason,
        finished_at=None,
        error=None,
    )
    try:
        _merge_chunk_outputs(work_dir, total, out_path)
        if _reconcile_merge_delete(job_id):
            return
        _post_merge_paragraph_indent_pass(out_path, st.format)
        if _reconcile_merge_delete(job_id):
            return
        cur = GLOBAL_JOBS.get(job_id)
        if cur is None:
            return
        if _reconcile_merge_delete(job_id):
            return
        done_state = _workflow_event_state(cur, WorkflowEvent.MERGE_COMPLETE)
        GLOBAL_JOBS.update(
            job_id,
            state=done_state.state,
            phase=done_state.phase,
            wait_reason=done_state.wait_reason,
            finished_at=time.time(),
            done_chunks=total,
        )
        do_cleanup = bool(st.cleanup_debug_dir) if cleanup_debug_dir is None else bool(cleanup_debug_dir)
        if do_cleanup:
            _cleanup_work_dir(job_id, work_dir)
        else:
            GLOBAL_JOBS.add_stat(job_id, "cleanup_work_dir_skipped", 1)
    except Exception as e:
        if _delete_requested(job_id):
            _mark_reset_requested(job_id, phase=JobPhase.MERGE)
            return
        GLOBAL_JOBS.update(job_id, state=JobState.ERROR, phase=JobPhase.MERGE, finished_at=time.time(), error=str(e))
