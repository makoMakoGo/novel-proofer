from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from novel_proofer.env import env_int
from novel_proofer.executions import GLOBAL_EXECUTIONS
from novel_proofer.states import JobCommand

logger = logging.getLogger(__name__)


def _max_workers_from_env() -> int:
    value = env_int("NOVEL_PROOFER_JOB_MAX_WORKERS", 2)
    if value < 1:
        raise ValueError("NOVEL_PROOFER_JOB_MAX_WORKERS must be >= 1")
    return value


_EXECUTOR = ThreadPoolExecutor(
    max_workers=_max_workers_from_env(),
    thread_name_prefix="novel-proofer-job",
)


def submit(
    job_id: str,
    command: JobCommand | str,
    fn: Callable[..., Any],
    /,
    *args: Any,
    on_crash: Callable[[BaseException], Any] | None = None,
    **kwargs: Any,
) -> None:
    """Run a job function in a bounded background thread pool.

    Notes:
    - We intentionally do not expose the Future to callers.
    - Execution lifecycle is tracked through GLOBAL_EXECUTIONS, not the durable job record.
    """

    execution = GLOBAL_EXECUTIONS.begin(job_id, command)

    def _run() -> Any:
        GLOBAL_EXECUTIONS.mark_running(execution.attempt_id)
        return fn(*args, **kwargs)

    try:
        fut = _EXECUTOR.submit(_run)
    except Exception:
        GLOBAL_EXECUTIONS.finish(execution.attempt_id)
        raise

    def _done(f: Future[Any]) -> None:
        try:
            f.result()
        except Exception as e:
            logger.exception("background job crashed: job_id=%s attempt_id=%s", execution.job_id, execution.attempt_id)
            if on_crash is not None:
                try:
                    on_crash(e)
                except Exception:
                    logger.exception(
                        "background crash reconciler failed: job_id=%s attempt_id=%s",
                        execution.job_id,
                        execution.attempt_id,
                    )
        except BaseException:
            logger.exception("background job crashed: job_id=%s attempt_id=%s", execution.job_id, execution.attempt_id)
        finally:
            callbacks = GLOBAL_EXECUTIONS.finish(execution.attempt_id)
            for cb in callbacks:
                try:
                    cb()
                except Exception:
                    logger.exception("background post-callback crashed: job_id=%s", execution.job_id)

    fut.add_done_callback(_done)


def add_done_callback(job_id: str, cb: Callable[[], Any]) -> None:
    """Run `cb` once the current in-flight job for `job_id` finishes.

    If `job_id` is not in-flight, runs `cb` immediately.
    """

    jid = str(job_id or "").strip()
    if not jid:
        raise ValueError("job_id is required")

    if not GLOBAL_EXECUTIONS.add_done_callback(jid, cb):
        cb()


def shutdown(*, wait: bool = False) -> None:
    _EXECUTOR.shutdown(wait=wait, cancel_futures=not wait)
