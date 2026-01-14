from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_in_flight: dict[str, Future] = {}


def _max_workers_from_env() -> int:
    raw = str(os.getenv("NOVEL_PROOFER_JOB_MAX_WORKERS", "") or "").strip()
    if not raw:
        return 2
    try:
        return max(1, int(raw))
    except Exception:
        return 2


_EXECUTOR = ThreadPoolExecutor(
    max_workers=_max_workers_from_env(),
    thread_name_prefix="novel-proofer-job",
)


def submit(job_id: str, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
    """Run a job function in a bounded background thread pool.

    Notes:
    - We intentionally do not expose the Future to callers. Job status is tracked via GLOBAL_JOBS.
    - Exceptions are logged, but job code is responsible for updating job/chunk states.
    """

    jid = str(job_id or "").strip()
    if not jid:
        raise ValueError("job_id is required")

    with _lock:
        if jid in _in_flight:
            raise ValueError(f"job_id '{jid}' is already in flight")
        fut = _EXECUTOR.submit(fn, *args, **kwargs)
        _in_flight[jid] = fut

    def _done(f: Future) -> None:
        with _lock:
            # Only remove if this callback matches the currently tracked future.
            if _in_flight.get(jid) is f:
                _in_flight.pop(jid, None)
        try:
            f.result()
        except Exception:
            logger.exception("background job crashed: job_id=%s", jid)

    fut.add_done_callback(_done)


def add_done_callback(job_id: str, cb: Callable[[], Any]) -> None:
    """Run `cb` once the current in-flight job for `job_id` finishes.

    If `job_id` is not in-flight, runs `cb` immediately.
    """

    jid = str(job_id or "").strip()
    if not jid:
        raise ValueError("job_id is required")

    with _lock:
        fut = _in_flight.get(jid)

    if fut is None:
        cb()
        return

    def _wrap(_f: Future) -> None:
        try:
            cb()
        except Exception:
            logger.exception("background post-callback crashed: job_id=%s", jid)

    fut.add_done_callback(_wrap)


def shutdown(*, wait: bool = False) -> None:
    _EXECUTOR.shutdown(wait=wait, cancel_futures=not wait)
