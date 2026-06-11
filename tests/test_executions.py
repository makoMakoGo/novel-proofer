from __future__ import annotations

import threading
import time

import pytest

import novel_proofer.background as background
from novel_proofer.executions import GLOBAL_EXECUTIONS, ExecutionAlreadyActive, ExecutionRegistry


def test_execution_registry_tracks_attempt_stop_and_callbacks() -> None:
    registry = ExecutionRegistry()

    execution = registry.begin("job-1", "process")
    assert execution.state == "queued"
    assert execution.command == "process"

    with pytest.raises(ExecutionAlreadyActive):
        registry.begin("job-1", "merge")

    registry.mark_running(execution.attempt_id)
    running = registry.get("job-1")
    assert running is not None
    assert running.state == "running"

    assert registry.request_stop("job-1", "pause") is True
    assert registry.stop_reason("job-1") == "pause"
    assert registry.request_stop("job-1", "delete") is True
    assert registry.stop_reason("job-1") == "delete"

    called: list[str] = []
    assert registry.add_done_callback("job-1", lambda: called.append("done")) is True

    callbacks = registry.finish(execution.attempt_id)
    assert len(callbacks) == 1
    for cb in callbacks:
        cb()

    assert called == ["done"]
    assert registry.get("job-1") is None
    assert registry.request_stop("job-1", "pause") is False


def test_background_submit_cleans_up_execution_on_base_exception() -> None:
    job_id = "job-base-exit"
    done = threading.Event()
    on_crash_types: list[type[BaseException]] = []

    def _boom() -> None:
        raise SystemExit("bye")

    background.submit(job_id, "process", _boom, on_crash=lambda exc: on_crash_types.append(type(exc)))
    background.add_done_callback(job_id, done.set)

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if done.is_set() and GLOBAL_EXECUTIONS.get(job_id) is None:
            break
        time.sleep(0.01)
    else:
        raise AssertionError("background submit did not clean up execution entry after BaseException")

    assert on_crash_types == []
