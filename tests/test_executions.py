from __future__ import annotations

import pytest

from novel_proofer.executions import ExecutionAlreadyActive, ExecutionRegistry


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
