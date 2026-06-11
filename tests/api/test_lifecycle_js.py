from __future__ import annotations

import re
from pathlib import Path


def extract_js_function(source: str, name: str) -> str:
    match = re.search(rf"\bfunction\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", source)
    assert match is not None, f"function {name} not found"

    depth = 0
    for index in range(match.end() - 1, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[match.start() : index + 1]

    raise AssertionError(f"function {name} has no closing brace")


def extract_js_block_after(source: str, needle: str) -> str:
    start = source.index(needle)
    brace_start = source.index("{", start)

    depth = 0
    for index in range(brace_start, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"block after {needle!r} has no closing brace")


def test_browser_lifecycle_handlers_do_not_mutate_jobs() -> None:
    api_js = Path("templates/static/js/api.js").read_text(encoding="utf-8")
    main_js = Path("templates/static/js/main.js").read_text(encoding="utf-8")

    lifecycle_section = extract_js_function(main_js, "bindLifecycleEvents")

    assert "bestEffortPauseJob" not in api_js
    assert "bestEffortPauseJob" not in main_js
    assert "sendBeacon" not in api_js
    assert "keepalive" not in api_js
    for forbidden in ("/pause", "pauseJob", "resetJob", "sendBeacon", "keepalive"):
        assert forbidden not in lifecycle_section

    assert "stopPolling()" in lifecycle_section
    assert "state.pollInFlight = false" in lifecycle_section
    assert re.search(r"addEventListener\(\s*['\"]pagehide['\"]\s*,\s*stopUiObserver\s*\)", lifecycle_section)
    assert re.search(r"addEventListener\(\s*['\"]beforeunload['\"]\s*,\s*stopUiObserver\s*\)", lifecycle_section)
    assert re.search(r"addEventListener\(\s*['\"]pageshow['\"]\s*,\s*resumeUiObserver\s*\)", lifecycle_section)
    assert "event.persisted" in lifecycle_section
    assert "attachedJobId()" in lifecycle_section
    assert "refreshJobOnce(jobId)" in lifecycle_section


def test_restore_and_missing_job_only_change_ui_attachment() -> None:
    main_js = Path("templates/static/js/main.js").read_text(encoding="utf-8")

    restore_section = extract_js_function(main_js, "restoreLastAttachedJob")
    assert "restoreUiAttachment()" in restore_section
    assert "attachJob(last)" in restore_section
    assert "api." not in restore_section

    refresh_section = extract_js_function(main_js, "refreshJobOnce")
    assert "r.status === 404" in refresh_section
    assert "detachUi()" in refresh_section
    assert "api.pauseJob" not in refresh_section
    assert "api.resetJob" not in refresh_section


def test_new_task_detaches_without_backend_mutation() -> None:
    main_js = Path("templates/static/js/main.js").read_text(encoding="utf-8")

    new_task_section = extract_js_block_after(main_js, "ui.elements.btnCancel?.addEventListener")
    assert "detachUi({ clearFile: true })" in new_task_section
    assert "api.pauseJob" not in new_task_section
    assert "api.resetJob" not in new_task_section
    assert "api.purgeAllJobs" not in new_task_section
