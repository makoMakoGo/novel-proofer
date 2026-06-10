from __future__ import annotations

from pathlib import Path


def test_browser_lifecycle_handlers_do_not_mutate_jobs() -> None:
    api_js = Path("templates/static/js/api.js").read_text(encoding="utf-8")
    main_js = Path("templates/static/js/main.js").read_text(encoding="utf-8")

    lifecycle_section = main_js.split("function bindLifecycleEvents()", 1)[1].split(
        "function restoreLastAttachedJob()", 1
    )[0]

    assert "bestEffortPauseJob" not in api_js
    assert "bestEffortPauseJob" not in main_js
    assert "sendBeacon" not in api_js
    assert "keepalive" not in api_js
    assert "/pause" not in lifecycle_section
    assert "pauseJob" not in lifecycle_section
    assert "resetJob" not in lifecycle_section
    assert "window.addEventListener('pagehide', stopUiObserver)" in lifecycle_section
    assert "window.addEventListener('beforeunload', stopUiObserver)" in lifecycle_section
