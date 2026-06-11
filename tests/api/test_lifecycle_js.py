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
    assert "refreshJobOnce(state.currentJobId)" in lifecycle_section
