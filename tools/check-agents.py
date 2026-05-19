from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_SNIPPETS = (
    "uv sync --frozen --no-install-project --group dev",
    "uv run --frozen --no-sync ruff format --check",
    "uv run --frozen --no-sync ruff check",
    "uv run --frozen --no-sync python -m mypy novel_proofer",
    "uv run --frozen --no-sync python -m pytest -q",
    "uv run --frozen --no-sync python tools/export-openapi.py --check",
    "uv run --frozen --no-sync python tools/check-large-files.py",
)


def main() -> int:
    text = Path("AGENTS.md").read_text(encoding="utf-8")
    missing = [snippet for snippet in REQUIRED_SNIPPETS if snippet not in text]
    if missing:
        print("AGENTS.md is missing required agent instructions:", file=sys.stderr)
        for snippet in missing:
            print(f"- {snippet}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
