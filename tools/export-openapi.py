from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from novel_proofer.api import app

SCHEMA_PATH = Path("docs/openapi.json")


def _schema_text() -> str:
    return json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Export the FastAPI OpenAPI schema.")
    parser.add_argument("--check", action="store_true", help="Fail if docs/openapi.json is stale.")
    args = parser.parse_args()

    expected = _schema_text()
    if args.check:
        actual = SCHEMA_PATH.read_text(encoding="utf-8") if SCHEMA_PATH.exists() else ""
        if actual != expected:
            print(
                "docs/openapi.json is stale; run `uv run --frozen --no-sync python tools/export-openapi.py`.",
                file=sys.stderr,
            )
            return 1
        return 0

    SCHEMA_PATH.write_text(expected, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
