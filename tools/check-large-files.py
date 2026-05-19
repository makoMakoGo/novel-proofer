from __future__ import annotations

import subprocess
import sys
from pathlib import Path

MAX_SOURCE_BYTES = 500_000
MAX_BINARY_BYTES = 1_500_000
ALLOWLIST = {Path("images/校正前后对比.png")}


def _tracked_files() -> list[Path]:
    raw = subprocess.check_output(["git", "ls-files", "-z"])
    return [Path(p.decode("utf-8")) for p in raw.split(b"\0") if p]


def _is_binary(path: Path) -> bool:
    with path.open("rb") as f:
        return b"\0" in f.read(4096)


def main() -> int:
    failures: list[str] = []
    for path in _tracked_files():
        if path in ALLOWLIST or not path.is_file():
            continue
        size = path.stat().st_size
        limit = MAX_BINARY_BYTES if _is_binary(path) else MAX_SOURCE_BYTES
        if size > limit:
            failures.append(f"{path}: {size} bytes exceeds {limit}")

    if failures:
        print("Tracked file size check failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
