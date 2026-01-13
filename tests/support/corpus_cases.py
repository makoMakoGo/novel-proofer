from __future__ import annotations

import json
import time
from pathlib import Path


def read_json_object(path: Path) -> dict:
    if not path.exists():
        raise AssertionError(f"missing fixture file: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise AssertionError(f"JSON must be an object: {path}")
    return obj


def read_text(path: Path) -> str:
    if not path.exists():
        raise AssertionError(f"missing fixture file: {path}")
    return path.read_text(encoding="utf-8")


def list_case_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()])


def count_leading_blank_lines(text: str) -> int:
    n = 0
    i = 0
    while True:
        j = text.find("\n", i)
        if j < 0:
            break
        line = text[i:j]
        if line.strip() != "":
            break
        n += 1
        i = j + 1
    return n


def count_trailing_blank_lines(text: str) -> int:
    lines = text.splitlines()
    n = 0
    for line in reversed(lines):
        if line.strip() != "":
            break
        n += 1
    return n


def assert_substrings_in_order(haystack: str, needles: list[str], *, case_name: str) -> None:
    cursor = 0
    for s in needles:
        if not s:
            continue
        idx = haystack.find(s, cursor)
        if idx < 0:
            raise AssertionError(f"[{case_name}] missing substring: {s!r}")
        cursor = idx + len(s)


def write_failure_artifacts(
    *,
    artifacts_root: Path,
    suite: str,
    case_name: str,
    input_text: str,
    output_text: str,
    meta: dict | None = None,
) -> Path:
    """Write failure artifacts and return the case artifact directory.

    Artifacts are overwritten on each run for the same case.
    """

    case_dir = artifacts_root / suite / case_name
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "input.txt").write_text(input_text, encoding="utf-8")
    (case_dir / "output.txt").write_text(output_text, encoding="utf-8")
    (case_dir / "run_id.txt").write_text(time.strftime("%Y-%m-%dT%H-%M-%S"), encoding="utf-8")
    if meta is not None:
        (case_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return case_dir

