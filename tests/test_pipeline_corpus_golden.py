from __future__ import annotations

import json
import tempfile
from dataclasses import replace as dc_replace
from pathlib import Path

import pytest
from _pytest.config import Config

import novel_proofer.runner as runner
from novel_proofer.formatting.chunking import chunk_by_lines_with_first_chunk_max
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.client import LLMTextResult
from novel_proofer.llm.config import LLMConfig


def _read_json_object(path: Path) -> dict:
    if not path.exists():
        raise AssertionError(f"missing fixture file: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise AssertionError(f"JSON must be an object: {path}")
    return obj


def _read_text(path: Path) -> str:
    if not path.exists():
        raise AssertionError(f"missing fixture file: {path}")
    return path.read_text(encoding="utf-8")


def _assert_invariants(text: str) -> None:
    assert "\r" not in text
    assert "\t" not in text
    assert "<think" not in text.lower()


def _assert_is_multichunk(text: str, fmt: FormatConfig) -> None:
    max_chars = max(200, min(4_000, int(fmt.max_chunk_chars)))
    first_chunk_max_chars = min(4_000, max(max_chars, 2_000))
    chunks = chunk_by_lines_with_first_chunk_max(text, max_chars=max_chars, first_chunk_max_chars=first_chunk_max_chars)
    assert len(chunks) >= 2, "fixture must be multi-chunk to exercise per-chunk + merge"


def _run_pipeline_for_text(input_text: str, fmt: FormatConfig, llm: LLMConfig, *, base_work_dir: Path) -> str:
    with tempfile.TemporaryDirectory(dir=str(base_work_dir)) as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"

        job = GLOBAL_JOBS.create("in.txt", "out.txt", total_chunks=0)
        job_id = job.job_id
        try:
            GLOBAL_JOBS.update(job_id, work_dir=str(work_dir), output_path=str(out_path), cleanup_debug_dir=True)
            runner.run_job(job_id, input_text, fmt, llm)
            st = GLOBAL_JOBS.get(job_id)
            assert st is not None
            assert st.state == "done", st.error
            return out_path.read_text(encoding="utf-8")
        finally:
            GLOBAL_JOBS.delete(job_id)


def _case_dirs() -> list[Path]:
    root = Path(__file__).resolve().parent / "fixtures" / "pipeline_corpus" / "cases"
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()])


@pytest.mark.parametrize("case_dir", _case_dirs(), ids=lambda p: p.name)
def test_pipeline_corpus_golden(case_dir: Path, pytestconfig: Config, monkeypatch: pytest.MonkeyPatch):
    case_meta = _read_json_object(case_dir / "case.json")
    input_text = _read_text(case_dir / "input.txt")
    expected_path = case_dir / "expected.txt"

    fmt_meta = case_meta.get("format") or {}
    fmt = dc_replace(FormatConfig(), **fmt_meta)
    _assert_is_multichunk(input_text, fmt)

    llm = LLMConfig(base_url="http://example.com", model="m", max_concurrency=1)

    def fake_call(_cfg: LLMConfig, input_text: str, *, should_stop=None, on_retry=None):  # noqa: ANN001
        return (LLMTextResult(text=input_text, raw_text="RAW"), 0, None, None)

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call)

    merged = _run_pipeline_for_text(input_text, fmt, llm, base_work_dir=case_dir)
    _assert_invariants(merged)

    update = bool(pytestconfig.getoption("--update-golden"))
    if update or not expected_path.exists():
        expected_path.write_text(merged, encoding="utf-8")
        return

    expected = expected_path.read_text(encoding="utf-8")
    assert merged == expected

