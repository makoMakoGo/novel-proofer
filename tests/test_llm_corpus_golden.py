from __future__ import annotations

import json
import tempfile
from dataclasses import replace as dc_replace
from pathlib import Path

from _pytest.config import Config

import pytest

import novel_proofer.runner as runner
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.jobs import GLOBAL_JOBS
from novel_proofer.llm.config import LLMConfig
from tests.conftest import llm_config_from_env


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
    # Newlines normalized
    assert "\r" not in text
    # Never keep think tags in final output
    assert "<think" not in text.lower()
    # No tabs
    assert "\t" not in text


def _run_pipeline_for_text(
    input_text: str,
    fmt: FormatConfig,
    llm: LLMConfig,
    *,
    base_work_dir: Path,
    input_filename: str = "in.txt",
    output_filename: str = "out.txt",
) -> str:
    """Run the real runner pipeline and return final output text."""

    with tempfile.TemporaryDirectory(dir=str(base_work_dir)) as td:
        work_dir = Path(td) / "work"
        out_path = Path(td) / "out.txt"

        job = GLOBAL_JOBS.create(input_filename, output_filename, total_chunks=0)
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
    root = Path(__file__).resolve().parent / "fixtures" / "llm_corpus" / "cases"
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()])


@pytest.mark.llm_integration
@pytest.mark.parametrize("case_dir", _case_dirs(), ids=lambda p: p.name)
def test_llm_corpus_golden_end_to_end(case_dir: Path, pytestconfig: Config):
    case_meta = _read_json_object(case_dir / "case.json")
    input_text = _read_text(case_dir / "input.txt")
    expected_path = case_dir / "expected.txt"

    fmt_meta = case_meta.get("format") or {}
    fmt = FormatConfig(
        max_chunk_chars=int(fmt_meta.get("max_chunk_chars", 2000)),
        paragraph_indent=bool(fmt_meta.get("paragraph_indent", True)),
        indent_with_fullwidth_space=bool(fmt_meta.get("indent_with_fullwidth_space", True)),
        normalize_blank_lines=bool(fmt_meta.get("normalize_blank_lines", True)),
        trim_trailing_spaces=bool(fmt_meta.get("trim_trailing_spaces", True)),
        normalize_ellipsis=bool(fmt_meta.get("normalize_ellipsis", True)),
        normalize_em_dash=bool(fmt_meta.get("normalize_em_dash", True)),
        normalize_cjk_punctuation=bool(fmt_meta.get("normalize_cjk_punctuation", True)),
        fix_cjk_punct_spacing=bool(fmt_meta.get("fix_cjk_punct_spacing", True)),
        normalize_quotes=bool(fmt_meta.get("normalize_quotes", False)),
    )

    base_llm = llm_config_from_env()
    # Apply per-case LLM overrides.
    llm_meta = case_meta.get("llm") or {}
    llm = dc_replace(
        base_llm,
        temperature=float(llm_meta.get("temperature", base_llm.temperature)),
        timeout_seconds=float(llm_meta.get("timeout_seconds", base_llm.timeout_seconds)),
        extra_params=llm_meta.get("extra_params", base_llm.extra_params),
        max_concurrency=1,
    )

    merged = _run_pipeline_for_text(input_text, fmt, llm, base_work_dir=case_dir)
    _assert_invariants(merged)

    update = bool(pytestconfig.getoption("--update-golden"))
    if update or not expected_path.exists():
        expected_path.write_text(merged, encoding="utf-8")
        return

    expected = expected_path.read_text(encoding="utf-8")
    assert merged == expected
