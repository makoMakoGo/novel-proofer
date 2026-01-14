from __future__ import annotations

from dataclasses import replace as dc_replace
from pathlib import Path

import pytest

import novel_proofer.runner as runner
from novel_proofer.formatting.chunking import chunk_by_lines_with_first_chunk_max
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import is_separator_line
from novel_proofer.llm.client import LLMTextResult
from novel_proofer.llm.config import LLMConfig
from tests.conftest import should_update_golden
from tests.support.corpus_cases import list_case_dirs, read_json_object, read_text, write_failure_artifacts
from tests.support.pipeline_runner import run_pipeline_for_text
from tests.support.text_assertions import (
    assert_common_text_invariants,
    assert_no_trailing_spaces,
    assert_no_triple_blank_lines,
    assert_paragraph_indent_rules,
)


def _cases_root() -> Path:
    return Path(__file__).resolve().parents[1] / "cases" / "pipeline"


def _artifact_root() -> Path:
    return Path(__file__).resolve().parents[1] / ".artifacts"


def _assert_is_multichunk(text: str, fmt: FormatConfig) -> None:
    max_chars = max(200, min(4_000, int(fmt.max_chunk_chars)))
    first_chunk_max_chars = min(4_000, max(max_chars, 2_000))
    chunks = chunk_by_lines_with_first_chunk_max(text, max_chars=max_chars, first_chunk_max_chars=first_chunk_max_chars)
    assert len(chunks) >= 2, "fixture must be multi-chunk to exercise per-chunk + merge"


@pytest.mark.parametrize("case_dir", list_case_dirs(_cases_root()), ids=lambda p: p.name)
def test_pipeline_corpus_golden(case_dir: Path, pytestconfig: pytest.Config, monkeypatch: pytest.MonkeyPatch) -> None:
    case_meta = read_json_object(case_dir / "case.json")
    input_text = read_text(case_dir / "input.txt")
    expected_path = case_dir / "expected.txt"

    fmt_meta = case_meta.get("format") or {}
    fmt = dc_replace(FormatConfig(), **fmt_meta)
    _assert_is_multichunk(input_text, fmt)

    llm = LLMConfig(base_url="http://example.com", model="m", max_concurrency=1)

    def fake_call(_cfg: LLMConfig, input_text: str, *, should_stop=None, on_retry=None):
        # Mimic the global prompt cleanup: remove obvious separator lines such as "====".
        cleaned = "".join(line for line in input_text.splitlines(keepends=True) if not is_separator_line(line))
        return (LLMTextResult(text=cleaned, raw_text="RAW"), 0, None, None)

    monkeypatch.setattr(runner, "call_llm_text_resilient_with_meta_and_raw", fake_call)

    merged = run_pipeline_for_text(input_text, fmt, llm, base_work_dir=case_dir)

    assert_common_text_invariants(merged)
    if fmt.trim_trailing_spaces:
        assert_no_trailing_spaces(merged)
    if fmt.normalize_blank_lines:
        assert_no_triple_blank_lines(merged)
    assert_paragraph_indent_rules(merged, fmt)

    if should_update_golden(pytestconfig) or not expected_path.exists():
        expected_path.write_text(merged, encoding="utf-8")
        return

    expected = expected_path.read_text(encoding="utf-8")
    try:
        assert merged == expected
    except AssertionError as e:
        art_dir = write_failure_artifacts(
            artifacts_root=_artifact_root(),
            suite="pipeline_corpus",
            case_name=case_dir.name,
            input_text=input_text,
            output_text=merged,
            meta={"case": case_meta},
        )
        (art_dir / "expected.txt").write_text(expected, encoding="utf-8")
        raise AssertionError(f"{e}\n\nArtifacts: {art_dir}") from e
