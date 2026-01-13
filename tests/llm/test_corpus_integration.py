from __future__ import annotations

from dataclasses import replace as dc_replace
from pathlib import Path

import pytest

from novel_proofer.formatting.config import FormatConfig
from tests.conftest import llm_config_from_env
from tests.support.corpus_cases import (
    assert_substrings_in_order,
    count_leading_blank_lines,
    list_case_dirs,
    read_json_object,
    read_text,
    write_failure_artifacts,
)
from tests.support.pipeline_runner import run_pipeline_for_text
from tests.support.text_assertions import (
    assert_common_text_invariants,
    assert_no_trailing_spaces,
    assert_no_triple_blank_lines,
    assert_paragraph_indent_rules,
)


def _cases_root() -> Path:
    return Path(__file__).resolve().parents[1] / "cases" / "llm"


def _artifact_root() -> Path:
    return Path(__file__).resolve().parents[1] / ".artifacts"


@pytest.mark.llm_integration
@pytest.mark.parametrize("case_dir", list_case_dirs(_cases_root()), ids=lambda p: p.name)
def test_llm_corpus_end_to_end_invariants(case_dir: Path) -> None:
    case_meta = read_json_object(case_dir / "case.json")
    input_text = read_text(case_dir / "input.txt")

    fmt_meta = case_meta.get("format") or {}
    fmt = dc_replace(FormatConfig(), **fmt_meta)

    base_llm = llm_config_from_env()
    llm_meta = case_meta.get("llm") or {}
    llm = dc_replace(
        base_llm,
        temperature=float(llm_meta.get("temperature", base_llm.temperature)),
        timeout_seconds=float(llm_meta.get("timeout_seconds", base_llm.timeout_seconds)),
        extra_params=llm_meta.get("extra_params", base_llm.extra_params),
        max_concurrency=1,
    )

    merged = run_pipeline_for_text(input_text, fmt, llm, base_work_dir=case_dir)

    try:
        assert_common_text_invariants(merged)
        if fmt.trim_trailing_spaces:
            assert_no_trailing_spaces(merged)
        if fmt.normalize_blank_lines:
            assert_no_triple_blank_lines(merged)
        assert_paragraph_indent_rules(merged, fmt)

        assertions = case_meta.get("assertions") or {}

        must_contain = assertions.get("must_contain") or []
        for s in must_contain:
            assert s in merged, f"[{case_dir.name}] missing: {s!r}"

        must_not_contain = assertions.get("must_not_contain") or []
        for s in must_not_contain:
            assert s not in merged, f"[{case_dir.name}] must not contain: {s!r}"

        ordered_substrings = assertions.get("ordered_substrings") or []
        assert_substrings_in_order(merged, list(ordered_substrings), case_name=case_dir.name)

        leading_blank_lines = assertions.get("leading_blank_lines")
        if leading_blank_lines is not None:
            want = int(leading_blank_lines)
            have = count_leading_blank_lines(merged)
            assert have == want, f"[{case_dir.name}] leading blank lines: {have} != {want}"
    except AssertionError as e:
        art_dir = write_failure_artifacts(
            artifacts_root=_artifact_root(),
            suite="llm_corpus",
            case_name=case_dir.name,
            input_text=input_text,
            output_text=merged,
            meta={
                "case": {k: v for k, v in case_meta.items() if k != "llm"},
                "llm": {
                    "base_url": llm.base_url,
                    "model": llm.model,
                    "temperature": llm.temperature,
                    "timeout_seconds": llm.timeout_seconds,
                    "max_concurrency": llm.max_concurrency,
                    "extra_params": llm.extra_params,
                },
            },
        )
        raise AssertionError(f"{e}\n\nArtifacts: {art_dir}") from e

