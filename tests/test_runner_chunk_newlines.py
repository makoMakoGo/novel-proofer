from __future__ import annotations

from novel_proofer.runner import _align_trailing_newlines


def test_align_trailing_newlines_restores_missing_blank_line() -> None:
    pre = "上一段落。\n\n"
    llm = "上一段落。\n"
    assert _align_trailing_newlines(pre, llm) == "上一段落。\n\n"


def test_align_trailing_newlines_adds_missing_newline() -> None:
    pre = "上一段落。\n"
    llm = "上一段落。"
    assert _align_trailing_newlines(pre, llm) == "上一段落。\n"


def test_align_trailing_newlines_trims_excess_newlines() -> None:
    pre = "上一段落。\n"
    llm = "上一段落。\n\n\n"
    assert _align_trailing_newlines(pre, llm) == "上一段落。\n"


def test_align_trailing_newlines_normalizes_crlf() -> None:
    pre = "上一段落。\r\n\r\n"
    llm = "上一段落。\r\n"
    assert _align_trailing_newlines(pre, llm) == "上一段落。\n\n"

