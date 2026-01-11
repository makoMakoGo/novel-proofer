from __future__ import annotations

from novel_proofer.formatting.chunking import chunk_by_lines, chunk_by_lines_with_first_chunk_max


def test_chunk_by_lines_max_chars_non_positive_returns_whole_text() -> None:
    text = "a\nb\n"
    assert chunk_by_lines(text, max_chars=0) == [text]
    assert chunk_by_lines(text, max_chars=-1) == [text]


def test_chunk_by_lines_empty_text_returns_single_empty_chunk() -> None:
    assert chunk_by_lines("", max_chars=10) == [""]


def test_chunk_by_lines_prefers_blank_line_break() -> None:
    text = "aaa\n\nbbb\n"
    # Force a split right before "bbb\n", preferring the last blank line.
    assert chunk_by_lines(text, max_chars=6) == ["aaa\n\n", "bbb\n"]


def test_chunk_by_lines_flushes_at_boundary_when_over_budget() -> None:
    text = "aaa\n\nbbb\n"
    # When reaching the budget exactly on a blank line, flush immediately.
    assert chunk_by_lines(text, max_chars=5) == ["aaa\n\n", "bbb\n"]


def test_chunk_by_lines_flushes_all_when_no_blank_line_available() -> None:
    text = "aaa\nbbb\nccc\n"
    out = chunk_by_lines(text, max_chars=6)
    assert out == ["aaa\n", "bbb\n", "ccc\n"]


def test_chunk_by_lines_flush_upto_leaves_tail() -> None:
    # Ensure flush_upto() leaves a non-empty tail so internal blank-line tracking
    # re-scans the remaining buffer.
    text = "aa\n\nbb\ncc\n"
    assert chunk_by_lines(text, max_chars=7) == ["aa\n\n", "bb\ncc\n"]


def test_chunk_by_lines_with_first_chunk_max_uses_larger_budget_for_first_chunk() -> None:
    text = "aaa\n\nbbbb\n\ncccc\n\ndddd\n"
    out = chunk_by_lines_with_first_chunk_max(text, max_chars=6, first_chunk_max_chars=12)
    assert out == ["aaa\n\nbbbb\n\n", "cccc\n\n", "dddd\n"]
