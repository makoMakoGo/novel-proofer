from __future__ import annotations

from pathlib import Path

from novel_proofer.runner import _align_trailing_newlines
from novel_proofer.runner import _merge_chunk_outputs


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


def test_merge_chunk_outputs_inserts_blank_line_between_chunks(tmp_path: Path) -> None:
    work_dir = tmp_path / "job"
    (work_dir / "out").mkdir(parents=True, exist_ok=True)

    # Two paragraphs split across chunk boundary without an explicit blank line.
    (work_dir / "out" / "000000.txt").write_text("　　第一段。\n", encoding="utf-8")
    (work_dir / "out" / "000001.txt").write_text("　　第二段。\n", encoding="utf-8")

    out_path = tmp_path / "merged.txt"
    _merge_chunk_outputs(work_dir, total_chunks=2, out_path=out_path)

    assert out_path.read_text(encoding="utf-8") == "　　第一段。\n\n　　第二段。\n"


def test_merge_chunk_outputs_inserts_blank_line_within_single_chunk(tmp_path: Path) -> None:
    work_dir = tmp_path / "job"
    (work_dir / "out").mkdir(parents=True, exist_ok=True)

    # Two non-blank lines adjacent in one chunk should be separated by a blank line.
    (work_dir / "out" / "000000.txt").write_text("　　A\n　　B\n", encoding="utf-8")

    out_path = tmp_path / "merged.txt"
    _merge_chunk_outputs(work_dir, total_chunks=1, out_path=out_path)

    assert out_path.read_text(encoding="utf-8") == "　　A\n\n　　B\n"
