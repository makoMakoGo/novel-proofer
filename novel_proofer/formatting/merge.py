from __future__ import annotations

import uuid
from collections.abc import Iterable
from io import StringIO
from pathlib import Path
from typing import TextIO


def _normalize_newlines(text: str) -> str:
    if "\r" not in text:
        return text
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _iter_normalized_lines_for_merge(text: str) -> list[str]:
    """Normalize text into lines for final merge.

    - Normalizes CRLF/CR to LF.
    - Treats whitespace-only lines as blank lines.
    - Trims trailing whitespace on non-blank lines.
    - Preserves explicit blank lines (including multiple) inside the chunk.
    """

    text = _normalize_newlines(text)
    had_trailing_newline = text.endswith("\n")
    lines = text.split("\n")
    if had_trailing_newline and lines:
        # Drop the implicit last empty element created by split("\n") when text endswith "\n".
        lines.pop()

    out: list[str] = []
    for line in lines:
        if line.strip() == "":
            out.append("")
        else:
            out.append(line.rstrip())
    return out


def merge_text_chunks(chunks: Iterable[tuple[str, bool]], writer: TextIO) -> None:
    """Merge chunk texts into a single stream.

    Each chunk is a tuple of (text, is_last_chunk). The merge algorithm:
    - Normalizes CRLF/CR to LF.
    - Ensures a blank line between adjacent non-blank lines (paragraph separation),
      especially across chunk boundaries.
    - Preserves explicit blank lines inside chunks.
    - Preserves the final newline only if the last chunk ended with one.
    """

    prev_nonblank = False
    for chunk_text, is_last in chunks:
        lines = _iter_normalized_lines_for_merge(chunk_text)
        keep_final_newline = chunk_text.endswith("\n") or chunk_text.endswith("\r")
        last_line_idx = len(lines) - 1

        for j, line in enumerate(lines):
            if line == "":
                writer.write("\n")
                prev_nonblank = False
                continue

            if prev_nonblank:
                writer.write("\n")
            writer.write(line)
            if not (is_last and not keep_final_newline and j == last_line_idx):
                writer.write("\n")
            prev_nonblank = True


def merge_text_chunks_to_path(chunks: Iterable[tuple[str, bool]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + f".{uuid.uuid4().hex}.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        merge_text_chunks(chunks, f)
    tmp.replace(out_path)


def merge_text_parts(parts: list[str]) -> str:
    buf = StringIO()
    merge_text_chunks(((p, i == len(parts) - 1) for i, p in enumerate(parts)), buf)
    return buf.getvalue()
