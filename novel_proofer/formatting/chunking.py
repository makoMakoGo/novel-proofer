from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path


def chunk_by_lines(text: str, max_chars: int) -> list[str]:
    if max_chars <= 0:
        return [text]

    lines = text.splitlines(keepends=True)
    if not lines:
        return [""]

    chunks: list[str] = []
    buf: list[str] = []
    size = 0
    last_blank_idx: int | None = None

    def flush_all() -> None:
        nonlocal buf, size, last_blank_idx
        if buf:
            chunks.append("".join(buf))
        buf = []
        size = 0
        last_blank_idx = None

    def flush_upto(end_idx_inclusive: int) -> None:
        nonlocal buf, size, last_blank_idx
        if end_idx_inclusive < 0:
            return
        head = buf[: end_idx_inclusive + 1]
        tail = buf[end_idx_inclusive + 1 :]
        if head:
            chunks.append("".join(head))
        buf = tail
        size = sum(len(x) for x in buf)
        if last_blank_idx is None:
            return
        if last_blank_idx <= end_idx_inclusive:
            last_blank_idx = None
        else:
            last_blank_idx -= end_idx_inclusive + 1

    for line in lines:
        if buf and size + len(line) > max_chars:
            # Prefer breaking at the last blank line (paragraph boundary)
            if last_blank_idx is not None and last_blank_idx >= 0:
                flush_upto(last_blank_idx)
            else:
                flush_all()

        buf.append(line)
        size += len(line)
        if line.strip() == "":
            last_blank_idx = len(buf) - 1

        # If we are already over budget, flush at boundary ASAP.
        if size >= max_chars and last_blank_idx is not None:
            flush_upto(last_blank_idx)

    flush_all()
    return chunks


def chunk_by_lines_with_first_chunk_max(text: str, *, max_chars: int, first_chunk_max_chars: int) -> list[str]:
    """Chunk text by lines, allowing a larger first chunk budget.

    This is useful when the first chunk needs to carry additional context
    (e.g. front-matter that must be cleaned with a different prompt).
    """
    if max_chars <= 0:
        return [text]

    # Fallback to the standard behavior when the first chunk budget is not larger.
    if first_chunk_max_chars <= max_chars or first_chunk_max_chars <= 0:
        return chunk_by_lines(text, max_chars=max_chars)

    first_pass = chunk_by_lines(text, max_chars=first_chunk_max_chars)
    if not first_pass:
        return [text]

    first = first_pass[0]
    rest_text = "".join(first_pass[1:])
    if rest_text == "":
        return [first]

    rest = chunk_by_lines(rest_text, max_chars=max_chars)
    if rest == [""]:
        return [first]
    return [first, *rest]


def iter_chunks_by_lines_with_first_chunk_max_from_file(
    path: Path,
    *,
    max_chars: int,
    first_chunk_max_chars: int,
    encoding: str = "utf-8",
) -> Iterator[str]:
    """Stream-chunk a text file by lines, preferring blank-line boundaries.

    This is the file-based equivalent of chunk_by_lines_with_first_chunk_max(), but it
    avoids loading the entire input into memory.
    """

    if max_chars <= 0:
        yield path.read_text(encoding=encoding)
        return

    budget = max_chars
    if first_chunk_max_chars > max_chars and first_chunk_max_chars > 0:
        budget = first_chunk_max_chars

    first_chunk_emitted = False

    buf: list[str] = []
    size = 0
    last_blank_idx: int | None = None
    saw_any_line = False

    def _flush_all() -> list[str]:
        nonlocal buf, size, last_blank_idx
        if not buf:
            return []
        out = ["".join(buf)]
        buf = []
        size = 0
        last_blank_idx = None
        return out

    def _flush_upto(end_idx_inclusive: int) -> list[str]:
        nonlocal buf, size, last_blank_idx
        if end_idx_inclusive < 0:
            return []
        head = buf[: end_idx_inclusive + 1]
        tail = buf[end_idx_inclusive + 1 :]
        out = ["".join(head)] if head else []
        buf = tail
        size = sum(len(x) for x in buf)
        if last_blank_idx is None:
            return out
        if last_blank_idx <= end_idx_inclusive:
            last_blank_idx = None
        else:
            last_blank_idx -= end_idx_inclusive + 1
        return out

    def _on_emit() -> None:
        nonlocal first_chunk_emitted, budget
        if not first_chunk_emitted:
            first_chunk_emitted = True
            budget = max_chars

    with path.open("r", encoding=encoding) as f:
        for line in f:
            saw_any_line = True

            if buf and size + len(line) > budget:
                # Prefer breaking at the last blank line (paragraph boundary)
                if last_blank_idx is not None and last_blank_idx >= 0:
                    for c in _flush_upto(last_blank_idx):
                        _on_emit()
                        yield c
                else:
                    for c in _flush_all():
                        _on_emit()
                        yield c

            buf.append(line)
            size += len(line)
            if line.strip() == "":
                last_blank_idx = len(buf) - 1

            # If we are already over budget, flush at boundary ASAP.
            if size >= budget and last_blank_idx is not None:
                for c in _flush_upto(last_blank_idx):
                    _on_emit()
                    yield c

    if not saw_any_line:
        yield ""
        return

    for c in _flush_all():
        _on_emit()
        yield c
