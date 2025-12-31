from __future__ import annotations


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
        last_blank_idx = None
        for i, line in enumerate(buf):
            if line.strip() == "":
                last_blank_idx = i

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
