from __future__ import annotations

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import _is_chapter_title


def assert_common_text_invariants(text: str) -> None:
    # Newlines normalized
    assert "\r" not in text
    # No tabs
    assert "\t" not in text
    # Never keep think tags in final output
    assert "<think" not in text.lower()


def assert_no_trailing_spaces(text: str) -> None:
    for i, line in enumerate(text.split("\n"), start=1):
        if line.endswith(" ") or line.endswith("\t"):
            raise AssertionError(f"line {i} has trailing whitespace")


def assert_no_triple_blank_lines(text: str) -> None:
    if "\n\n\n" in text:
        raise AssertionError("contains 2+ consecutive blank lines (\\n\\n\\n)")


def assert_paragraph_indent_rules(text: str, fmt: FormatConfig) -> None:
    if not fmt.paragraph_indent:
        return

    indent = "　　" if fmt.indent_with_fullwidth_space else "  "
    lines = text.split("\n")
    for i, line in enumerate(lines, start=1):
        if line.strip() == "":
            continue

        if _is_chapter_title(line):
            if line != line.lstrip():
                raise AssertionError(f"line {i} chapter title must not be indented: {line!r}")
            continue

        stripped = line.strip()
        if stripped and all(ch in "-=*_—" for ch in stripped) and len(stripped) >= 3:
            continue

        is_para_start = i == 1 or not lines[i - 2].strip()
        if is_para_start:
            # Match production behavior: very short non-paragraph lines may remain unindented.
            if stripped and len(stripped) >= 2 and not line.startswith(indent):
                raise AssertionError(f"line {i} must start with paragraph indent {indent!r}: {line!r}")
        else:
            if line.startswith(indent):
                raise AssertionError(f"line {i} mid-paragraph must not be indented: {line!r}")
