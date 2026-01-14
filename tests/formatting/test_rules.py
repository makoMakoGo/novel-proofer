from __future__ import annotations

from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules


def test_apply_rules_all_transforms_and_stats() -> None:
    cfg = FormatConfig(
        max_chunk_chars=2000,
        paragraph_indent=True,
        indent_with_fullwidth_space=False,
        normalize_blank_lines=True,
        trim_trailing_spaces=True,
        normalize_ellipsis=True,
        normalize_em_dash=True,
        normalize_cjk_punctuation=True,
        fix_cjk_punct_spacing=True,
        normalize_quotes=True,
    )

    text = (
        "  第1章  开始\r\n"
        "  PROLOGUE\r\n"
        '他说:"你好", "世界"....\r\n'
        "你好,世界;今:天?好!吗.\r\n"
        "你好(世界) \r\n"
        "你好 ， 世界 ！ \r\n"
        "   \r\n"
        "  已有缩进的行。\r\n"
        "===\r\n"
        "！\r\n"
        "\r\n"
        f"{'这' * 50}。\t  \r\n"
        "\r\n\r\n\r\n\r\n"
        "还有。。。以及………以及--以及———\r\n"
        'He said "hello".\r\n'
        '他说:"未闭合\r\n'
    )

    out, stats = apply_rules(text, cfg)

    # Newlines normalized.
    assert "\r" not in out

    # Trailing spaces trimmed.
    assert "\t" not in out

    # Blank lines collapsed.
    assert "\n\n\n\n" not in out

    # Ellipsis and em-dash normalized.
    assert "...." not in out
    assert "。。。" not in out
    assert "………" not in out
    assert "--" not in out
    assert "……" in out
    assert "——" in out

    # CJK punctuation normalized and spacing fixed.
    assert "你好，世界；今：天？好！吗。" in out
    assert "你好（世界）" in out
    assert "你好，世界！" in out

    # Quotes normalized only on CJK lines with even quotes.
    assert "“你好”" in out
    assert 'He said "hello".' in out

    # Paragraph indent: titles stripped, normal paragraphs indented.
    lines = out.split("\n")
    assert lines[0].startswith("第1章")
    assert lines[1] == "PROLOGUE"
    assert any(line.startswith("  ") and line.endswith("。") and len(line.strip()) > 40 for line in lines)
    assert "\n===\n" in out
    assert "\n！\n" in out

    # Stats: keys present with positive counts.
    for key in (
        "normalize_newlines",
        "trim_trailing_spaces",
        "normalize_blank_lines",
        "normalize_ellipsis",
        "normalize_em_dash",
        "normalize_cjk_punctuation",
        "fix_cjk_punct_spacing",
        "normalize_quotes",
        "paragraph_indent",
    ):
        assert stats.get(key, 0) > 0


def test_apply_rules_fullwidth_indent() -> None:
    cfg = FormatConfig(
        max_chunk_chars=2000,
        paragraph_indent=True,
        indent_with_fullwidth_space=True,
        normalize_blank_lines=False,
        trim_trailing_spaces=False,
        normalize_ellipsis=False,
        normalize_em_dash=False,
        normalize_cjk_punctuation=False,
        fix_cjk_punct_spacing=False,
        normalize_quotes=False,
    )
    out, stats = apply_rules(("啊" * 41) + "\n", cfg)
    assert out.startswith("\u3000\u3000")
    assert stats.get("paragraph_indent", 0) > 0


def test_paragraph_indent_mid_para_no_indent() -> None:
    """Mid-paragraph lines (after non-blank) should NOT be indented."""
    cfg = FormatConfig(
        max_chunk_chars=2000,
        paragraph_indent=True,
        indent_with_fullwidth_space=True,
    )
    # Simulate LLM splitting a long paragraph into multiple lines.
    text = (
        "　　当然由于早晨的缘故，纳兰嫣然还没有进行一天的修炼与运动，所以这时候纳兰嫣然的脚丫还不算特别臭，\n"
        "　　但萧炎凭借着过去的经验，清楚地知道随着修炼和运动，脚底味道就会变得恐怖了……\n"
    )
    out, _ = apply_rules(text, cfg)
    lines = out.rstrip("\n").split("\n")
    # First line should be indented (paragraph start).
    assert lines[0].startswith("\u3000\u3000")
    # Second line should NOT be indented (mid-paragraph).
    assert not lines[1].startswith("\u3000")
    assert not lines[1].startswith("  ")


def test_paragraph_indent_after_blank_line() -> None:
    """A line after a blank line should be indented (new paragraph)."""
    cfg = FormatConfig(
        max_chunk_chars=2000,
        paragraph_indent=True,
        indent_with_fullwidth_space=True,
    )
    text = "　　第一段落。\n\n第二段落开头没有缩进。\n"
    out, _ = apply_rules(text, cfg)
    lines = out.rstrip("\n").split("\n")
    # First paragraph indented.
    assert lines[0].startswith("\u3000\u3000")
    # Blank line preserved.
    assert lines[1] == ""
    # Second paragraph should be indented (after blank).
    assert lines[2].startswith("\u3000\u3000")


def test_paragraph_indent_mixed_cjk_ascii_not_title() -> None:
    """Mixed CJK + ASCII (e.g., '（你纯M啊）') should NOT be treated as an English all-caps title."""
    cfg = FormatConfig(
        max_chunk_chars=2000,
        paragraph_indent=True,
        indent_with_fullwidth_space=True,
    )
    text = "　　上一段。\n\n（你纯M啊）\n"
    out, _ = apply_rules(text, cfg)
    lines = out.rstrip("\n").split("\n")
    assert lines[2].startswith("\u3000\u3000（你纯M啊）")
