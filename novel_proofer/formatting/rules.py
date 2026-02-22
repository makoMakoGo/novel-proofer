from __future__ import annotations

import re

from novel_proofer.formatting.config import FormatConfig

_FULLWIDTH_SPACE = "\u3000"

_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]")


_chapter_like_re = re.compile(
    r"^(\s|\u3000)*((第\s*[0-9一二三四五六七八九十百千两零〇]+\s*[章节回卷部集幕]\b)|((楔子|序|序章|后记|尾声|番外)\b))",
    re.IGNORECASE,
)


def _has_cjk(s: str) -> bool:
    return _CJK_RE.search(s) is not None


def is_chapter_title(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    # Common book title formats (help keep the very first title unindented).
    if (
        len(s) <= 80
        and s[-1] not in "。！？…"
        and (
            (s.startswith("《") and s.endswith("》"))
            or (s.startswith("【") and s.endswith("】"))
            or (s.endswith("】") and "【" in s)
            or (s.endswith("》") and "《" in s)
        )
    ):
        return True
    # Common patterns: 第X章 / 序章 / 番外
    if _chapter_like_re.match(line):
        return True
    # Also accept short all-caps-like headings (rare in cn novels).
    # Only apply this to lines with ASCII letters and NO CJK (avoid misclassifying cn paragraphs like "（你纯M啊）").
    return len(s) <= 40 and s.upper() == s and any(c.isascii() and c.isalpha() for c in s) and not _has_cjk(s)


_SEPARATOR_CHARS = "-=*_—"


def is_separator_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and len(stripped) >= 3 and all(ch in _SEPARATOR_CHARS for ch in stripped)


_ellipsis_ascii_re = re.compile(r"\.{3,}")
_ellipsis_cn_re = re.compile(r"[。．｡]{3,}")
_ellipsis_excess_re = re.compile(r"…{3,}")
_em_dash_re = re.compile(r"[-—]{2,}")
_trailing_spaces_re = re.compile(r"[ \t]+(?=\n)")
_blank_lines_re = re.compile(r"\n{3,}")


def apply_rules(text: str, config: FormatConfig) -> tuple[str, dict[str, int]]:
    stats: dict[str, int] = {}

    # Normalize newlines early.
    if "\r\n" in text or "\r" in text:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        stats["normalize_newlines"] = stats.get("normalize_newlines", 0) + 1

    if config.trim_trailing_spaces:
        text, n = _trailing_spaces_re.subn("", text)
        if n:
            stats["trim_trailing_spaces"] = stats.get("trim_trailing_spaces", 0) + n

    if config.normalize_blank_lines:
        text, n = _blank_lines_re.subn("\n\n", text)
        if n:
            stats["normalize_blank_lines"] = stats.get("normalize_blank_lines", 0) + n

    if config.normalize_ellipsis:
        text, n1 = _ellipsis_ascii_re.subn("……", text)
        text, n2 = _ellipsis_cn_re.subn("……", text)
        text, n3 = _ellipsis_excess_re.subn("……", text)
        n = n1 + n2 + n3
        if n:
            stats["normalize_ellipsis"] = stats.get("normalize_ellipsis", 0) + n

    if config.normalize_em_dash:
        # Chinese em dash commonly uses '——' (two U+2014). Normalize common variants.
        text, n = _em_dash_re.subn("——", text)
        if n:
            stats["normalize_em_dash"] = stats.get("normalize_em_dash", 0) + n

    if config.normalize_cjk_punctuation:
        text, n = _normalize_cjk_punctuation(text)
        if n:
            stats["normalize_cjk_punctuation"] = stats.get("normalize_cjk_punctuation", 0) + n

    if config.fix_cjk_punct_spacing:
        text, n = _fix_cjk_punct_spacing(text)
        if n:
            stats["fix_cjk_punct_spacing"] = stats.get("fix_cjk_punct_spacing", 0) + n

    if config.normalize_quotes:
        text, n = _normalize_quotes(text)
        if n:
            stats["normalize_quotes"] = stats.get("normalize_quotes", 0) + n

    if config.paragraph_indent:
        text, changed = _normalize_paragraph_indent(text, config)
        if changed:
            stats["paragraph_indent"] = stats.get("paragraph_indent", 0) + 1

    return text, stats


_CJK = r"\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af"

_num_fullwidth_dot_re = re.compile(r"(?<=\d)[\uFF0E\u3002](?=\d)")
_num_fullwidth_comma_re = re.compile(r"(?<=\d)\uFF0C(?=\d)")
_cjk_comma_after_re = re.compile(rf"(?<=[{_CJK}])(?<!\d),(?!\d)")
_cjk_comma_before_re = re.compile(rf"(?<!\d),(?!\d)(?=[{_CJK}])")
_cjk_semicolon_re = re.compile(rf"(?<=[{_CJK}]);")
_cjk_colon_re = re.compile(rf"(?<=[{_CJK}]):")
_cjk_question_re = re.compile(rf"(?<=[{_CJK}])\?")
_cjk_exclamation_re = re.compile(rf"(?<=[{_CJK}])!")
# Closing punctuation chars for period lookahead: ASCII quotes, curly quotes, brackets
_CLOSING_PUNCT = r'"\u201c\u201d\'\u2018\u2019\)\]\u3011\u300b\u300d\u300f'
_cjk_period_re = re.compile(rf"(?<=[{_CJK}])\.(?=(?:[{_CJK}]|\s|$|[{_CLOSING_PUNCT}]))")
_cjk_open_paren_after_re = re.compile(rf"(?<=[{_CJK}])\(")
_close_paren_before_cjk_re = re.compile(rf"\)(?=[{_CJK}])")
_open_paren_before_cjk_re = re.compile(rf"\((?=[{_CJK}])")
_cjk_close_paren_re = re.compile(rf"(?<=[{_CJK}])\)")
_cjk_space_before_punct_re = re.compile(rf"(?<=[{_CJK}])[ \t]+(?=[，。！？；：、,.!?;:])")
_punct_space_before_cjk_re = re.compile(rf"(?<=[，。！？；：、,.!?;:])[ \t]+(?=[{_CJK}])")


def _normalize_cjk_punctuation(text: str) -> tuple[str, int]:
    """Convert common ASCII punctuation to fullwidth when in CJK context.

    Heuristics:
    - Only convert when adjacent to CJK characters.
    - Avoid decimals like 3.14 and numbers like 1,000.
    """

    count = 0

    text, n = _num_fullwidth_dot_re.subn(".", text)
    count += n
    text, n = _num_fullwidth_comma_re.subn(",", text)
    count += n

    text, n = _cjk_comma_after_re.subn("，", text)
    count += n
    text, n = _cjk_comma_before_re.subn("，", text)
    count += n

    text, n = _cjk_semicolon_re.subn("；", text)
    count += n
    text, n = _cjk_colon_re.subn("：", text)
    count += n

    text, n = _cjk_question_re.subn("？", text)
    count += n
    text, n = _cjk_exclamation_re.subn("！", text)
    count += n

    text, n = _cjk_period_re.subn("。", text)
    count += n

    text, n = _cjk_open_paren_after_re.subn("（", text)
    count += n
    text, n = _close_paren_before_cjk_re.subn("）", text)
    count += n
    text, n = _open_paren_before_cjk_re.subn("（", text)
    count += n
    text, n = _cjk_close_paren_re.subn("）", text)
    count += n

    return text, count


def _fix_cjk_punct_spacing(text: str) -> tuple[str, int]:
    """Remove spaces between CJK characters and punctuation in CJK context."""

    count = 0

    text, n = _cjk_space_before_punct_re.subn("", text)
    count += n

    text, n = _punct_space_before_cjk_re.subn("", text)
    count += n

    return text, count


def _normalize_quotes(text: str) -> tuple[str, int]:
    """Convert straight double quotes to Chinese quotes in safe cases.

    - Only touches lines that contain CJK.
    - Only converts when the count of `"` in the line is even.
    """

    lines = text.split("\n")
    changed = 0

    for i, line in enumerate(lines):
        if '"' not in line:
            continue
        if not _has_cjk(line):
            continue
        quote_count = line.count('"')
        if quote_count < 2 or quote_count % 2 != 0:
            continue

        out: list[str] = []
        open_quote = True
        for ch in line:
            if ch == '"':
                out.append("“" if open_quote else "”")
                open_quote = not open_quote
            else:
                out.append(ch)

        new_line = "".join(out)
        if new_line != line:
            lines[i] = new_line
            changed += quote_count

    return "\n".join(lines), changed


_leading_ws_re = re.compile(r"^\s+")


def _normalize_paragraph_indent(text: str, config: FormatConfig) -> tuple[str, bool]:
    indent = (_FULLWIDTH_SPACE * 2) if config.indent_with_fullwidth_space else "  "

    lines = text.split("\n")
    changed = False

    for i, line in enumerate(lines):
        if not line:
            continue

        if is_chapter_title(line):
            # Strip leading whitespace for titles.
            new_line = _leading_ws_re.sub("", line)
            if new_line != line:
                lines[i] = new_line
                changed = True
            continue

        # Skip lines that look like separators.
        if is_separator_line(line):
            continue

        # Only indent at paragraph start (first line or after blank line).
        is_para_start = i == 0 or not lines[i - 1].strip()

        if is_para_start:
            # For paragraph starts, ensure a consistent indent.
            if line.startswith(indent):
                continue

            new_line = _leading_ws_re.sub("", line)
            # Avoid indenting very short non-paragraph lines (e.g., single punctuation)
            if new_line and len(new_line) >= 2:
                new_line = indent + new_line
                if new_line != line:
                    lines[i] = new_line
                    changed = True
        else:
            # Mid-paragraph line: strip any leading whitespace (no indent).
            new_line = _leading_ws_re.sub("", line)
            if new_line != line:
                lines[i] = new_line
                changed = True

    return "\n".join(lines), changed
