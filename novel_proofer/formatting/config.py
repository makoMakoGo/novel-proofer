from __future__ import annotations

from dataclasses import dataclass

_CHUNK_CHARS_MIN = 200
_CHUNK_CHARS_MAX = 4_000
_FIRST_CHUNK_CHARS_MIN = 2_000


@dataclass(frozen=True)
class FormatConfig:
    # Chunk size matters mainly when LLM is enabled.
    # For purely local rules, we still process in chunks to cap memory peaks.
    # LLM 默认更小分片，降低 504 风险。
    max_chunk_chars: int = 2_000

    # Layout rules
    paragraph_indent: bool = True
    indent_with_fullwidth_space: bool = True
    normalize_blank_lines: bool = True
    trim_trailing_spaces: bool = True

    # Punctuation rules
    normalize_ellipsis: bool = True
    normalize_em_dash: bool = True
    normalize_cjk_punctuation: bool = True
    fix_cjk_punct_spacing: bool = True

    # Potentially ambiguous; default off.
    normalize_quotes: bool = False


def clamp_chunk_params(max_chunk_chars: int) -> tuple[int, int]:
    """Clamp and derive chunk size parameters.

    Returns (max_chars, first_chunk_max_chars).
    """
    max_chars = max(_CHUNK_CHARS_MIN, min(_CHUNK_CHARS_MAX, int(max_chunk_chars)))
    first_chunk_max_chars = min(_CHUNK_CHARS_MAX, max(max_chars, _FIRST_CHUNK_CHARS_MIN))
    return max_chars, first_chunk_max_chars
