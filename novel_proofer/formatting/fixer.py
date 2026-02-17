from __future__ import annotations

from dataclasses import dataclass

from novel_proofer.formatting.chunking import chunk_by_lines_with_first_chunk_max
from novel_proofer.formatting.config import FormatConfig, clamp_chunk_params
from novel_proofer.formatting.merge import merge_text_parts
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.llm.client import call_llm_text_resilient
from novel_proofer.llm.config import LLMConfig, build_first_chunk_config


@dataclass
class FormatResult:
    text: str
    stats: dict[str, int]


def format_txt(text: str, config: FormatConfig, llm: LLMConfig) -> FormatResult:
    stats: dict[str, int] = {}
    max_chars, first_chunk_max_chars = clamp_chunk_params(config.max_chunk_chars)
    chunks = chunk_by_lines_with_first_chunk_max(text, max_chars=max_chars, first_chunk_max_chars=first_chunk_max_chars)

    out_parts: list[str] = []
    for i, chunk in enumerate(chunks):
        fixed, chunk_stats = apply_rules(chunk, config)
        llm_cfg = build_first_chunk_config(llm) if i == 0 else llm
        fixed = call_llm_text_resilient(llm_cfg, fixed)
        stats["llm_chunks"] = stats.get("llm_chunks", 0) + 1
        out_parts.append(fixed)
        for k, v in chunk_stats.items():
            stats[k] = stats.get(k, 0) + v

    return FormatResult(text=merge_text_parts(out_parts), stats=stats)
