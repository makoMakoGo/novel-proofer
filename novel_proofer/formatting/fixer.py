from __future__ import annotations

from dataclasses import dataclass, replace

from novel_proofer.formatting.chunking import chunk_by_lines_with_first_chunk_max
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.llm.client import call_llm_text_resilient
from novel_proofer.llm.config import FIRST_CHUNK_SYSTEM_PROMPT_PREFIX, LLMConfig


@dataclass
class FormatResult:
    text: str
    stats: dict[str, int]


def _merge_text_parts(parts: list[str]) -> str:
    out: list[str] = []
    prev_nonblank = False
    keep_final_newline = True

    for part in parts:
        keep_final_newline = part.endswith("\n") or part.endswith("\r")
        if "\r" in part:
            part = part.replace("\r\n", "\n").replace("\r", "\n")

        had_trailing_newline = part.endswith("\n")
        lines = part.split("\n")
        if had_trailing_newline and lines:
            lines.pop()

        for line in lines:
            if line.strip() == "":
                out.append("\n")
                prev_nonblank = False
                continue

            if prev_nonblank:
                out.append("\n")

            out.append(line.rstrip())
            out.append("\n")
            prev_nonblank = True

    merged = "".join(out)
    if parts and not keep_final_newline and merged.endswith("\n"):
        merged = merged[:-1]
    return merged


def format_txt(text: str, config: FormatConfig, llm: LLMConfig | None = None) -> FormatResult:
    stats: dict[str, int] = {}
    max_chars = int(config.max_chunk_chars)
    max_chars = max(200, min(4_000, max_chars))
    first_chunk_max_chars = max_chars
    if llm is not None and llm.enabled:
        first_chunk_max_chars = min(4_000, max(first_chunk_max_chars, 2_000))
    chunks = chunk_by_lines_with_first_chunk_max(text, max_chars=max_chars, first_chunk_max_chars=first_chunk_max_chars)

    out_parts: list[str] = []
    for i, chunk in enumerate(chunks):
        fixed, chunk_stats = apply_rules(chunk, config)
        if llm is not None and llm.enabled:
            llm_cfg = llm
            if i == 0:
                llm_cfg = replace(llm, system_prompt=FIRST_CHUNK_SYSTEM_PROMPT_PREFIX + "\n\n" + llm.system_prompt)
            fixed = call_llm_text_resilient(llm_cfg, fixed)
            stats["llm_chunks"] = stats.get("llm_chunks", 0) + 1
        out_parts.append(fixed)
        for k, v in chunk_stats.items():
            stats[k] = stats.get(k, 0) + v

    return FormatResult(text=_merge_text_parts(out_parts), stats=stats)
