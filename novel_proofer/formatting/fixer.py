from __future__ import annotations

from dataclasses import dataclass

from novel_proofer.formatting.chunking import chunk_by_lines
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.formatting.rules import apply_rules
from novel_proofer.llm.client import call_llm_text_resilient
from novel_proofer.llm.config import LLMConfig


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
    chunks = chunk_by_lines(text, max_chars=max(2_000, int(config.max_chunk_chars)))

    out_parts: list[str] = []
    for chunk in chunks:
        fixed, chunk_stats = apply_rules(chunk, config)
        if llm is not None and llm.enabled:
            fixed = call_llm_text_resilient(llm, fixed)
            stats["llm_chunks"] = stats.get("llm_chunks", 0) + 1
        out_parts.append(fixed)
        for k, v in chunk_stats.items():
            stats[k] = stats.get(k, 0) + v

    return FormatResult(text=_merge_text_parts(out_parts), stats=stats)
