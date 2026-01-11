from __future__ import annotations

import pytest

import novel_proofer.formatting.fixer as fixer
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.llm.config import FIRST_CHUNK_SYSTEM_PROMPT_PREFIX, LLMConfig


def test_format_txt_llm_enabled_calls_llm_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call_llm_text_resilient(cfg: LLMConfig, input_text: str, *, should_stop=None, on_retry=None):  # noqa: ANN001
        return input_text + "LLM"

    monkeypatch.setattr(fixer, "call_llm_text_resilient", fake_call_llm_text_resilient)

    cfg = FormatConfig(max_chunk_chars=2000)
    llm = LLMConfig(enabled=True, base_url="http://example.com", model="m")

    out = fixer.format_txt("x\n", cfg, llm=llm)
    assert out.text.endswith("LLM")
    assert out.stats.get("llm_chunks") == 1


def test_format_txt_llm_enabled_keeps_front_matter_in_first_chunk_when_chunk_size_small(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_call_llm_text_resilient(cfg: LLMConfig, input_text: str, *, should_stop=None, on_retry=None):  # noqa: ANN001
        calls.append((cfg.system_prompt, input_text))
        return input_text

    monkeypatch.setattr(fixer, "call_llm_text_resilient", fake_call_llm_text_resilient)

    cfg = FormatConfig(max_chunk_chars=200)
    llm = LLMConfig(enabled=True, base_url="http://example.com", model="m")

    header = "作者：X\n标签：Y\n内容简介：Z\n\n"
    body = ("正文段落。\n\n" * 500).strip() + "\n"
    fixer.format_txt(header + body, cfg, llm=llm)

    assert len(calls) >= 2
    assert calls[0][0].startswith(FIRST_CHUNK_SYSTEM_PROMPT_PREFIX)
    assert "作者" in calls[0][1] and "标签" in calls[0][1] and "内容简介" in calls[0][1]
    assert not calls[1][0].startswith(FIRST_CHUNK_SYSTEM_PROMPT_PREFIX)
    assert "作者" not in calls[1][1] and "标签" not in calls[1][1] and "内容简介" not in calls[1][1]
