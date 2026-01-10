from __future__ import annotations

import pytest

import novel_proofer.formatting.fixer as fixer
from novel_proofer.formatting.config import FormatConfig
from novel_proofer.llm.config import LLMConfig


def test_format_txt_llm_enabled_calls_llm_and_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_call_llm_text_resilient(cfg: LLMConfig, input_text: str, *, should_stop=None, on_retry=None):  # noqa: ANN001
        return input_text + "LLM"

    monkeypatch.setattr(fixer, "call_llm_text_resilient", fake_call_llm_text_resilient)

    cfg = FormatConfig(max_chunk_chars=2000)
    llm = LLMConfig(enabled=True, provider="openai_compatible", base_url="http://example.com", model="m")

    out = fixer.format_txt("x\n", cfg, llm=llm)
    assert out.text.endswith("LLM")
    assert out.stats.get("llm_chunks") == 1

