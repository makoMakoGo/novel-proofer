"""Unit tests for ThinkTagFilter."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from novel_proofer.llm.client import _maybe_filter_think_tags
from novel_proofer.llm.config import LLMConfig
from novel_proofer.llm.think_filter import ThinkTagFilter, filter_think_tags


class TestThinkTagFilter:
    """Tests for ThinkTagFilter class."""

    def test_no_think_tags(self):
        """Content without think tags passes through unchanged."""
        f = ThinkTagFilter()
        result = f.feed("Hello world")
        result += f.flush()
        assert result == "Hello world"

    def test_simple_think_tag(self):
        """Simple think tag is filtered out."""
        f = ThinkTagFilter()
        result = f.feed("<think>thinking...</think>output")
        result += f.flush()
        assert result == "output"

    def test_think_tag_case_insensitive(self):
        """Think tags are matched case-insensitively."""
        f = ThinkTagFilter()
        result = f.feed("<THINK>thinking...</THINK>output")
        result += f.flush()
        assert result == "output"

        f.reset()
        result = f.feed("<Think>thinking...</Think>output")
        result += f.flush()
        assert result == "output"

    def test_think_tag_with_content_before_and_after(self):
        """Content before and after think tag is preserved."""
        f = ThinkTagFilter()
        result = f.feed("before<think>thinking</think>after")
        result += f.flush()
        assert result == "beforeafter"

    def test_multiple_think_tags(self):
        """Multiple think tags are all filtered."""
        f = ThinkTagFilter()
        result = f.feed("a<think>1</think>b<think>2</think>c")
        result += f.flush()
        assert result == "abc"

    def test_nested_think_tags(self):
        """Nested think tags use greedy matching."""
        f = ThinkTagFilter()
        result = f.feed("<think>outer<think>inner</think>still outer</think>output")
        result += f.flush()
        assert result == "output"

    def test_cross_chunk_open_tag(self):
        """Think tag opening split across chunks."""
        f = ThinkTagFilter()
        result = f.feed("before<thi")
        result += f.feed("nk>thinking</think>after")
        result += f.flush()
        assert result == "beforeafter"

    def test_cross_chunk_close_tag(self):
        """Think tag closing split across chunks."""
        f = ThinkTagFilter()
        result = f.feed("<think>thinking</thi")
        result += f.feed("nk>after")
        result += f.flush()
        assert result == "after"

    def test_cross_chunk_content(self):
        """Think content split across multiple chunks."""
        f = ThinkTagFilter()
        result = f.feed("<think>part1")
        result += f.feed("part2")
        result += f.feed("part3</think>output")
        result += f.flush()
        assert result == "output"

    def test_incomplete_open_tag_at_end(self):
        """Incomplete tag at end is preserved if not a tag."""
        f = ThinkTagFilter()
        result = f.feed("content<")
        result += f.flush()
        assert result == "content<"

    def test_unclosed_think_tag(self):
        """Unclosed think tag filters everything after it."""
        f = ThinkTagFilter()
        result = f.feed("<think>never closed")
        result += f.flush()
        assert result == ""

    def test_reset(self):
        """Reset clears state for reuse."""
        f = ThinkTagFilter()
        f.feed("<think>partial")
        f.reset()
        result = f.feed("fresh content")
        result += f.flush()
        assert result == "fresh content"

    def test_empty_think_tag(self):
        """Empty think tag is filtered."""
        f = ThinkTagFilter()
        result = f.feed("before<think></think>after")
        result += f.flush()
        assert result == "beforeafter"

    def test_multiline_think_content(self):
        """Multiline content inside think tag is filtered."""
        f = ThinkTagFilter()
        result = f.feed("<think>\nline1\nline2\n</think>output")
        result += f.flush()
        assert result == "output"

    def test_empty_chunk_returns_empty_string(self):
        f = ThinkTagFilter()
        assert f.feed("") == ""

    def test_nested_open_without_close_in_chunk_increments_depth(self):
        f = ThinkTagFilter()
        out = f.feed("<think><think>abc")
        out += f.flush()
        assert out == ""


class TestFilterThinkTagsFunction:
    """Tests for filter_think_tags convenience function."""

    def test_simple_filter(self):
        """One-shot filtering works."""
        result = filter_think_tags("<think>hidden</think>visible")
        assert result == "visible"

    def test_no_tags(self):
        """Content without tags passes through."""
        result = filter_think_tags("no tags here")
        assert result == "no tags here"

    def test_complex_content(self):
        """Complex content with multiple tags."""
        text = "start<think>thought1</think>middle<THINK>thought2</THINK>end"
        result = filter_think_tags(text)
        assert result == "startmiddleend"

    def test_unclosed_think_tag_filters_trailing_content(self):
        text = "prefix<think>never closed\nVISIBLE"
        result = filter_think_tags(text)
        assert result == "prefix"



class TestMaybeFilterThinkTags:
    def test_unclosed_returns_raw_stripped_tags(self):
        cfg = LLMConfig(filter_think_tags=True)
        raw = "before<think>no close\nAFTER"
        assert _maybe_filter_think_tags(cfg, raw, input_text="x" * 1000) == "beforeno close\nAFTER"

    def test_balanced_filters(self):
        cfg = LLMConfig(filter_think_tags=True)
        raw = "A<think>hidden</think>B"
        assert _maybe_filter_think_tags(cfg, raw, input_text="x" * 10) == "AB"

    def test_balanced_filters_can_fall_back_to_stripping(self):
        cfg = LLMConfig(filter_think_tags=True)
        raw = "A<think>hidden</think>B"
        assert _maybe_filter_think_tags(cfg, raw, input_text="x" * 10_000) == "AhiddenB"

    def test_disabled_returns_raw(self):
        cfg = LLMConfig(filter_think_tags=False)
        raw = "A<think>hidden</think>B"
        assert _maybe_filter_think_tags(cfg, raw, input_text="x" * 1000) == raw

    def test_low_output_ratio_falls_back_to_stripping_tags(self):
        cfg = LLMConfig(filter_think_tags=True)
        raw = "<think>hidden</think>VISIBLE"
        # Simulate a filter bug/edge where output becomes too small vs input.
        assert _maybe_filter_think_tags(cfg, raw, input_text="x" * 10_000) == "hiddenVISIBLE"
