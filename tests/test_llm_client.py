"""Unit tests for novel_proofer.llm.client."""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from novel_proofer.llm import client as llm_client
from novel_proofer.llm.client import LLMError
from novel_proofer.llm.config import LLMConfig


class _FakeResponse:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self._i = 0

    def read(self, _n: int = -1) -> bytes:
        if self._i >= len(self._chunks):
            return b""
        chunk = self._chunks[self._i]
        self._i += 1
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _DoneThenBoomResponse:
    def __init__(self, first: bytes) -> None:
        self._first = first
        self._read_once = False

    def read(self, _n: int) -> bytes:
        if self._read_once:
            raise AssertionError("response.read() should not be called after SSE [DONE]")
        self._read_once = True
        return self._first

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_llm_config_removed_retry_fields():
    cfg = LLMConfig()
    assert not hasattr(cfg, "max_retries")
    assert not hasattr(cfg, "retry_backoff_seconds")
    assert not hasattr(cfg, "split_min_chars")
    with pytest.raises(TypeError):
        LLMConfig(max_retries=1)  # type: ignore[call-arg]


def test_call_llm_text_disabled_passthrough():
    cfg = LLMConfig(enabled=False)
    assert llm_client.call_llm_text(cfg, "hello") == "hello"


def test_call_llm_text_routes_to_openai_compatible(monkeypatch: pytest.MonkeyPatch):
    cfg = LLMConfig(enabled=True, provider="openai_compatible", base_url="http://x", model="m")

    def fake_call(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:  # noqa: ANN001
        return "OK"

    monkeypatch.setattr(llm_client, "_call_openai_compatible", fake_call)
    assert llm_client.call_llm_text(cfg, "hi") == "OK"


def test_call_llm_text_routes_to_gemini(monkeypatch: pytest.MonkeyPatch):
    cfg = LLMConfig(enabled=True, provider="gemini", base_url="http://x", model="m")

    def fake_call(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:  # noqa: ANN001
        return "G"

    monkeypatch.setattr(llm_client, "_call_gemini", fake_call)
    assert llm_client.call_llm_text(cfg, "hi") == "G"


def test_call_openai_compatible_payload_has_no_max_tokens_by_default(monkeypatch: pytest.MonkeyPatch):
    captured: dict = {}

    def fake_stream_request_with_debug(  # noqa: ANN001
        url: str, payload: dict, headers: dict[str, str], timeout: float, *, should_stop=None
    ) -> tuple[str, str]:
        captured["url"] = url
        captured["payload"] = payload
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["should_stop"] = should_stop
        return "RAW", "DBG"

    monkeypatch.setattr(llm_client, "_stream_request_with_debug", fake_stream_request_with_debug)

    cfg = LLMConfig(
        enabled=True,
        provider="openai_compatible",
        base_url="http://example.com",
        api_key="k",
        model="m",
        system_prompt="S",
        extra_params=None,
        filter_think_tags=False,
    )
    out = llm_client._call_openai_compatible(cfg, "U", should_stop=lambda: False)

    assert out == "RAW"
    assert captured["url"] == "http://example.com/v1/chat/completions"
    assert "max_tokens" not in captured["payload"]
    assert captured["payload"]["stream"] is True
    assert captured["payload"]["messages"][0]["role"] == "system"
    assert captured["payload"]["messages"][1]["role"] == "user"
    assert captured["headers"]["Authorization"] == "Bearer k"
    assert callable(captured["should_stop"])


def test_call_openai_compatible_merges_extra_params(monkeypatch: pytest.MonkeyPatch):
    captured: dict = {}

    def fake_stream_request_with_debug(  # noqa: ANN001
        url: str, payload: dict, headers: dict[str, str], timeout: float, *, should_stop=None
    ) -> tuple[str, str]:
        captured["payload"] = payload
        return "RAW", "DBG"

    monkeypatch.setattr(llm_client, "_stream_request_with_debug", fake_stream_request_with_debug)

    cfg = LLMConfig(
        enabled=True,
        base_url="http://example.com",
        model="m",
        extra_params={"max_tokens": 123, "temperature": 0.7},
        filter_think_tags=False,
    )
    _ = llm_client._call_openai_compatible(cfg, "U")

    assert captured["payload"]["max_tokens"] == 123
    assert captured["payload"]["temperature"] == 0.7


def test_call_gemini_payload_and_merge_extra_params(monkeypatch: pytest.MonkeyPatch):
    captured: dict = {}

    def fake_stream_request_with_debug(  # noqa: ANN001
        url: str, payload: dict, headers: dict[str, str], timeout: float, *, should_stop=None
    ) -> tuple[str, str]:
        captured["url"] = url
        captured["payload"] = payload
        captured["should_stop"] = should_stop
        return "RAW", "DBG"

    monkeypatch.setattr(llm_client, "_stream_request_with_debug", fake_stream_request_with_debug)

    cfg = LLMConfig(
        enabled=True,
        provider="gemini",
        base_url="http://example.com/",
        model="m",
        system_prompt="S",
        extra_params={"foo": "bar"},
        filter_think_tags=False,
    )
    out = llm_client._call_gemini(cfg, "U", should_stop=lambda: False)

    assert out == "RAW"
    assert captured["url"] == "http://example.com/v1beta/models/m:streamGenerateContent?alt=sse"
    assert captured["payload"]["contents"][0]["parts"][0]["text"] == "S\n\nU"
    assert captured["payload"]["generationConfig"]["temperature"] == cfg.temperature
    assert captured["payload"]["foo"] == "bar"
    assert callable(captured["should_stop"])


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("data: [DONE]", ("done", "")),
        ("data:", ("data", "")),
        ("data:  {\"x\":1}", ("data", "{\"x\":1}")),
        ("event: ping", None),
        ("", None),
    ],
)
def test_parse_sse_line(line: str, expected: tuple[str, str] | None):
    assert llm_client._parse_sse_line(line) == expected


def test_is_loopback_host():
    assert llm_client._is_loopback_host(None) is False
    assert llm_client._is_loopback_host("") is False
    assert llm_client._is_loopback_host("localhost") is True
    assert llm_client._is_loopback_host("127.0.0.1") is True
    assert llm_client._is_loopback_host("8.8.8.8") is False


def test_urlopen_uses_no_proxy_for_loopback(monkeypatch: pytest.MonkeyPatch):
    req = urllib.request.Request("http://localhost:123", method="GET")
    used = {"build_opener": 0, "urlopen": 0, "proxy_dict": None}

    class _FakeOpener:
        def open(self, _req, *, timeout: float):  # noqa: ANN001
            return ("opened", timeout)

    def fake_proxy_handler(d: dict):  # noqa: ANN001
        used["proxy_dict"] = d
        return object()

    def fake_build_opener(_handler):  # noqa: ANN001
        used["build_opener"] += 1
        return _FakeOpener()

    def fake_urlopen(_req, *, timeout: float):  # noqa: ANN001
        used["urlopen"] += 1
        raise AssertionError("should not call urllib.request.urlopen for loopback")

    monkeypatch.setattr(urllib.request, "ProxyHandler", fake_proxy_handler)
    monkeypatch.setattr(urllib.request, "build_opener", fake_build_opener)
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    out = llm_client._urlopen(req, timeout=1.25)
    assert out == ("opened", 1.25)
    assert used["build_opener"] == 1
    assert used["urlopen"] == 0
    assert used["proxy_dict"] == {}


def test_stream_request_parses_openai_sse(monkeypatch: pytest.MonkeyPatch):
    sse = (
        b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n"
        b"data: {\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n"
        b"data: [DONE]\n"
    )

    monkeypatch.setattr(llm_client, "_urlopen", lambda req, timeout: _FakeResponse([sse]))
    out = llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0)
    assert out == "Hello world"


def test_stream_request_stops_reading_after_done(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(llm_client, "_urlopen", lambda req, timeout: _DoneThenBoomResponse(b"data: [DONE]\n"))
    assert llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0) == ""


def test_stream_request_parses_gemini_sse(monkeypatch: pytest.MonkeyPatch):
    sse = b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"A\"},{\"text\":\"B\"}]}}]}\n"
    monkeypatch.setattr(llm_client, "_urlopen", lambda req, timeout: _FakeResponse([sse]))
    out = llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0)
    assert out == "AB"


def test_stream_request_should_stop_short_circuits(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(llm_client, "_urlopen", lambda req, timeout: _FakeResponse([b"data: {\"x\":1}\n"]))
    with pytest.raises(LLMError, match=r"cancelled"):
        llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0, should_stop=lambda: True)


def test_stream_request_wraps_url_error(monkeypatch: pytest.MonkeyPatch):
    def boom(req, timeout):  # noqa: ANN001
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(llm_client, "_urlopen", boom)
    with pytest.raises(LLMError, match=r"LLM request failed: .*boom"):
        llm_client._stream_request("http://x", {}, headers={}, timeout=1.0)


def test_http_post_json_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(llm_client, "_urlopen", lambda req, timeout: _FakeResponse([b"{\"ok\":true}"]))
    assert llm_client._http_post_json("http://x", {"a": 1}, headers={}, timeout=1.0) == {"ok": True}


def test_http_post_json_wraps_url_error(monkeypatch: pytest.MonkeyPatch):
    def boom(req, timeout):  # noqa: ANN001
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(llm_client, "_urlopen", boom)
    with pytest.raises(LLMError, match=r"LLM request failed: .*boom"):
        llm_client._http_post_json("http://x", {}, headers={}, timeout=1.0)


def test_call_llm_text_resilient_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch):
    calls: list[int] = []
    sleeps: list[float] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:  # noqa: ANN001
        calls.append(1)
        if len(calls) < 3:
            raise LLMError("HTTP 500", status_code=500)
        return "OK"

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(float(s)))

    cfg = LLMConfig(enabled=True)
    assert llm_client.call_llm_text_resilient(cfg, "x") == "OK"
    assert len(calls) == 3
    assert len(sleeps) == 2


def test_call_llm_text_resilient_non_retryable_raises(monkeypatch: pytest.MonkeyPatch):
    sleeps: list[float] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:  # noqa: ANN001
        raise LLMError("HTTP 400", status_code=400)

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(float(s)))

    cfg = LLMConfig(enabled=True)
    with pytest.raises(LLMError, match=r"HTTP 400"):
        llm_client.call_llm_text_resilient(cfg, "x")
    assert sleeps == []


def test_call_llm_text_resilient_with_meta_calls_on_retry(monkeypatch: pytest.MonkeyPatch):
    calls: list[int] = []
    on_retry_calls: list[tuple[int, int | None, str | None]] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:  # noqa: ANN001
        calls.append(1)
        if len(calls) == 1:
            raise LLMError("HTTP 500", status_code=500)
        return "OK"

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: None)

    cfg = LLMConfig(enabled=True)
    out, retries, last_code, last_msg = llm_client.call_llm_text_resilient_with_meta(
        cfg, "x", on_retry=lambda idx, code, msg: on_retry_calls.append((idx, code, msg))
    )

    assert out == "OK"
    assert retries == 1
    assert last_code == 500
    assert isinstance(last_msg, str) and "HTTP 500" in last_msg
    assert on_retry_calls == [(1, 500, last_msg)]
