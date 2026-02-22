"""Unit tests for novel_proofer.llm.client."""

from __future__ import annotations

import httpx
import pytest

from novel_proofer.llm import client as llm_client
from novel_proofer.llm.client import LLMError
from novel_proofer.llm.config import LLMConfig


class _FakeStreamResponse:
    def __init__(self, chunks: list[bytes], *, status_code: int = 200) -> None:
        self._chunks = chunks
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return
        req = httpx.Request("POST", "http://x")
        resp = httpx.Response(self.status_code, request=req, text="boom")
        raise httpx.HTTPStatusError("error", request=req, response=resp)

    def iter_bytes(self, *, chunk_size: int = 4096):
        _ = chunk_size
        yield from self._chunks


class _FakeStreamCM:
    def __init__(self, resp: _FakeStreamResponse) -> None:
        self._resp = resp

    def __enter__(self) -> _FakeStreamResponse:
        return self._resp

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _IterOnceThenBoom:
    def __init__(self, first: bytes) -> None:
        self._first = first
        self._read_once = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        if self._read_once:
            raise AssertionError("iter_bytes should not be consumed after SSE [DONE]")
        self._read_once = True
        return self._first

    def iter_bytes(self, *, chunk_size: int = 4096):
        _ = chunk_size
        return self


class _BoomOnNext:
    def iter_bytes(self, *, chunk_size: int = 4096):
        _ = chunk_size
        return self

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        raise AssertionError("iter_bytes should not be consumed when should_stop() is true")


class _FakeClient:
    def __init__(self, *, stream_cm=None, stream_exc: Exception | None = None, post_resp=None, post_exc=None) -> None:
        self._stream_cm = stream_cm
        self._stream_exc = stream_exc
        self._post_resp = post_resp
        self._post_exc = post_exc

    def stream(self, _method: str, _url: str, *, json: dict, headers: dict, timeout):
        _ = (json, headers, timeout)
        if self._stream_exc is not None:
            raise self._stream_exc
        return self._stream_cm

    def post(self, _url: str, *, json: dict, headers: dict, timeout):
        _ = (json, headers, timeout)
        if self._post_exc is not None:
            raise self._post_exc
        return self._post_resp


def test_llm_config_removed_retry_fields():
    cfg = LLMConfig()
    assert not hasattr(cfg, "max_retries")
    assert not hasattr(cfg, "retry_backoff_seconds")
    assert not hasattr(cfg, "split_min_chars")
    with pytest.raises(TypeError):
        LLMConfig(max_retries=1)  # type: ignore[call-arg]


def test_call_llm_text_routes_to_openai_compatible(monkeypatch: pytest.MonkeyPatch):
    cfg = LLMConfig(base_url="http://x", model="m")

    def fake_call(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:
        return "OK"

    monkeypatch.setattr(llm_client, "_call_openai_compatible", fake_call)
    assert llm_client.call_llm_text(cfg, "hi") == "OK"


def test_call_openai_compatible_payload_has_no_max_tokens_by_default(monkeypatch: pytest.MonkeyPatch):
    captured: dict = {}

    def fake_stream_request_with_debug(
        url: str,
        payload: dict,
        headers: dict[str, str],
        timeout: float,
        *,
        should_stop=None,
        max_connections: int,
    ) -> tuple[str, str]:
        captured["url"] = url
        captured["payload"] = payload
        captured["headers"] = headers
        captured["timeout"] = timeout
        captured["should_stop"] = should_stop
        captured["max_connections"] = max_connections
        return "RAW", "DBG"

    monkeypatch.setattr(llm_client, "_stream_request_with_debug", fake_stream_request_with_debug)

    cfg = LLMConfig(
        base_url="http://example.com",
        api_key="k",
        model="m",
        system_prompt="S",
        extra_params=None,
    )
    out = llm_client._call_openai_compatible(cfg, "U", should_stop=lambda: False)

    assert out == "RAW"
    assert captured["url"] == "http://example.com/chat/completions"
    assert "max_tokens" not in captured["payload"]
    assert captured["payload"]["stream"] is True
    assert captured["payload"]["messages"][0]["role"] == "system"
    assert captured["payload"]["messages"][1]["role"] == "user"
    assert captured["headers"]["Authorization"] == "Bearer k"
    assert callable(captured["should_stop"])
    assert captured["max_connections"] == 20


def test_call_openai_compatible_merges_extra_params(monkeypatch: pytest.MonkeyPatch):
    captured: dict = {}

    def fake_stream_request_with_debug(
        url: str,
        payload: dict,
        headers: dict[str, str],
        timeout: float,
        *,
        should_stop=None,
        max_connections: int,
    ) -> tuple[str, str]:
        _ = (url, headers, timeout, should_stop, max_connections)
        captured["payload"] = payload
        return "RAW", "DBG"

    monkeypatch.setattr(llm_client, "_stream_request_with_debug", fake_stream_request_with_debug)

    cfg = LLMConfig(
        base_url="http://example.com",
        api_key="test-key",
        model="m",
        extra_params={"max_tokens": 123, "temperature": 0.7},
    )
    _ = llm_client._call_openai_compatible(cfg, "U")

    assert captured["payload"]["max_tokens"] == 123
    assert captured["payload"]["temperature"] == 0.7


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("data: [DONE]", ("done", "")),
        ("data:", ("data", "")),
        ('data:  {"x":1}', ("data", '{"x":1}')),
        ("event: ping", None),
        ("", None),
    ],
)
def test_parse_sse_line(line: str, expected: tuple[str, str] | None):
    assert llm_client._parse_sse_line(line) == expected


@pytest.mark.parametrize(
    ("head_limit", "tail_limit", "parts", "expected"),
    [
        (5, 10, ["abc", "de"], "abcde"),
        (5, 10, ["abcd", "efgh"], "abcdefgh"),
        (5, 10, ["abc", "defghijk"], "abcdefghijk"),
        (5, 6, ["abcdefghij", "klmnopqrst"], "abcde\n...[truncated]...\nopqrst"),
    ],
)
def test_sse_debug_capture_render_is_compact(
    head_limit: int,
    tail_limit: int,
    parts: list[str],
    expected: str,
) -> None:
    cap = llm_client._SseDebugCapture(head_limit=head_limit, tail_limit=tail_limit)
    for part in parts:
        cap.add(part)
    assert cap.render() == expected


def test_is_loopback_host():
    assert llm_client._is_loopback_host(None) is False
    assert llm_client._is_loopback_host("") is False
    assert llm_client._is_loopback_host("localhost") is True
    assert llm_client._is_loopback_host("127.0.0.1") is True
    assert llm_client._is_loopback_host("8.8.8.8") is False


def test_httpx_client_for_url_bypasses_env_proxy_for_loopback(monkeypatch: pytest.MonkeyPatch):
    llm_client._HTTP_CLIENTS.clear()
    created: list[dict] = []

    class _CtorSpyClient:
        def __init__(self, *, limits, trust_env: bool) -> None:
            created.append({"limits": limits, "trust_env": trust_env})

        def close(self) -> None:
            return None

    monkeypatch.setattr(llm_client.httpx, "Client", _CtorSpyClient)

    _ = llm_client._httpx_client_for_url("http://localhost:123", max_connections=7)
    assert created[0]["trust_env"] is False
    assert created[0]["limits"].max_connections == 7

    _ = llm_client._httpx_client_for_url("http://example.com", max_connections=7)
    assert created[1]["trust_env"] is True
    assert created[1]["limits"].max_connections == 7


def test_stream_request_parses_openai_sse(monkeypatch: pytest.MonkeyPatch):
    sse = (
        b'data: {"choices":[{"delta":{"content":"Hello"}}]}\n'
        b'data: {"choices":[{"delta":{"content":" world"}}]}\n'
        b"data: [DONE]\n"
    )

    fake_resp = _FakeStreamResponse([sse])
    fake_client = _FakeClient(stream_cm=_FakeStreamCM(fake_resp))

    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    out = llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0, max_connections=1)
    assert out == "Hello world"


def test_stream_request_stops_reading_after_done(monkeypatch: pytest.MonkeyPatch):
    fake_resp = _FakeStreamResponse([b"data: [DONE]\n"])
    fake_resp.iter_bytes = _IterOnceThenBoom(b"data: [DONE]\n").iter_bytes  # type: ignore[method-assign]
    fake_client = _FakeClient(stream_cm=_FakeStreamCM(fake_resp))

    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    assert llm_client._stream_request("http://x", {"stream": True}, headers={}, timeout=1.0, max_connections=1) == ""


def test_stream_request_should_stop_short_circuits(monkeypatch: pytest.MonkeyPatch):
    fake_resp = _FakeStreamResponse([])
    fake_resp.iter_bytes = _BoomOnNext().iter_bytes  # type: ignore[method-assign]
    fake_client = _FakeClient(stream_cm=_FakeStreamCM(fake_resp))

    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    with pytest.raises(LLMError, match=r"cancelled"):
        llm_client._stream_request(
            "http://x",
            {"stream": True},
            headers={},
            timeout=1.0,
            should_stop=lambda: True,
            max_connections=1,
        )


def test_stream_request_wraps_url_error(monkeypatch: pytest.MonkeyPatch):
    fake_client = _FakeClient(stream_exc=httpx.RequestError("boom"))
    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    with pytest.raises(LLMError, match=r"LLM request failed: .*boom"):
        llm_client._stream_request("http://x", {}, headers={}, timeout=1.0, max_connections=1)


def test_http_post_json_success(monkeypatch: pytest.MonkeyPatch):
    class _FakeJsonResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"ok": True}

    fake_client = _FakeClient(post_resp=_FakeJsonResponse())
    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    assert llm_client._http_post_json("http://x", {"a": 1}, headers={}, timeout=1.0) == {"ok": True}


def test_http_post_json_wraps_url_error(monkeypatch: pytest.MonkeyPatch):
    fake_client = _FakeClient(post_exc=httpx.RequestError("boom"))
    monkeypatch.setattr(llm_client, "_httpx_client_for_url", lambda url, *, max_connections: fake_client)
    with pytest.raises(LLMError, match=r"LLM request failed: .*boom"):
        llm_client._http_post_json("http://x", {}, headers={}, timeout=1.0)


def test_call_llm_text_resilient_retries_and_succeeds(monkeypatch: pytest.MonkeyPatch):
    calls: list[int] = []
    sleeps: list[float] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:
        calls.append(1)
        if len(calls) < 3:
            raise LLMError("HTTP 500", status_code=500)
        return "OK"

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(float(s)))

    cfg = LLMConfig()
    assert llm_client.call_llm_text_resilient(cfg, "x") == "OK"
    assert len(calls) == 3
    assert len(sleeps) == 2


def test_call_llm_text_resilient_non_retryable_raises(monkeypatch: pytest.MonkeyPatch):
    sleeps: list[float] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:
        raise LLMError("HTTP 400", status_code=400)

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(float(s)))

    cfg = LLMConfig()
    with pytest.raises(LLMError, match=r"HTTP 400"):
        llm_client.call_llm_text_resilient(cfg, "x")
    assert sleeps == []


def test_call_llm_text_resilient_with_meta_calls_on_retry(monkeypatch: pytest.MonkeyPatch):
    calls: list[int] = []
    on_retry_calls: list[tuple[int, int | None, str | None]] = []

    def fake_call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop=None) -> str:
        calls.append(1)
        if len(calls) == 1:
            raise LLMError("HTTP 500", status_code=500)
        return "OK"

    monkeypatch.setattr(llm_client, "call_llm_text", fake_call_llm_text)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: None)

    cfg = LLMConfig()
    out, retries, last_code, last_msg = llm_client.call_llm_text_resilient_with_meta(
        cfg, "x", on_retry=lambda idx, code, msg: on_retry_calls.append((idx, code, msg))
    )

    assert out == "OK"
    assert retries == 1
    assert last_code == 500
    assert isinstance(last_msg, str) and "HTTP 500" in last_msg
    assert on_retry_calls == [(1, 500, last_msg)]


def test_call_llm_text_resilient_with_meta_and_raw_calls_on_retry(monkeypatch: pytest.MonkeyPatch):
    calls: list[int] = []
    on_retry_calls: list[tuple[int, int | None, str | None]] = []

    def fake_call_llm_text_with_raw(cfg: LLMConfig, input_text: str, *, should_stop=None):
        calls.append(1)
        if len(calls) == 1:
            raise LLMError("HTTP 503", status_code=503)
        return llm_client.LLMTextResult(text="OK", raw_text="RAW")

    monkeypatch.setattr(llm_client, "call_llm_text_with_raw", fake_call_llm_text_with_raw)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: None)

    cfg = LLMConfig()
    out, retries, last_code, last_msg = llm_client.call_llm_text_resilient_with_meta_and_raw(
        cfg, "x", on_retry=lambda idx, code, msg: on_retry_calls.append((idx, code, msg))
    )

    assert out.text == "OK"
    assert out.raw_text == "RAW"
    assert retries == 1
    assert last_code == 503
    assert isinstance(last_msg, str) and "HTTP 503" in last_msg
    assert on_retry_calls == [(1, 503, last_msg)]


def test_call_llm_text_resilient_with_meta_and_raw_non_retryable_raises(monkeypatch: pytest.MonkeyPatch):
    sleeps: list[float] = []

    def fake_call_llm_text_with_raw(cfg: LLMConfig, input_text: str, *, should_stop=None):
        raise LLMError("HTTP 401", status_code=401)

    monkeypatch.setattr(llm_client, "call_llm_text_with_raw", fake_call_llm_text_with_raw)
    monkeypatch.setattr(llm_client.time, "sleep", lambda s: sleeps.append(float(s)))

    cfg = LLMConfig()
    with pytest.raises(LLMError, match=r"HTTP 401"):
        llm_client.call_llm_text_resilient_with_meta_and_raw(cfg, "x")
    assert sleeps == []
