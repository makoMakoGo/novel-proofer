from __future__ import annotations

import atexit
import codecs
import ipaddress
import json
import logging
import re
import threading
import time
import urllib.parse
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass

import httpx

from novel_proofer.env import env_truthy
from novel_proofer.llm.config import LLMConfig
from novel_proofer.llm.think_filter import ThinkTagFilter

logger = logging.getLogger(__name__)


class LLMError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class LLMTextResult:
    text: str
    raw_text: str
    stream_debug: str = ""


_RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
_DEFAULT_MAX_RETRIES = 2
_DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
_CANCELLED_STATUS_CODE = 499


@dataclass(frozen=True)
class _RetryOutcome[T]:
    value: T | None
    retries: int
    last_error: Exception | None
    last_code: int | None
    last_msg: str | None


def _cancelled_error() -> LLMError:
    # 499 is commonly used as "Client Closed Request" (non-standard but practical).
    return LLMError("cancelled", status_code=_CANCELLED_STATUS_CODE)


def _is_loopback_host(host: str | None) -> bool:
    if not host:
        return False
    h = host.strip().lower()
    if h == "localhost":
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


_HTTP_CLIENTS_LOCK = threading.Lock()
_HTTP_CLIENTS: dict[tuple[bool, int], httpx.Client] = {}


def _close_http_clients() -> None:
    with _HTTP_CLIENTS_LOCK:
        clients = list(_HTTP_CLIENTS.values())
        _HTTP_CLIENTS.clear()
    for c in clients:
        with suppress(Exception):
            c.close()


atexit.register(_close_http_clients)


def _httpx_client_for_url(url: str, *, max_connections: int) -> httpx.Client:
    host = urllib.parse.urlparse(url).hostname
    trust_env = not _is_loopback_host(host)

    max_conn = max(1, int(max_connections))
    key = (trust_env, max_conn)

    with _HTTP_CLIENTS_LOCK:
        existing = _HTTP_CLIENTS.get(key)
        if existing is not None:
            return existing

        limits = httpx.Limits(max_connections=max_conn, max_keepalive_connections=max_conn)
        client = httpx.Client(limits=limits, trust_env=trust_env)
        _HTTP_CLIENTS[key] = client
        return client


def _parse_sse_line(line: str) -> tuple[str, str] | None:
    """Parse a single SSE line and extract data content.

    Returns:
      - ("data", data_str) for SSE `data:` lines (data may be empty for keep-alives)
      - ("done", "") for `data: [DONE]`
      - None for non-data lines
    """

    line = line.strip()
    if not line.startswith("data:"):
        return None
    data = line[5:].strip()
    if data == "[DONE]":
        return ("done", "")
    return ("data", data)


def _extract_content_from_sse_json(data: str, content_parts: list[str]) -> None:
    """Extract text content from SSE JSON data and append to content_parts."""
    if not data:
        return
    try:
        obj = json.loads(data)
        if "choices" in obj:
            for choice in obj.get("choices", []):
                delta = choice.get("delta", {})
                if delta.get("content"):
                    content_parts.append(delta["content"])
    except json.JSONDecodeError:
        pass


class _SseDebugCapture:
    def __init__(self, *, head_limit: int, tail_limit: int) -> None:
        self._head_limit = max(0, int(head_limit))
        self._tail_limit = max(0, int(tail_limit))
        self._head_parts: list[str] = []
        self._head_len = 0
        self._tail_parts: deque[str] = deque()
        self._tail_len = 0

    def add(self, s: str) -> None:
        if not s:
            return

        if self._head_len < self._head_limit:
            take = s[: self._head_limit - self._head_len]
            if take:
                self._head_parts.append(take)
                self._head_len += len(take)

        if self._tail_limit <= 0:
            return

        self._tail_parts.append(s)
        self._tail_len += len(s)

        while self._tail_parts and (self._tail_len - len(self._tail_parts[0]) >= self._tail_limit):
            left = self._tail_parts.popleft()
            self._tail_len -= len(left)

        if not self._tail_parts:
            self._tail_len = 0
            return

        excess = self._tail_len - self._tail_limit
        if excess > 0:
            first = self._tail_parts[0]
            if excess >= len(first):
                self._tail_parts.popleft()
                self._tail_len -= len(first)
            else:
                self._tail_parts[0] = first[excess:]
                self._tail_len -= excess

    def render(self) -> str:
        head = "".join(self._head_parts)
        if head and self._head_len < self._head_limit:
            return head
        tail = "".join(self._tail_parts)
        if head:
            return head + "\n...[truncated]...\n" + tail
        return tail


def _stream_request_impl(
    url: str,
    payload: dict,
    headers: dict[str, str],
    timeout: float,
    *,
    should_stop: Callable[[], bool] | None = None,
    collect_debug: bool,
    max_connections: int,
) -> tuple[str, str]:
    """Streaming HTTP POST request, returning (content, stream_debug).

    stream_debug is a truncated raw SSE capture to help debug cases where the
    parsed content is empty or malformed.
    """
    debug = _SseDebugCapture(head_limit=8_000, tail_limit=12_000) if collect_debug else None

    try:
        client = _httpx_client_for_url(url, max_connections=max_connections)
        request_headers = {"Accept": "text/event-stream", **headers}

        with client.stream(
            "POST",
            url,
            json=payload,
            headers=request_headers,
            timeout=httpx.Timeout(float(timeout)),
        ) as resp:
            resp.raise_for_status()
            content_parts: list[str] = []
            buffer = ""
            done = False
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

            it = resp.iter_bytes(chunk_size=4096)
            while True:
                if should_stop is not None and should_stop():
                    raise _cancelled_error()

                try:
                    chunk = next(it)
                except StopIteration:
                    break

                if not chunk:
                    continue

                decoded = decoder.decode(chunk)
                if debug is not None:
                    debug.add(decoded)

                buffer += decoded
                lines = buffer.split("\n")
                buffer = lines[-1]  # Keep incomplete line in buffer

                for line in lines[:-1]:
                    parsed = _parse_sse_line(line)
                    if parsed is None:
                        continue
                    kind, data_line = parsed
                    if kind == "done":
                        done = True
                        break
                    _extract_content_from_sse_json(data_line, content_parts)

                if done:
                    break

            # Flush any remaining decoder state (handles UTF-8 split across chunk boundaries).
            tail = decoder.decode(b"", final=True)
            if tail:
                if debug is not None:
                    debug.add(tail)
                buffer += tail

            # Process remaining buffer
            if buffer.strip():
                parsed = _parse_sse_line(buffer)
                if parsed is not None:
                    kind, data_line = parsed
                    if kind != "done":
                        _extract_content_from_sse_json(data_line, content_parts)

            content = "".join(content_parts)

            if not collect_debug:
                return content, ""

            return content, debug.render() if debug is not None else ""

    except httpx.HTTPStatusError as e:
        body = ""
        with suppress(Exception):
            body = (e.response.text or "").strip()
        raise LLMError(
            f"HTTP {e.response.status_code} from LLM: {body}", status_code=int(e.response.status_code)
        ) from e
    except httpx.RequestError as e:
        raise LLMError(f"LLM request failed: {e}") from e


def _stream_request(
    url: str,
    payload: dict,
    headers: dict[str, str],
    timeout: float,
    *,
    should_stop: Callable[[], bool] | None = None,
    max_connections: int,
) -> str:
    """Make a streaming HTTP POST request and collect all content.

    Args:
        url: Request URL
        payload: JSON payload (stream: true will be added)
        headers: HTTP headers
        timeout: Request timeout in seconds
        should_stop: Optional callback to abort streaming early

    Returns:
        Complete response content

    Raises:
        LLMError: On HTTP or parsing errors
    """
    content, _debug = _stream_request_impl(
        url,
        payload,
        headers,
        timeout,
        should_stop=should_stop,
        collect_debug=False,
        max_connections=max_connections,
    )
    return content


def _stream_request_with_debug(
    url: str,
    payload: dict,
    headers: dict[str, str],
    timeout: float,
    *,
    should_stop: Callable[[], bool] | None = None,
    max_connections: int,
) -> tuple[str, str]:
    if not env_truthy("NOVEL_PROOFER_LLM_STREAM_DEBUG"):
        return (
            _stream_request(
                url,
                payload,
                headers,
                timeout,
                should_stop=should_stop,
                max_connections=max_connections,
            ),
            "",
        )
    return _stream_request_impl(
        url,
        payload,
        headers,
        timeout,
        should_stop=should_stop,
        collect_debug=True,
        max_connections=max_connections,
    )


def _http_post_json(url: str, payload: dict, headers: dict[str, str], timeout: float) -> dict:
    try:
        client = _httpx_client_for_url(url, max_connections=1)
        resp = client.post(url, json=payload, headers=headers, timeout=httpx.Timeout(float(timeout)))
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        body = ""
        with suppress(Exception):
            body = (e.response.text or "").strip()
        raise LLMError(
            f"HTTP {e.response.status_code} from LLM: {body}", status_code=int(e.response.status_code)
        ) from e
    except (httpx.RequestError, ValueError) as e:
        raise LLMError(f"LLM request failed: {e}") from e


def _headers(cfg: LLMConfig) -> dict[str, str]:
    if cfg.api_key:
        return {"Authorization": f"Bearer {cfg.api_key}"}
    return {}


def call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    return _call_openai_compatible(cfg, input_text, should_stop=should_stop)


def call_llm_text_with_raw(
    cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None
) -> LLMTextResult:
    return _call_openai_compatible_with_raw(cfg, input_text, should_stop=should_stop)


def _raise_if_cancelled(should_stop: Callable[[], bool] | None) -> None:
    if should_stop is not None and should_stop():
        raise _cancelled_error()


def _run_with_retries[T](
    execute: Callable[[], T],
    *,
    should_stop: Callable[[], bool] | None,
    on_retry: Callable[[int, int | None, str | None], None] | None = None,
) -> _RetryOutcome[T]:
    attempts = max(0, int(_DEFAULT_MAX_RETRIES)) + 1
    last_error: Exception | None = None
    last_code: int | None = None
    last_msg: str | None = None

    for i in range(attempts):
        _raise_if_cancelled(should_stop)
        try:
            value = execute()
            return _RetryOutcome(
                value=value,
                retries=i,
                last_error=last_error,
                last_code=last_code,
                last_msg=last_msg,
            )
        except LLMError as e:
            last_error = e
            last_code = e.status_code
            last_msg = str(e)
            if e.status_code is not None and e.status_code not in _RETRYABLE_STATUS:
                raise
        except Exception as e:
            last_error = e
            last_msg = str(e)

        if i < attempts - 1:
            _raise_if_cancelled(should_stop)
            if on_retry is not None:
                on_retry(i + 1, last_code, last_msg)
            time.sleep(max(0.0, float(_DEFAULT_RETRY_BACKOFF_SECONDS)) * (2**i))

    return _RetryOutcome(
        value=None,
        retries=max(0, attempts - 1),
        last_error=last_error,
        last_code=last_code,
        last_msg=last_msg,
    )


def call_llm_text_resilient(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    """Call LLM with retry/backoff on transient failures."""

    outcome = _run_with_retries(
        lambda: call_llm_text(cfg, input_text, should_stop=should_stop),
        should_stop=should_stop,
    )

    if outcome.value is not None:
        return outcome.value
    if outcome.last_error is None:
        raise LLMError("LLM failed with unknown error")
    if isinstance(outcome.last_error, LLMError):
        raise outcome.last_error
    raise LLMError(str(outcome.last_error))


def call_llm_text_resilient_with_meta(
    cfg: LLMConfig,
    input_text: str,
    *,
    should_stop: Callable[[], bool] | None = None,
    on_retry: Callable[[int, int | None, str | None], None] | None = None,
) -> tuple[str, int, int | None, str | None]:
    """Like call_llm_text_resilient, but returns (text, retries, last_code, last_message)."""

    outcome = _run_with_retries(
        lambda: call_llm_text(cfg, input_text, should_stop=should_stop),
        should_stop=should_stop,
        on_retry=on_retry,
    )
    if outcome.value is None:
        raise LLMError(outcome.last_msg or "LLM failed", status_code=outcome.last_code)
    return outcome.value, outcome.retries, outcome.last_code, outcome.last_msg


def call_llm_text_resilient_with_meta_and_raw(
    cfg: LLMConfig,
    input_text: str,
    *,
    should_stop: Callable[[], bool] | None = None,
    on_retry: Callable[[int, int | None, str | None], None] | None = None,
) -> tuple[LLMTextResult, int, int | None, str | None]:
    """Like call_llm_text_resilient_with_meta, but also returns raw output and stream debug."""

    outcome = _run_with_retries(
        lambda: call_llm_text_with_raw(cfg, input_text, should_stop=should_stop),
        should_stop=should_stop,
        on_retry=on_retry,
    )
    if outcome.value is None:
        raise LLMError(outcome.last_msg or "LLM failed", status_code=outcome.last_code)
    return outcome.value, outcome.retries, outcome.last_code, outcome.last_msg


_THINK_OPEN_RE = re.compile(r"<\s*think\b[^>]*>", re.IGNORECASE)
_THINK_CLOSE_RE = re.compile(r"</\s*think\s*>", re.IGNORECASE)
_THINK_FILTER_MIN_LEN = 200
_THINK_FILTER_MIN_RATIO = 0.2


def _looks_like_think_unclosed(text: str) -> bool:
    """Best-effort guard for providers that output '<think>' without closing.

    Some models (or streaming truncation) may leave an opening think tag without a
    matching closing tag, which would cause ThinkTagFilter to drop the remainder.
    """

    opens = len(_THINK_OPEN_RE.findall(text))
    closes = len(_THINK_CLOSE_RE.findall(text))
    return opens > closes


def _strip_think_tags_keep_content(text: str) -> str:
    """Remove think tag markers but keep their inner content."""

    if not text:
        return text
    text = _THINK_OPEN_RE.sub("", text)
    text = _THINK_CLOSE_RE.sub("", text)
    return text


def _maybe_filter_think_tags(_cfg: LLMConfig, raw_content: str, *, input_text: str | None = None) -> str:
    # Think tag filtering is always enabled (safe-by-default).

    if not raw_content:
        return raw_content

    # Fast path: avoid work when no think tags at all.
    if "<" not in raw_content:
        return raw_content
    if _THINK_OPEN_RE.search(raw_content) is None:
        return raw_content

    # For normal, well-formed tags, filter them out.
    f = ThinkTagFilter()
    filtered = f.feed(raw_content)
    filtered += f.flush()

    # Guard: if the provider output is malformed (unclosed) or filtering produced an
    # implausibly short output vs input, fall back to stripping tag markers only.
    if _looks_like_think_unclosed(raw_content):
        return _strip_think_tags_keep_content(raw_content)

    if input_text is not None:
        expected = len(input_text)
        if expected >= _THINK_FILTER_MIN_LEN and len(filtered.strip()) < max(
            _THINK_FILTER_MIN_LEN, int(expected * _THINK_FILTER_MIN_RATIO)
        ):
            return _strip_think_tags_keep_content(raw_content)

    return filtered


def _call_openai_compatible(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    return _call_openai_compatible_with_raw(cfg, input_text, should_stop=should_stop).text


def _call_openai_compatible_with_raw(
    cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None
) -> LLMTextResult:
    if not cfg.base_url:
        raise LLMError("LLM base_url is empty")
    if not cfg.model:
        raise LLMError("LLM model is empty")

    logger.info("LLM request: model=%s streaming=true chars=%s", cfg.model, len(input_text))
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    payload: dict = {
        "model": cfg.model,
        "temperature": cfg.temperature,
        "stream": True,  # Always use streaming
        "messages": [
            {"role": "system", "content": cfg.system_prompt},
            {"role": "user", "content": input_text},
        ],
    }

    # Merge extra_params if provided
    if cfg.extra_params:
        payload.update(cfg.extra_params)

    raw_content, stream_debug = _stream_request_with_debug(
        url,
        payload,
        headers=_headers(cfg),
        timeout=cfg.timeout_seconds,
        should_stop=should_stop,
        max_connections=max(1, int(cfg.max_concurrency)),
    )

    return LLMTextResult(
        text=_maybe_filter_think_tags(cfg, raw_content, input_text=input_text),
        raw_text=raw_content,
        stream_debug=stream_debug,
    )
