from __future__ import annotations

import ipaddress
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable

from novel_proofer.llm.config import LLMConfig
from novel_proofer.llm.think_filter import ThinkTagFilter


class LLMError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


_RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
_DEFAULT_MAX_RETRIES = 2
_DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
_CANCELLED_STATUS_CODE = 499


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


def _urlopen(req: urllib.request.Request, timeout: float):
    host = urllib.parse.urlparse(req.full_url).hostname
    if _is_loopback_host(host):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return opener.open(req, timeout=timeout)
    return urllib.request.urlopen(req, timeout=timeout)


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
        # OpenAI format
        if "choices" in obj:
            for choice in obj.get("choices", []):
                delta = choice.get("delta", {})
                if "content" in delta and delta["content"]:
                    content_parts.append(delta["content"])
        # Gemini format
        elif "candidates" in obj:
            for cand in obj.get("candidates", []):
                parts = cand.get("content", {}).get("parts", [])
                for p in parts:
                    if "text" in p:
                        content_parts.append(p["text"])
    except json.JSONDecodeError:
        pass


def _stream_request(
    url: str,
    payload: dict,
    headers: dict[str, str],
    timeout: float,
    *,
    should_stop: Callable[[], bool] | None = None,
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
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "text/event-stream", **headers},
        method="POST",
    )
    
    try:
        with _urlopen(req, timeout=timeout) as resp:
            content_parts = []
            buffer = ""
            done = False
            
            while True:
                if should_stop is not None and should_stop():
                    raise _cancelled_error()

                chunk = resp.read(4096)
                if not chunk:
                    break

                buffer += chunk.decode("utf-8", errors="replace")
                lines = buffer.split("\n")
                buffer = lines[-1]  # Keep incomplete line in buffer
                
                for line in lines[:-1]:
                    parsed = _parse_sse_line(line)
                    if parsed is None:
                        continue
                    kind, data = parsed
                    if kind == "done":
                        done = True
                        break
                    _extract_content_from_sse_json(data, content_parts)

                if done:
                    break
            
            # Process remaining buffer
            if buffer.strip():
                parsed = _parse_sse_line(buffer)
                if parsed is not None:
                    kind, data = parsed
                    if kind != "done":
                        _extract_content_from_sse_json(data, content_parts)

            return "".join(content_parts)
            
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise LLMError(f"HTTP {e.code} from LLM: {body}", status_code=int(e.code)) from e
    except urllib.error.URLError as e:
        raise LLMError(f"LLM request failed: {e}") from e


def _http_post_json(url: str, payload: dict, headers: dict[str, str], timeout: float) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with _urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise LLMError(f"HTTP {e.code} from LLM: {body}", status_code=int(e.code)) from e
    except urllib.error.URLError as e:
        raise LLMError(f"LLM request failed: {e}") from e


def _headers(cfg: LLMConfig) -> dict[str, str]:
    if cfg.api_key:
        return {"Authorization": f"Bearer {cfg.api_key}"}
    return {}


def call_llm_text(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    if not cfg.enabled:
        return input_text

    provider = (cfg.provider or "").strip().lower()
    if provider == "gemini":
        return _call_gemini(cfg, input_text, should_stop=should_stop)

    return _call_openai_compatible(cfg, input_text, should_stop=should_stop)


def call_llm_text_resilient(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    """Call LLM with retry/backoff on transient failures."""

    attempts = max(0, int(_DEFAULT_MAX_RETRIES)) + 1
    last_error: Exception | None = None

    for i in range(attempts):
        if should_stop is not None and should_stop():
            raise _cancelled_error()

        try:
            return call_llm_text(cfg, input_text, should_stop=should_stop)
        except LLMError as e:
            last_error = e
            if e.status_code is not None and e.status_code not in _RETRYABLE_STATUS:
                raise
        except Exception as e:
            last_error = e

        if i < attempts - 1:
            if should_stop is not None and should_stop():
                raise _cancelled_error()
            time.sleep(max(0.0, float(_DEFAULT_RETRY_BACKOFF_SECONDS)) * (2**i))

    if last_error is None:
        raise LLMError("LLM failed with unknown error")
    if isinstance(last_error, LLMError):
        raise last_error
    raise LLMError(str(last_error))


def call_llm_text_resilient_with_meta(
    cfg: LLMConfig,
    input_text: str,
    *,
    should_stop: Callable[[], bool] | None = None,
    on_retry: Callable[[int, int | None, str | None], None] | None = None,
) -> tuple[str, int, int | None, str | None]:
    """Like call_llm_text_resilient, but returns (text, retries, last_code, last_message)."""

    attempts = max(0, int(_DEFAULT_MAX_RETRIES)) + 1
    last_code: int | None = None
    last_msg: str | None = None

    for i in range(attempts):
        if should_stop is not None and should_stop():
            raise _cancelled_error()
        try:
            return call_llm_text(cfg, input_text, should_stop=should_stop), i, last_code, last_msg
        except LLMError as e:
            last_code = e.status_code
            last_msg = str(e)
            if e.status_code is not None and e.status_code not in _RETRYABLE_STATUS:
                raise
        except Exception as e:
            last_msg = str(e)

        if i < attempts - 1:
            if should_stop is not None and should_stop():
                raise _cancelled_error()
            if on_retry is not None:
                on_retry(i + 1, last_code, last_msg)
            time.sleep(max(0.0, float(_DEFAULT_RETRY_BACKOFF_SECONDS)) * (2**i))

    raise LLMError(last_msg or "LLM failed", status_code=last_code)


_THINK_OPEN_RE = re.compile(r"<\s*think\b[^>]*>", re.IGNORECASE)
_THINK_CLOSE_RE = re.compile(r"</\s*think\s*>", re.IGNORECASE)


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


def _maybe_filter_think_tags(cfg: LLMConfig, raw_content: str, *, input_text: str | None = None) -> str:
    if not cfg.filter_think_tags:
        return raw_content

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
        if expected >= 200 and len(filtered.strip()) < max(200, int(expected * 0.2)):
            return _strip_think_tags_keep_content(raw_content)

    return filtered


def _call_openai_compatible(
    cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None
) -> str:
    if not cfg.base_url:
        raise LLMError("LLM base_url is empty")
    if not cfg.model:
        raise LLMError("LLM model is empty")

    print(f"[LLM] model={cfg.model} (streaming)", flush=True)
    url = cfg.base_url.rstrip("/") + "/v1/chat/completions"
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

    raw_content = _stream_request(
        url, payload, headers=_headers(cfg), timeout=cfg.timeout_seconds, should_stop=should_stop
    )

    return _maybe_filter_think_tags(cfg, raw_content, input_text=input_text)


def _call_gemini(cfg: LLMConfig, input_text: str, *, should_stop: Callable[[], bool] | None = None) -> str:
    if not cfg.base_url:
        raise LLMError("LLM base_url is empty")
    if not cfg.model:
        raise LLMError("LLM model is empty")

    print(f"[LLM] model={cfg.model} (streaming)", flush=True)
    # Gemini streaming uses alt=sse query parameter
    url = cfg.base_url.rstrip("/") + f"/v1beta/models/{cfg.model}:streamGenerateContent?alt=sse"
    payload: dict = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": cfg.system_prompt + "\n\n" + input_text},
                ],
            }
        ],
        "generationConfig": {"temperature": cfg.temperature},
    }

    # Merge extra_params if provided
    if cfg.extra_params:
        payload.update(cfg.extra_params)

    raw_content = _stream_request(
        url, payload, headers=_headers(cfg), timeout=cfg.timeout_seconds, should_stop=should_stop
    )

    return _maybe_filter_think_tags(cfg, raw_content, input_text=input_text)
