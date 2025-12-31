from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from novel_proofer.llm.config import LLMConfig
from novel_proofer.llm.think_filter import ThinkTagFilter


class LLMError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


_RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}


def _parse_sse_line(line: str) -> str | None:
    """Parse a single SSE line and extract data content.
    
    Returns the data string if line is a data line, None otherwise.
    """
    line = line.strip()
    if line.startswith("data:"):
        data = line[5:].strip()
        if data == "[DONE]":
            return None
        return data
    return None


def _stream_request(url: str, payload: dict, headers: dict[str, str], timeout: float) -> str:
    """Make a streaming HTTP POST request and collect all content.
    
    Args:
        url: Request URL
        payload: JSON payload (stream: true will be added)
        headers: HTTP headers
        timeout: Request timeout in seconds
        
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
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_parts = []
            buffer = ""
            
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                    
                buffer += chunk.decode("utf-8", errors="replace")
                lines = buffer.split("\n")
                buffer = lines[-1]  # Keep incomplete line in buffer
                
                for line in lines[:-1]:
                    data = _parse_sse_line(line)
                    if data is None:
                        continue
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
                        continue
            
            # Process remaining buffer
            if buffer.strip():
                data = _parse_sse_line(buffer)
                if data:
                    try:
                        obj = json.loads(data)
                        if "choices" in obj:
                            for choice in obj.get("choices", []):
                                delta = choice.get("delta", {})
                                if "content" in delta and delta["content"]:
                                    content_parts.append(delta["content"])
                        elif "candidates" in obj:
                            for cand in obj.get("candidates", []):
                                parts = cand.get("content", {}).get("parts", [])
                                for p in parts:
                                    if "text" in p:
                                        content_parts.append(p["text"])
                    except json.JSONDecodeError:
                        pass
            
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
        with urllib.request.urlopen(req, timeout=timeout) as resp:
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


def call_llm_text(cfg: LLMConfig, input_text: str) -> str:
    if not cfg.enabled:
        return input_text

    provider = (cfg.provider or "").strip().lower()
    if provider == "gemini":
        return _call_gemini(cfg, input_text)

    return _call_openai_compatible(cfg, input_text)


def call_llm_text_resilient(cfg: LLMConfig, input_text: str) -> str:
    """Call LLM with retry/backoff on transient failures."""

    attempts = max(0, int(cfg.max_retries)) + 1
    last_error: Exception | None = None

    for i in range(attempts):
        try:
            return call_llm_text(cfg, input_text)
        except LLMError as e:
            last_error = e
            if e.status_code is not None and e.status_code not in _RETRYABLE_STATUS:
                raise
        except Exception as e:
            last_error = e

        if i < attempts - 1:
            time.sleep(max(0.0, float(cfg.retry_backoff_seconds)) * (2**i))

    if last_error is None:
        raise LLMError("LLM failed with unknown error")
    if isinstance(last_error, LLMError):
        raise last_error
    raise LLMError(str(last_error))


def call_llm_text_resilient_with_meta(cfg: LLMConfig, input_text: str) -> tuple[str, int, int | None, str | None]:
    """Like call_llm_text_resilient, but returns (text, retries, last_code, last_message)."""

    attempts = max(0, int(cfg.max_retries)) + 1
    last_code: int | None = None
    last_msg: str | None = None

    for i in range(attempts):
        try:
            return call_llm_text(cfg, input_text), i, last_code, last_msg
        except LLMError as e:
            last_code = e.status_code
            last_msg = str(e)
            if e.status_code is not None and e.status_code not in _RETRYABLE_STATUS:
                raise
        except Exception as e:
            last_msg = str(e)

        if i < attempts - 1:
            time.sleep(max(0.0, float(cfg.retry_backoff_seconds)) * (2**i))

    raise LLMError(last_msg or "LLM failed", status_code=last_code)


def _call_openai_compatible(cfg: LLMConfig, input_text: str) -> str:
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

    raw_content = _stream_request(url, payload, headers=_headers(cfg), timeout=cfg.timeout_seconds)
    
    # Apply think tag filtering if enabled
    if cfg.filter_think_tags:
        f = ThinkTagFilter()
        result = f.feed(raw_content)
        result += f.flush()
        return result
    return raw_content


def _call_gemini(cfg: LLMConfig, input_text: str) -> str:
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

    raw_content = _stream_request(url, payload, headers=_headers(cfg), timeout=cfg.timeout_seconds)
    
    # Apply think tag filtering if enabled
    if cfg.filter_think_tags:
        f = ThinkTagFilter()
        result = f.feed(raw_content)
        result += f.flush()
        return result
    return raw_content
