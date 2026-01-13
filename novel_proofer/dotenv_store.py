from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DOTENV_ASSIGN_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")

_LOCK = threading.Lock()


def dotenv_path(*, workdir: Path) -> Path:
    override = str(os.getenv("NOVEL_PROOFER_DOTENV_PATH", "") or "").strip()
    if override:
        return Path(override)
    return workdir / ".env"


def _parse_assignment(line: str) -> tuple[str, str] | None:
    raw = str(line or "")
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped.startswith("#"):
        return None
    m = _DOTENV_ASSIGN_RE.match(raw)
    if not m:
        return None
    key = str(m.group(1) or "").strip()
    value = str(m.group(2) or "")
    if not key:
        return None
    return key, value


def _decode_value(raw: str) -> str:
    v = str(raw or "").strip()
    if not v:
        return ""
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    return v


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


@dataclass(frozen=True)
class LLMDefaults:
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    temperature: float | None = None
    timeout_seconds: float | None = None
    max_concurrency: int | None = None
    extra_params: dict[str, Any] | None = None


_ENV_BASE_URL = "NOVEL_PROOFER_LLM_BASE_URL"
_ENV_MODEL = "NOVEL_PROOFER_LLM_MODEL"
_ENV_API_KEY = "NOVEL_PROOFER_LLM_API_KEY"
_ENV_TEMPERATURE = "NOVEL_PROOFER_LLM_TEMPERATURE"
_ENV_TIMEOUT_SECONDS = "NOVEL_PROOFER_LLM_TIMEOUT_SECONDS"
_ENV_MAX_CONCURRENCY = "NOVEL_PROOFER_LLM_MAX_CONCURRENCY"
_ENV_EXTRA_PARAMS = "NOVEL_PROOFER_LLM_EXTRA_PARAMS"

_LLM_ENV_KEYS = (
    _ENV_BASE_URL,
    _ENV_MODEL,
    _ENV_API_KEY,
    _ENV_TEMPERATURE,
    _ENV_TIMEOUT_SECONDS,
    _ENV_MAX_CONCURRENCY,
    _ENV_EXTRA_PARAMS,
)


def read_llm_defaults(path: Path) -> LLMDefaults:
    if not path.exists():
        return LLMDefaults()

    raw_values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_assignment(line)
        if parsed is None:
            continue
        k, v = parsed
        if k in _LLM_ENV_KEYS:
            raw_values[k] = _decode_value(v)

    # Keep empty string ("KEY=") distinct from missing key.
    base_url = raw_values.get(_ENV_BASE_URL)
    model = raw_values.get(_ENV_MODEL)
    api_key = raw_values.get(_ENV_API_KEY)

    temperature: float | None = None
    if raw_values.get(_ENV_TEMPERATURE):
        try:
            temperature = float(raw_values[_ENV_TEMPERATURE])
        except Exception as e:
            raise ValueError(f"{_ENV_TEMPERATURE} must be a float") from e

    timeout_seconds: float | None = None
    if raw_values.get(_ENV_TIMEOUT_SECONDS):
        try:
            timeout_seconds = float(raw_values[_ENV_TIMEOUT_SECONDS])
        except Exception as e:
            raise ValueError(f"{_ENV_TIMEOUT_SECONDS} must be a float") from e

    max_concurrency: int | None = None
    if raw_values.get(_ENV_MAX_CONCURRENCY):
        try:
            max_concurrency = int(raw_values[_ENV_MAX_CONCURRENCY])
        except Exception as e:
            raise ValueError(f"{_ENV_MAX_CONCURRENCY} must be an int") from e

    extra_params: dict[str, Any] | None = None
    raw_extra = raw_values.get(_ENV_EXTRA_PARAMS) or ""
    if raw_extra.strip():
        try:
            obj = json.loads(raw_extra)
        except Exception as e:
            raise ValueError(f"{_ENV_EXTRA_PARAMS} must be a JSON object string") from e
        if not isinstance(obj, dict):
            raise ValueError(f"{_ENV_EXTRA_PARAMS} must be a JSON object string")
        extra_params = obj

    return LLMDefaults(
        base_url=base_url,
        api_key=api_key,
        model=model,
        temperature=temperature,
        timeout_seconds=timeout_seconds,
        max_concurrency=max_concurrency,
        extra_params=extra_params,
    )


def update_llm_defaults(path: Path, *, updates: dict[str, str | None]) -> None:
    """Update managed LLM env keys in a dotenv file, preserving unknown lines.

    Notes:
    - Only keys present in `updates` are modified/added.
    - `None` means "clear" (write as KEY=).
    """

    for k in updates:
        if k not in _LLM_ENV_KEYS:
            raise ValueError(f"unsupported key: {k}")

    with _LOCK:
        lines: list[str] = []
        if path.exists():
            lines = path.read_text(encoding="utf-8").splitlines()

        key_to_index: dict[str, int] = {}
        for i, line in enumerate(lines):
            parsed = _parse_assignment(line)
            if parsed is None:
                continue
            k, _v = parsed
            if k in updates and k not in key_to_index:
                key_to_index[k] = i

        def _format_line(key: str, value: str | None) -> str:
            v = "" if value is None else str(value)
            return f"{key}={v}"

        for key in updates:
            new_line = _format_line(key, updates[key])
            if key in key_to_index:
                lines[key_to_index[key]] = new_line
            else:
                lines.append(new_line)

        content = "\n".join(lines).rstrip("\n") + "\n"
        _atomic_write_text(path, content)


def llm_env_updates_from_defaults_patch(
    patch: LLMDefaults,
    *,
    fields_set: set[str],
) -> dict[str, str | None]:
    """Convert an LLMDefaults patch into dotenv updates.

    - Only fields present in `fields_set` are included.
    - For strings: None clears (KEY=).
    - For extra_params: dict serialized as compact JSON; None clears.
    """

    updates: dict[str, str | None] = {}

    if "base_url" in fields_set:
        updates[_ENV_BASE_URL] = patch.base_url
    if "model" in fields_set:
        updates[_ENV_MODEL] = patch.model
    if "api_key" in fields_set:
        updates[_ENV_API_KEY] = patch.api_key

    if "temperature" in fields_set:
        updates[_ENV_TEMPERATURE] = None if patch.temperature is None else str(float(patch.temperature))
    if "timeout_seconds" in fields_set:
        updates[_ENV_TIMEOUT_SECONDS] = None if patch.timeout_seconds is None else str(float(patch.timeout_seconds))
    if "max_concurrency" in fields_set:
        updates[_ENV_MAX_CONCURRENCY] = None if patch.max_concurrency is None else str(int(patch.max_concurrency))

    if "extra_params" in fields_set:
        if patch.extra_params is None:
            updates[_ENV_EXTRA_PARAMS] = None
        else:
            updates[_ENV_EXTRA_PARAMS] = json.dumps(patch.extra_params, ensure_ascii=False, separators=(",", ":"))

    return updates
