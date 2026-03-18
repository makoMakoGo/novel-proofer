from __future__ import annotations

import json
import os


def env_truthy(name: str) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from e


def env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be a float, got {raw!r}") from e


def env_json_object(name: str) -> dict | None:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{name} must be valid JSON, got {raw!r}") from e
    if not isinstance(obj, dict):
        raise ValueError(f"{name} must be a JSON object")
    return obj
