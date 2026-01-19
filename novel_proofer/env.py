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
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def env_json_object(name: str) -> dict | None:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return None
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError(f"{name} must be a JSON object")
    return obj
