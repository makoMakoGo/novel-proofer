from __future__ import annotations

import pytest

from novel_proofer.background import _max_workers_from_env
from novel_proofer.env import env_float, env_int, env_json_object


def test_env_int_raises_on_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVEL_PROOFER_BAD_INT", "abc")
    with pytest.raises(ValueError, match="must be an integer"):
        env_int("NOVEL_PROOFER_BAD_INT", 1)


def test_env_float_raises_on_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVEL_PROOFER_BAD_FLOAT", "abc")
    with pytest.raises(ValueError, match="must be a float"):
        env_float("NOVEL_PROOFER_BAD_FLOAT", 1.0)


def test_env_json_object_raises_on_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVEL_PROOFER_BAD_JSON", "{bad")
    with pytest.raises(ValueError, match="must be valid JSON"):
        env_json_object("NOVEL_PROOFER_BAD_JSON")


def test_max_workers_from_env_rejects_non_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVEL_PROOFER_JOB_MAX_WORKERS", "0")
    with pytest.raises(ValueError, match="must be >= 1"):
        _max_workers_from_env()
