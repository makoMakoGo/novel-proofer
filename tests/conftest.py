from __future__ import annotations

import os
import sys
from pathlib import Path

# Avoid polluting the host environment: tests never create venvs or install deps.
# Use `start.bat --smoke` for a venv-isolated test run.
import pytest

# Ensure the repo root (containing `novel_proofer/`) is importable when pytest
# picks `tests/` as the rootdir (e.g., single-file runs).
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def _load_dotenv(path: Path) -> None:
    """Best-effort .env loader (no external deps).

    Only loads KEY=VALUE lines. Ignores comments and blank lines.
    Does not override existing environment variables.
    """

    try:
        if not path.exists():
            return
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"')
            if not key:
                continue
            os.environ.setdefault(key, value)
    except Exception:
        # Never fail pytest collection due to local env loading.
        return


_load_dotenv(REPO_ROOT / ".env.test")


def _env_truthy(name: str) -> bool:
    from novel_proofer.env import env_truthy

    return env_truthy(name)


def _env_int(name: str, default: int) -> int:
    from novel_proofer.env import env_int

    return env_int(name, default)


def _env_float(name: str, default: float) -> float:
    from novel_proofer.env import env_float

    return env_float(name, default)


def _env_json_object(name: str) -> dict | None:
    from novel_proofer.env import env_json_object

    return env_json_object(name)


def pytest_addoption(parser):
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update golden output files instead of asserting.",
    )
    parser.addoption(
        "--run-llm-tests",
        action="store_true",
        default=False,
        help="Allow tests that call a real external LLM endpoint.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "llm_integration: tests that call a real external OpenAI-compatible endpoint (requires NOVEL_PROOFER_RUN_LLM_TESTS=true or --run-llm-tests)",
    )

    # Hard guard: forbid running tests outside the repo venv.
    # This avoids polluting the host interpreter/site-packages.
    in_venv = (getattr(sys, "base_prefix", sys.prefix) != sys.prefix) or bool(os.getenv("VIRTUAL_ENV"))
    if not in_venv:
        raise RuntimeError(
            "Tests must run inside a virtualenv (.venv). "
            "Use `start.bat --smoke` (recommended) or activate .venv then run `python -m pytest -q`."
        )


def pytest_collection_modifyitems(config, items):
    run_llm_tests = bool(config.getoption("--run-llm-tests")) or _env_truthy("NOVEL_PROOFER_RUN_LLM_TESTS")

    base_url = str(os.getenv("NOVEL_PROOFER_LLM_BASE_URL", "")).strip()
    model = str(os.getenv("NOVEL_PROOFER_LLM_MODEL", "")).strip()

    skip_not_enabled = (
        "LLM integration tests are disabled; set NOVEL_PROOFER_RUN_LLM_TESTS=true or pass --run-llm-tests"
    )
    skip_missing_cfg = "Missing LLM config; set NOVEL_PROOFER_LLM_BASE_URL and NOVEL_PROOFER_LLM_MODEL"

    for item in items:
        if item.get_closest_marker("llm_integration") is None:
            continue

        if not run_llm_tests:
            item.add_marker(pytest.mark.skip(reason=skip_not_enabled))
            continue

        if not base_url or not model:
            item.add_marker(pytest.mark.skip(reason=skip_missing_cfg))
            continue


def llm_config_from_env():
    """Build an LLMConfig from env vars for opt-in integration tests."""

    from novel_proofer.llm.config import LLMConfig  # local import to keep collection cheap

    base_url = str(os.getenv("NOVEL_PROOFER_LLM_BASE_URL", "")).strip()
    model = str(os.getenv("NOVEL_PROOFER_LLM_MODEL", "")).strip()
    api_key = str(os.getenv("NOVEL_PROOFER_LLM_API_KEY", "")).strip()

    extra_params = _env_json_object("NOVEL_PROOFER_LLM_EXTRA_PARAMS")

    return LLMConfig(
        base_url=base_url,
        api_key=api_key,
        model=model,
        temperature=_env_float("NOVEL_PROOFER_LLM_TEMPERATURE", 0.0),
        timeout_seconds=_env_float("NOVEL_PROOFER_LLM_TIMEOUT_SECONDS", 180.0),
        max_concurrency=_env_int("NOVEL_PROOFER_LLM_MAX_CONCURRENCY", 1),
        extra_params=extra_params,
    )


def should_update_golden(pytestconfig) -> bool:
    return bool(pytestconfig.getoption("--update-golden"))
