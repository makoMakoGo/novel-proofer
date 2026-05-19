# Agent Guide

## Project Overview

Novel Proofer is a local Chinese novel formatting/proofreading tool. It has a FastAPI backend in `novel_proofer/` and a static HTML/Tailwind frontend in `templates/`.

## Setup And Run

- Use `uv`; do not install Python packages globally.
- Sync dev dependencies: `uv sync --frozen --no-install-project --group dev`
- Run the server: `uv run --frozen --no-sync -m novel_proofer.server`
- One-command startup is also available via `start.bat` on Windows or `bash start.sh` on Unix-like shells.

## Validation

Run these before handing off code changes:

```bash
uv run --frozen --no-sync ruff format --check
uv run --frozen --no-sync ruff check
uv run --frozen --no-sync python -m mypy novel_proofer
uv run --frozen --no-sync python -m pytest -q
uv run --frozen --no-sync python tools/export-openapi.py --check
uv run --frozen --no-sync python tools/check-large-files.py
```

`pytest` addopts in `pyproject.toml` enforce coverage, branch coverage, marker strictness, and slow-test duration reporting.

## Development Notes

- Keep generated/runtime data out of commits: `.env*` (except examples), `output/`, caches, and `tests/.artifacts/` are ignored.
- If you change API routes or response models, regenerate the schema with `uv run --frozen --no-sync python tools/export-openapi.py`.
- If you change Tailwind classes in `templates/index.html` or `templates/static/js/**`, run `npm ci && npm run build:css`.
- Do not log API keys, authorization headers, or request payloads containing secrets. Use `novel_proofer.logging_setup.RedactingFormatter` for app file logs.
- Prefer existing modules and patterns: API orchestration in `api.py`, job state in `jobs.py`/`workflow.py`, formatting in `formatting/`, and LLM calls in `llm/`.

See `CLAUDE.md` for the detailed architecture and endpoint reference.
