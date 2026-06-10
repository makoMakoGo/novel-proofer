# Agent Guide

## Project Overview

Novel Proofer is a local Chinese novel formatting/proofreading tool. It has a FastAPI backend in `novel_proofer/` and a static HTML/Tailwind frontend in `templates/`.

It applies deterministic local rules for punctuation, indentation, whitespace, and CJK punctuation normalization, and uses an OpenAI-compatible LLM for semantic formatting such as paragraph splitting, dialogue separation, chapter-title handling, and first-chunk ad/watermark cleanup.

## Setup And Run

Use `uv`; do not install Python packages globally.

```bash
# Sync dev dependencies
uv sync --frozen --no-install-project --group dev

# Run the server
uv run --frozen --no-sync -m novel_proofer.server

# One-command startup
start.bat
bash start.sh
```

The server runs at `http://127.0.0.1:18080` by default unless the startup script chooses another available port.

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

If API routes or response models change, regenerate/check the schema with:

```bash
uv run --frozen --no-sync python tools/export-openapi.py
```

If Tailwind classes change in `templates/index.html` or `templates/static/js/**`, run:

```bash
npm ci
npm run build:css
```

## Architecture

```
novel_proofer/
├── server.py       # Entry point: uvicorn CLI wrapper
├── api.py          # FastAPI app, REST endpoints, request validation
├── jobs.py         # JobStore: thread-safe job/chunk state management
├── workflow.py     # Workflow guards and invariants
├── runner.py       # Orchestrator: chunking -> local rules -> LLM -> merge
├── background.py   # Bounded job-level thread pool
├── models.py       # Pydantic request/response models
├── converters.py   # API conversion helpers
├── formatting/
│   ├── config.py   # FormatConfig dataclass
│   ├── rules.py    # Deterministic text transformations
│   ├── chunking.py # Split text by line boundaries
│   ├── merge.py    # Merge processed chunks
│   └── fixer.py    # Legacy/utility formatters
└── llm/
    ├── config.py   # LLMConfig and system prompts
    ├── client.py   # OpenAI-compatible streaming client with retry logic
    └── think_filter.py
```

## Data Flow

1. Upload (`POST /api/v1/jobs`): cache decoded UTF-8 input and create job status.
2. Validate phase: split text by line boundaries and apply deterministic local rules.
3. Process phase: call the LLM concurrently per chunk, with retry and output validation.
4. Post-process: re-apply local rules to LLM output for consistency.
5. Merge phase: combine chunk outputs and write final output under `output/`.

Runtime and generated data live under `output/`:

- `output/.inputs/{job_id}.txt`: input cache for reruns/recovery.
- `output/.jobs/{job_id}/`: per-job debug/intermediate artifacts.
- `output/.state/jobs/{job_id}.json`: job state snapshots.
- `output/{job_id}_...txt`: final merged output.

Keep generated/runtime data out of commits: `.env*` except examples, `output/`, caches, and `tests/.artifacts/` are ignored.

## Key Concepts

- Job states: `queued`, `running`, `paused`, `done`, `error`, `cancelled`.
- Job phases: `validate`, `process`, `merge`, `done`.
- Chunk states: `pending`, `processing`, `retrying`, `done`, `error`.
- `retrying` is an automatic retry/backoff state within a single chunk processing attempt, not a manual retry queue.
- `reset` is the hard delete/cleanup action for a job. It should not be confused with UI detach/new task.

## Workflow Recovery Refactor

Future workflow recovery work is tracked through GitHub issues/PRs. See `docs/WORKFLOW_RECOVERY_REFACTOR.md` for the approved breaking vocabulary:

- `JobRecord`: durable task and artifact truth.
- `JobExecution`: volatile in-process worker attempt.
- `UiAttachment`: browser-side association with a job id.

The refactor intentionally does not preserve old compatibility shims or silent fallback behavior.

## Local Rules Reference

Rules in `formatting/rules.py` run in this order:

1. Normalize newlines.
2. Trim trailing spaces.
3. Collapse multiple blank lines.
4. Normalize ellipsis.
5. Normalize em dash.
6. Convert ASCII punctuation to fullwidth in CJK context.
7. Remove spaces between CJK and punctuation.
8. Convert straight quotes to curly in CJK lines when enabled.
9. Apply paragraph indent, skipping chapter titles.

## API Endpoints

- `POST /api/v1/jobs` - Create job.
- `POST /api/v1/jobs/{id}/rerun-all` - Re-create job from the same input with new options.
- `GET /api/v1/jobs` - List jobs.
- `GET /api/v1/jobs/{id}` - Get job status and chunks.
- `GET /api/v1/jobs/{id}/input-stats` - Get cached input stats.
- `GET /api/v1/jobs/{id}/download` - Download final output.
- `POST /api/v1/jobs/{id}/pause` / `resume` - Pause/resume.
- `POST /api/v1/jobs/{id}/retry-failed` - Retry failed chunks.
- `POST /api/v1/jobs/{id}/merge` - Merge outputs into the final file.
- `POST /api/v1/jobs/{id}/reset` - Delete job state/intermediates while keeping final output.
- `POST /api/v1/jobs/{id}/cleanup-debug` - Delete intermediate/debug files.
- `POST /api/v1/jobs/purge-all` - Delete all jobs except optionally excluded IDs.
- `GET /api/v1/settings/llm` / `PUT` - Read/write default LLM settings.

## Testing Patterns

Tests use pytest with `httpx.AsyncClient`/FastAPI `TestClient` for API tests.

- `tests/formatting/` - Formatting rules and chunking tests.
- `tests/api/` - REST endpoint integration tests.
- `tests/runner/` - Runner orchestration tests.
- `tests/llm/` - LLM client and think-filter tests.
- `tests/jobs/` - Job store and persistence tests.

Prefer tests that assert external behavior and state transitions instead of private implementation details.

## Development Notes

- Do not log API keys, authorization headers, or request payloads containing secrets.
- Use `novel_proofer.logging_setup.RedactingFormatter` for app file logs.
- Prefer existing modules and patterns:
  - API orchestration in `api.py`.
  - Job state in `jobs.py` and workflow rules in `workflow.py`.
  - Formatting in `formatting/`.
  - LLM calls in `llm/`.
- Avoid silent fallback behavior. Failures should surface clearly through explicit errors, logs, failing tests, or visible job error state.

## GitHub Issue Workflow

Issues and PRDs are tracked in GitHub Issues.

Issue titles must start with one of the repository prefixes:

- `[Bug]`
- `[Feature]`
- `[Suggestion]`
- `[General]`

Use the existing labels when publishing issues:

- `type:bug`, `type:feature`, `type:suggestion`, `type:general`
- `P0`, `P1`, `P2`, `P3`
- `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`

For substantial work, create or update GitHub issues first, then implement through a branch and PR.
