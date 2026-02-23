# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Novel Proofer is a Chinese novel formatting/proofreading tool with a FastAPI backend and HTML frontend. It applies deterministic local rules for punctuation/indentation normalization, and uses LLM for semantic formatting (paragraph splitting, dialogue separation, chapter title handling).

## Commands

**推荐使用 `uv`（`pyproject.toml` + `uv.lock`）管理依赖。** `start.bat` / `start.sh` 会优先用 uv，同步 `.venv` 并启动服务；未安装 uv 时会回退到 venv + pip + `requirements.lock.txt`。

```bash
# 一键启动（优先 uv；fallback: venv + pip）
start.bat

# uv（推荐）
uv sync --frozen --no-install-project --group dev
uv run --frozen --no-sync -m novel_proofer.server          # http://127.0.0.1:18080

# pip（fallback）
python -m venv .venv                                    # create venv (first time)
.venv\Scripts\activate                                # activate venv
pip install -r requirements.lock.txt
python -m novel_proofer.server                            # http://127.0.0.1:18080

# tests
uv run --frozen --no-sync pytest -q
pytest -q                                                 # if already in venv

# Windows 自检（自动 sync 并跑测试）
start.bat --smoke
```

## Architecture

```
novel_proofer/
├── server.py      # Entry point: uvicorn CLI wrapper
├── api.py         # FastAPI app, REST endpoints, request validation
├── jobs.py        # JobStore: thread-safe job/chunk state management
├── runner.py      # Orchestrator: chunking -> local rules -> LLM -> merge
├── formatting/
│   ├── config.py  # FormatConfig dataclass
│   ├── rules.py   # Deterministic text transformations (punctuation, indent)
│   ├── chunking.py# Split text by line boundaries for parallel processing
│   └── fixer.py   # Legacy/utility formatters
└── llm/
    ├── config.py  # LLMConfig, system prompts (including first-chunk cleanup)
    ├── client.py  # OpenAI-compatible streaming client with retry logic
    └── think_filter.py  # State machine to strip <think> tags from responses
```

### Data Flow

1. **Upload** (`POST /api/v1/jobs`): File decoded (UTF-8/GBK), JobStatus created
2. **Chunking**: Text split by line boundaries (`chunk_by_lines_with_first_chunk_max`)
3. **Local Rules**: Each chunk processed by `apply_rules()` -> saved to `output/.jobs/{id}/pre/`
4. **LLM**: Concurrent workers call streaming endpoint, retry on 408/429/5xx
5. **Validation**: Output length ratio checked (0.85-1.15x input)
6. **Post-processing**: Local rules re-applied to LLM output for consistency
7. **Merge**: Chunks combined with paragraph separation, output to `output/`

### Key Concepts

- **Chunk states**: `pending` -> `processing` -> `done`/`error`; `retrying` during backoff
- **Job states**: `queued` -> `running` -> `done`/`error`/`paused`/`cancelled`
- **First chunk special handling**: Uses extended system prompt to clean ads/watermarks/metadata
- **Think tag filtering**: State machine removes `<think>...</think>` from reasoning models

## Testing Patterns

Tests use pytest with `httpx.AsyncClient` for API tests. Key fixtures in `conftest.py` set up import paths. Test files mirror module structure:

- `tests/formatting/` - Unit tests for formatting rules/chunking
- `tests/api/` - Integration tests for REST endpoints
- `tests/runner/` - Runner orchestration tests
- `tests/llm/test_client.py` - LLM client with mocked HTTP

## Local Rules Reference

Rules in `formatting/rules.py` (order matters):
1. Normalize newlines (CRLF -> LF)
2. Trim trailing spaces
3. Collapse multiple blank lines to one
4. Normalize ellipsis (`...` -> `......`)
5. Normalize em-dash (`--` -> `——`)
6. Convert ASCII punctuation to fullwidth in CJK context
7. Remove spaces between CJK and punctuation
8. Convert straight quotes to curly in CJK lines (even count only)
9. Apply paragraph indent (two fullwidth spaces), skip chapter titles

## API Endpoints

- `POST /api/v1/jobs` - Create job (multipart: file + options JSON)
- `POST /api/v1/jobs/{id}/rerun-all` - Re-create job from same input with new options
- `GET /api/v1/jobs` - List jobs (filter by state/phase, pagination)
- `GET /api/v1/jobs/{id}` - Get job status and chunks
- `GET /api/v1/jobs/{id}/input-stats` - Cached input stats for UI (non-whitespace char count)
- `GET /api/v1/jobs/{id}/download` - Download final output file
- `POST /api/v1/jobs/{id}/pause` / `resume` - Pause/resume (process phase)
- `POST /api/v1/jobs/{id}/retry-failed` - Retry failed chunks with new LLM config
- `POST /api/v1/jobs/{id}/merge` - Merge outputs into final file
- `POST /api/v1/jobs/{id}/reset` - Delete job (keeps final output in `output/`)
- `POST /api/v1/jobs/{id}/cleanup-debug` - Delete intermediate files (debug)
- `POST /api/v1/jobs/purge-all` - Delete all jobs except optionally excluded IDs
- `GET /api/v1/settings/llm` / `PUT` - Read/write default LLM settings

<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->
