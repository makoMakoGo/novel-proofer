# FastAPI API 重构设计

## 目标

- 用 FastAPI 统一 HTTP 层：路由、参数解析、校验、错误响应。
- API 版本化并清理历史接口：所有可公开使用的端点放到 `/api/v1`。
- 保持核心业务不变：runner/jobs/formatting/llm 模块尽量复用。
- LLM 提供商收敛为 OpenAI-compatible（移除 Gemini 分支）。


## 路由规划（v1）

### Health

- `GET /healthz`
  - 200: `{"ok": true}`

### Jobs（异步）

- `POST /api/v1/jobs`
  - Content-Type: `multipart/form-data`
  - Parts:
    - `file`: 上传的 `.txt`
    - `options`: JSON 字符串（包含 format/llm/output 等配置）
  - 201: `{"job": <Job>}`

- `GET /api/v1/jobs/{job_id}`
  - Query:
    - `chunks`: `0|1`（默认 `1`）
    - `chunk_state`: `all|pending|processing|retrying|done|error`（默认 `all`）
    - `limit`: `int`（默认 0=不分页）
    - `offset`: `int`（默认 0）
  - 200: `{"job": <Job>, "chunks": [...], "chunk_counts": {...}, "has_more": bool}`

### Job Actions

- `POST /api/v1/jobs/{job_id}/cancel`
- `POST /api/v1/jobs/{job_id}/pause`
- `POST /api/v1/jobs/{job_id}/resume`
- `POST /api/v1/jobs/{job_id}/retry-failed`
  - JSON body: `{"llm": {...}}`（允许用户修改 LLM 配置后重试）
- `POST /api/v1/jobs/{job_id}/cleanup-debug`

说明：
- action 端点保持幂等（重复 cancel/pause/cleanup 不应报 500）。
- `retry-failed` 仅在存在失败 chunk 时生效，否则返回 ok 但不做处理。


## 数据模型（高层）

### Job
- `id: str`
- `state: queued|running|paused|done|error|cancelled`
- `progress: { total_chunks, done_chunks, percent }`
- `input_filename: str`
- `output_filename: str`
- `output_path: str`（用于 UI 提示；不提供下载接口）
- `stats: { ... }`
- `error: Optional[str]`
- `created_at/started_at/finished_at: float|None`

### Chunk
- `index: int`
- `state: pending|processing|retrying|done|error`
- `retries: int`
- `input_chars/output_chars: int|None`
- `last_error_code/last_error_message: Optional[...]`


## 错误响应

统一为：

```json
{
  "error": {
    "code": "not_found|bad_request|conflict|internal_error",
    "message": "..."
  }
}
```

FastAPI 的 validation error 映射为 `bad_request`，并将关键信息压缩成可读 message（UI 不展示 API 路径）。


## 实现策略

- HTTP 层只做：解析/校验 → 调用现有业务函数（runner/jobs/formatting/llm）→ 返回 JSON。
- job 执行仍采用后台线程（沿用当前 `run_job()` 方式），UI 通过轮询查询进度。
- LLM 调用继续使用项目内置的流式 HTTP 客户端（不引入 OpenAI 官方 SDK），以保持对 OpenAI-compatible 服务的兼容性与可控的流解析/调试信息。
- 兼容入口：可保留 `python -m novel_proofer.server` 作为 uvicorn 启动器（但不再提供旧路由），便于 Windows 用户继续双击 `start.bat`。
