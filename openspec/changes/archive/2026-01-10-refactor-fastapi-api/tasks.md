## 1. Spec & API Design
- [x] 1.1 新增 `api` capability spec（定义 `/api/v1` 端点、请求/响应、错误结构）
- [x] 1.2 设计并冻结 job JSON 模型（job、chunk、stats、分页）
- [x] 1.3 更新 `llm` spec：移除 Gemini 要求，仅保留 OpenAI-compatible

## 2. Dependencies & Entrypoints
- [x] 2.1 更新 `requirements.txt`：引入 FastAPI/Uvicorn/Pydantic/python-multipart
- [x] 2.2 更新 `start.bat` 启动方式（Uvicorn）
- [x] 2.3 更新 README（移除 stdlib-only 描述、更新启动/测试命令）

## 3. FastAPI Server Implementation
- [x] 3.1 新增 FastAPI app 与路由模块（health、jobs、UI 静态页面）
- [x] 3.2 实现 `POST /api/v1/jobs`（multipart：file + options），启动后台 job
- [x] 3.3 实现 `GET /api/v1/jobs/{job_id}`（含 chunks 过滤与分页）
- [x] 3.4 实现 job 动作端点（cancel/pause/resume/retry-failed/cleanup-debug）
- [x] 3.5 统一错误响应（校验错误、not found、conflict 等）
- [x] 3.6 删除 Gemini provider：精简 LLMConfig/LLM client，仅保留 OpenAI-compatible

## 4. UI Migration
- [x] 4.1 `templates/index.html` 适配新 API（创建 job、轮询、动作按钮、错误展示）
- [x] 4.2 保持 UI 文案契约（不暴露 API 路径；完成提示仍指向 `output/`）

## 5. Remove Legacy HTTP API
- [x] 5.1 移除旧 `/format` 与 `/api/jobs/*` handler 逻辑（或保留 `server.py` 作为 FastAPI 启动器但不再暴露旧路由）
- [x] 5.2 清理相关 dead code/文档/测试

## 6. Tests
- [x] 6.1 用 TestClient 覆盖新 API happy-path（本地模式、LLM 关闭）
- [x] 6.2 覆盖 job 状态机与动作（pause/resume/cancel/retry/cleanup）
- [x] 6.3 更新/移除旧接口相关测试（/format、旧 /api/jobs/*）
- [x] 6.4 `pytest -q` 全通过
