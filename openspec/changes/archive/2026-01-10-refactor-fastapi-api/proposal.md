## Why

当前服务端基于 `ThreadingHTTPServer` + 手写路由/表单解析，随着功能增长（异步 job、分片状态轮询、重试/暂停/取消、调试中间文件落盘等），维护成本与一致性风险都在上升：

- 接口风格不统一（历史同步 `/format` + 现有 `/api/jobs/*`），并且“旧接口”仅为兼容而保留，形成长期包袱。
- 参数解析与校验分散在 handler 内，边界条件多、测试覆盖成本高。
- 无标准化的 API schema / 文档，调试与二次开发不便。

本次重构接受 breaking change：**删除旧接口并重新设计更干净的 HTTP API**，同时引入 FastAPI/Uvicorn/Pydantic 提升可维护性、可测试性与可观测性。


## What Changes

- **服务端框架迁移**：用 FastAPI 替换 `ThreadingHTTPServer`，用 Uvicorn 作为运行入口。
- **API 全面重设计**：移除 `/format` 与现有 `/api/jobs/*`，新增版本化 API（`/api/v1/...`）并提供一致的 JSON 错误结构。
- **移除 Gemini 支持**：删除 Gemini provider 相关代码/文档/测试，仅保留 OpenAI-compatible 流式调用路径。
- **OpenAI 调用不使用官方 SDK**：继续使用项目内置的流式 HTTP 客户端实现（便于保持对“OpenAI-compatible”服务的广泛兼容与可控的流解析/调试信息）。
- **核心功能保持不变**：
  - 输入 TXT → 分片 → 本地规则 →（可选）LLM 并发处理 → 全部成功后合并输出到 `output/`
  - job 状态可查询、可取消、可暂停/继续、可重试失败分片、可清理调试中间文件
- **UI 同步迁移**：`templates/index.html` 适配新 API（不改用户文案与交互目标）。
- **测试体系迁移**：将原先依赖旧接口的测试改为覆盖新 API；优先使用 FastAPI TestClient，减少真实端口/线程带来的不稳定。
- **部署方式更新**：`start.bat` 与 README 更新为 FastAPI 启动方式；`requirements.txt` 增加依赖（不再 stdlib-only）。


## Impact

- **BREAKING**：旧接口路径（`/format`、`/api/jobs/*`）将不可用；UI 与测试将随之更新。
- **依赖新增**：引入 `fastapi`、`uvicorn`、`pydantic`、`python-multipart`（用于文件上传表单）。
- **运行方式变化**：从 `python -m novel_proofer.server` 迁移到 `uvicorn ...`（或保留一个兼容启动器但不再提供旧 API）。


## Non-goals

- 不修改排版/LLM 的业务语义（仅迁移接口与运行框架）。
- 不引入鉴权/多用户/持久化数据库等新能力。
- 不改变输出文件布局（仍写入 `output/`，调试目录仍在 `output/.jobs/<job_id>/`）。
