# Project Context

## Purpose
本项目是一个“小说排版校对器（TXT）”的本地 Web 服务：
- 面向中文网络小说/长文本 `.txt` 的排版与标点统一
- **默认目标是“只做排版与标点统一，不做内容改写”**，尽量保持原意、措辞、段落结构与行数稳定（见 `novel_proofer/llm/config.py` 的 `LLMConfig.system_prompt`）
- 支持两条处理路径：
  - 本地规则（纯 stdlib、确定性、保守）
  - 可选 LLM（用于更复杂的对话/标点处理；有并发与重试）

运行形态：
- 本地启动 HTTP 服务并打开页面上传 TXT（页面模板：`templates/index.html`）
- 输出文件默认写入 `output/` 目录（由 `novel_proofer/server.py` 创建）

## Tech Stack
- 语言：Python 3（当前环境 `python --version` 显示 3.14.x）
- 依赖：可引入成熟的、社区认可的第三方库（如 FastAPI、httpx、pytest 等），选择最合适的方案
- Web Server：FastAPI（`novel_proofer/api.py`） + Uvicorn（`novel_proofer/server.py` 启动器）
- 并发：`concurrent.futures.ThreadPoolExecutor`（LLM 分片并发，`novel_proofer/runner.py`）
- LLM 访问：`urllib.request` 直接发 HTTP JSON（`novel_proofer/llm/client.py`）
  - OpenAI-compatible：`POST {base_url}/v1/chat/completions`（SSE 流式）
- 前端：单页 HTML + 原生 JS（`templates/index.html`），可按需引入框架或库
- Windows 便捷启动：`start.bat`（创建/激活 `.venv`，可选安装 `requirements.txt`，自动选空闲端口，启动服务）

## Project Conventions

### Code Style
- 代码风格：偏 PEP 8、4 空格缩进、显式类型标注（`from __future__ import annotations` 广泛使用）
- 配置承载：用 `@dataclass(frozen=True)` 定义配置对象
  - 排版规则：`novel_proofer/formatting/config.py` → `FormatConfig`
  - LLM：`novel_proofer/llm/config.py` → `LLMConfig`
- 命名：模块/函数/变量使用 `snake_case`；常量使用全大写（例如 `OUTPUT_DIR`）

### Architecture Patterns
核心模块分层（建议保持这种分层，避免把逻辑堆到 handler 里）：
- HTTP 层：`novel_proofer/api.py`
  - FastAPI app + 路由/校验/统一错误结构
  - 主要端点：
    - `GET /`：返回页面模板
    - `GET /healthz`：健康检查
    - `POST /api/v1/jobs`：创建异步任务（multipart：file + options JSON）
    - `GET /api/v1/jobs/{job_id}`：查询任务状态（可选返回 chunk 列表，支持过滤与分页）
    - `POST /api/v1/jobs/{job_id}/cancel|pause|resume|retry-failed|cleanup-debug`：任务动作端点
- 任务与状态：`novel_proofer/jobs.py`
  - `JobStore` 为内存态任务存储（线程锁保护），用于进度、错误与统计信息
- 执行器：`novel_proofer/runner.py`
  - `run_job()`：按行分片→本地规则预处理→（可选）LLM 并发处理→本地规则二次收敛→全部成功后合并写最终输出
  - `retry_failed_chunks()`：当存在失败分片时，仅对失败分片继续 LLM 处理；全部成功后再合并输出
  - LLM 失败处理：对可重试/超时/5xx 等错误做重试（不再自动二分拆分）
  - LLM 输出校验：当输入切片较大（>=200 字符）时，输出长度需在输入的 85%~115% 之间；否则该分片标记为 `error`，可换模型/配置后手动重试失败分片
- 排版规则：`novel_proofer/formatting/`
  - `chunking.py`：优先按空行（段落边界）拆分
  - `rules.py`：确定性规则（换行、行尾空格、空行压缩、省略号/破折号、CJK 标点、引号等）
  - `fixer.py`：同步路径的封装（本地规则 + 可选 LLM）
- LLM：`novel_proofer/llm/`
  - `client.py`：HTTP 调用 + 重试/退避
  - `config.py`：提供商/鉴权/并发/重试等参数 + system prompt

数据与文件：
- 上传 TXT：服务端会尝试 `utf-8-sig/utf-8/gb18030/gbk` 解码（`novel_proofer/api.py:_decode_text`）
- 分片工作区：每个 job 会在 `output/.jobs/<job_id>/` 下落盘保存分片中间结果（`pre/` 与 `out/` 等）；默认任务成功后自动清理，可在 UI 取消勾选“完成后删除调试中间文件”来保留
- 最终输出：仅当全部分片成功时，才会将合并结果写入 `output/`（统一为 `utf-8`；文件名会做安全清洗 `_safe_filename`）

### Testing Strategy
测试分为两类：
- `start.bat --smoke`：在虚拟环境中运行 `pytest -q` 做基本自检（会安装 `requirements-dev.txt`）
- pytest 单元测试：覆盖 formatting/runner/api 等关键逻辑（需要安装 `requirements-dev.txt`）
  - 安装：`pip install -r requirements-dev.txt`
  - 运行：`pytest -q`

现有测试（位于 `tests/` 目录）：
- `tests/test_api_endpoints.py`：FastAPI v1 端点基本行为
- `tests/test_think_filter.py`：ThinkTagFilter 单元测试（pytest）

建议测试约定：
- 可按需引入 pytest 等测试框架
- 覆盖关键路径：分片、规则统计、LLM 重试、job 状态更新、文件名安全处理

### Git Workflow
这是个人自用项目（单维护者），工作流以“低摩擦、可回滚”为主：
- Commit message：仍使用 **Conventional Commits**，方便后续回看/生成变更记录
  - 格式：`<type>(<scope>)?: <description>`
  - 常用 `type`：`feat`、`fix`、`docs`、`refactor`、`perf`、`test`、`chore`
  - **BREAKING**：使用 `type!:` 或在 footer 写 `BREAKING CHANGE: ...`
- 分支策略：默认直接在 `main`/`master` 上提交；只有“风险较高/改动较大”时才临时开分支（例如 `feat/*`、`fix/*`），完成后合回并删除
- 合并方式：不强制 PR；如果用了分支，建议 `--ff-only` 或 squash，按你偏好保持历史清晰
- 发布/里程碑（可选）：用 git tag 记录可用版本（例如 `v0.1.0`），重要改动在 commit message 里体现

## Domain Context
- 目标文本：中文小说 TXT（常见包含“第X章/序/番外”等标题行）
- 重要原则：
  - 本地规则是“保守的排版修复”，尽量不改变内容语义
  - LLM 路径也必须遵守“不改写内容”的原则（system prompt 已约束），更像是“智能标点/对话格式修正”
- 典型规则：
  - 段首缩进（可用全角空格 `\u3000`）
  - 段落空行：连续 2+ 空行压缩为 1 空行
  - 省略号统一为“……”；破折号统一为“——”
  - CJK 场景下的标点全角化与去空格
  - 引号统一（默认关闭，属于可能引入歧义的规则）

## Important Constraints
- Windows 友好：提供 `start.bat` 一键启动；端口自动探测（`netstat`）
- 大文件处理：上传体积限制 200MB（`novel_proofer/api.py`），并支持分片并发
- **环境隔离（强制）**：
  - 本项目所有运行/测试/验证都必须使用项目内的虚拟环境解释器：`./.venv/Scripts/python.exe`
  - 不要用主机 `python` 直接启动服务
  - 参考启动脚本：`start.bat` 会创建/激活 `.venv` 并用 `"%VENV_DIR%\Scripts\python.exe" -m novel_proofer.server ...`
- 安全：
  - 不要在仓库里硬编码 `LLM` 的 `api_key`
  - 上传文件名会被清洗（`_safe_filename`）

## External Dependencies
- 可选外部服务：OpenAI-compatible LLM（通过页面表单或 API 传入）
  - 需要 `base_url`/`model`，可选 `api_key`
- 其他：无数据库、无消息队列、无外部存储；任务状态仅保存在内存中（进程重启即丢失）
