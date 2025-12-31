# Change: Update UI copy and add cancel/pause flow

## Why
当前 UI 文案与实际行为不一致：页面主按钮写“生成修订稿并下载”，但实际使用方式是“开始校对 → 后台处理 → 结果写入 output/”。对个人自用而言，UI 应只呈现你关心的动作与结果位置，而不暴露任何接口细节。

同时，缺少一个明确的“暂停/取消”操作：当用户点击停止后，应尽快让任务进入 `cancelled` 状态，并避免继续发起新的 LLM 请求；已在进行中的请求无法强制终止时，也要做到“不再继续后续 chunk 的调用”。

## What Changes
- **UI 文案与提示（纯前端视角）**：
  - 主按钮改为“开始校对”。
  - 完成后提示“结果已输出到 output/”（或显示具体输出路径）。
  - UI 不展示任何后端 API 路径或接口名。
- **新增取消/暂停能力（用户可控）**：
  - UI 增加“取消/暂停”按钮用于停止当前任务。
  - 取消后 UI 明确显示已取消，并停止轮询。
- **行为约束**：取消后系统不应继续发起新的 LLM 调用；任务状态对外可观测为 `cancelled`。

## Impact
- Affected capability/spec: `ui`（新增）
- Affected code:
  - `templates/index.html`（按钮文案、提示文案、取消按钮与交互）
  - `novel_proofer/jobs.py`（支持 cancelled 状态/对外状态一致性）
  - `novel_proofer/runner.py`（尽早检查取消，避免继续 LLM 调用与后续 chunk 处理）
  - `novel_proofer/server.py`（取消请求处理与返回结构，如需调整）

## Implementation Reference (not shown in UI)
以下仅作为实现者参考，不应出现在 UI 文案/提示中：
- `GET /health`：健康检查
- `POST /api/jobs/create`：创建异步校对任务
- `GET /api/jobs/status?job_id=...`：轮询任务进度/分片状态
- `POST /api/jobs/cancel`：取消任务
- `GET /api/jobs/download`：当前禁用（410），产物以写入 `output/` 为主
- `POST /format`：遗留同步路径（保留兼容用）

## Non-Goals
- 不强制实现真正的“暂停后可恢复”（resume）。本次“暂停”指用户可取消当前任务。
- 不新增下载端点；输出仍以写入 `output/` 为主（如未来恢复下载可另开 change）。
