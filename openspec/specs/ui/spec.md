# ui Specification

## Purpose
用户界面行为规范：包括表单参数、进度与完成提示、调试信息展示与过滤、暂停/取消/重试等交互，并确保 UI 以用户目标为中心，不暴露后端接口细节。
## Requirements
### Requirement: UI uses user-centered labels
页面 SHALL 以用户目标为中心，不使用实现细节作为按钮文案：
- 主按钮文案 SHALL 为“开始校对”。

#### Scenario: User starts proofreading
- **WHEN** 用户点击“开始校对”
- **THEN** UI 进入处理状态，并显示进度

### Requirement: Completion message reflects actual output
异步任务完成后，UI SHALL 明确告知产物位置，而不是引导用户“下载”：
- 成功提示 SHALL 说明结果已输出到项目 `output/` 目录（或显示具体输出路径）。

#### Scenario: Job completes successfully
- **GIVEN** 校对任务完成
- **THEN** UI 显示完成状态
- **AND** UI 明确提示输出位置为 `output/`（或具体 `output_path`）

### Requirement: User can choose debug artifact cleanup
参数设置页 SHALL 提供“完成后删除调试中间文件”选项：
- 默认勾选（任务成功后自动清理调试中间文件）
- 用户可取消勾选，以便在任务成功后仍能检查中间结果

#### Scenario: User keeps debug artifacts for inspection
- **GIVEN** 用户取消勾选“完成后删除调试中间文件”
- **WHEN** 任务成功完成
- **THEN** 调试中间文件仍然保留可供检查

### Requirement: UI does not expose backend API details
界面提示与文案 SHALL 不包含后端 API 路径或接口名（例如 `/api/...`）。

#### Scenario: User reads help text
- **WHEN** 用户查看页面提示/帮助信息
- **THEN** UI 不出现任何 `/api/...` 字符串

### Requirement: User can cancel an in-progress job
系统 SHALL 提供取消正在进行任务的能力：
- UI 在存在活跃任务时提供“取消/暂停”按钮。
- 用户点击后，UI SHALL 停止继续等待该任务完成，并显示已取消。
- 取消成功后任务状态对外可观测为 `cancelled`。

#### Scenario: User cancels a running job
- **GIVEN** 任务处于进行中
- **WHEN** 用户点击“取消/暂停”
- **THEN** UI 停止轮询并显示已取消
- **AND** 任务状态最终为 `cancelled`

### Requirement: Cancellation prevents further LLM calls
取消操作 SHALL 尽可能快地阻止新的 LLM 请求：
- 在开始处理一个 chunk（或在发起 LLM 请求）之前，系统 SHALL 检查任务是否已取消。
- 若已取消，系统 SHALL 不再为后续 chunk 发起新的 LLM 调用。

#### Scenario: Cancellation happens mid-processing
- **GIVEN** 任务已开始处理多个 chunk
- **WHEN** 用户发起取消
- **THEN** 系统不再启动后续 chunk 的 LLM 调用
- **AND** 任务状态最终为 `cancelled`

### Requirement: User can retry failed chunks
当任务因部分 chunk 失败而进入 `error` 状态时，系统 SHALL 支持用户仅重试失败 chunk：
- UI 在 `error` 状态时提供“重试失败部分”按钮
- 用户可先修改 LLM 配置（如 Base URL/Model/API Key），再触发重试
- 重试 SHALL 仅处理失败 chunk，并保留已完成 chunk 的结果
- **只有当全部 chunk 成功后**，系统才会生成最终输出文件到 `output/`

#### Scenario: User retries failed chunks with a new LLM config
- **GIVEN** 任务状态为 `error`
- **AND** 存在 chunk 状态为 `error`
- **WHEN** 用户点击“重试失败部分”
- **THEN** 系统仅对失败 chunk 重新发起 LLM 调用
- **AND** 已完成 chunk 不会被重复处理
- **AND** 若全部 chunk 成功，任务状态变为 `done` 并生成最终输出文件

#### Scenario: Retry still has failures
- **GIVEN** 用户已触发一次“重试失败部分”
- **WHEN** 重试后仍有 chunk 失败
- **THEN** 任务状态保持为 `error`
- **AND** UI 仍可继续重试失败部分

### Requirement: Debug info displayed in separate tab
系统 SHALL 采用 Tab 切换方式组织页面内容：
- 提供"进度"和"调试信息"两个 Tab
- "进度" Tab 显示主要操作区域（表单、进度条、状态信息）
- "调试信息" Tab 显示 chunk 状态列表
- 默认显示"进度" Tab

#### Scenario: User switches to debug tab during processing
- **GIVEN** 校对任务正在进行中
- **WHEN** 用户点击"调试信息" Tab
- **THEN** 页面切换显示 chunk 状态列表
- **AND** Tab 切换不影响后台任务执行

#### Scenario: User switches back to progress tab
- **GIVEN** 用户当前在"调试信息" Tab
- **WHEN** 用户点击"进度" Tab
- **THEN** 页面切换回主操作区域
- **AND** 进度条和状态信息正常更新

### Requirement: Chunk states have visual distinction
系统 SHALL 为不同状态的 chunk 提供视觉区分：
- `pending`（待处理）：灰色/默认样式
- `processing`（处理中）：蓝色/动画指示
- `done`（完成）：绿色/勾选标记
- `error`（错误）：红色/错误图标
- `retrying`（重试中）：橙色/重试图标

#### Scenario: Chunk encounters error
- **GIVEN** 某个 chunk 处理失败
- **WHEN** 调试面板显示该 chunk
- **THEN** 该 chunk 行以红色高亮显示
- **AND** 显示错误代码和简短错误信息

#### Scenario: Chunk is retrying
- **GIVEN** 某个 chunk 正在重试
- **WHEN** 调试面板显示该 chunk
- **THEN** 该 chunk 行以橙色显示
- **AND** 显示当前重试次数

### Requirement: Debug tab shows summary statistics
调试信息 Tab SHALL 在顶部显示汇总统计信息：
- 总 chunk 数
- 已完成数
- 错误数
- 重试中数

#### Scenario: User checks processing summary
- **GIVEN** 校对任务正在进行
- **WHEN** 用户切换到"调试信息" Tab
- **THEN** Tab 顶部显示统计摘要（如"总计 50 | 完成 30 | 错误 2 | 重试 1"）

### Requirement: Debug tab supports filtering
调试信息 Tab SHALL 支持按状态过滤 chunk 列表：
- 提供过滤按钮：全部 / 仅错误 / 仅重试
- 过滤后列表仅显示匹配状态的 chunk

#### Scenario: User filters to show only errors
- **GIVEN** 用户在"调试信息" Tab 且有多个 chunk
- **WHEN** 用户点击"仅错误"过滤按钮
- **THEN** 列表仅显示状态为 error 的 chunk
- **AND** 其他状态的 chunk 被隐藏

### Requirement: Active tab state persists
当前激活的 Tab SHALL 在页面刷新后保持：
- 使用 localStorage 存储当前 Tab
- 页面加载时恢复上次的 Tab 选择

#### Scenario: User refreshes page on debug tab
- **GIVEN** 用户当前在"调试信息" Tab
- **WHEN** 用户刷新页面
- **THEN** 页面自动恢复到"调试信息" Tab
