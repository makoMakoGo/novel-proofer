## ADDED Requirements

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
