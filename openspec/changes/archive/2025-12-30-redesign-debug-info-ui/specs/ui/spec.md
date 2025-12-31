## ADDED Requirements

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
