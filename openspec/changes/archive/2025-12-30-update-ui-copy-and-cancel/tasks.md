## 1. UI 文案与提示（仅前端视角）
- [x] 1.1 更新主按钮文案为“开始校对”
- [x] 1.2 更新页面提示：不展示任何后端接口路径；仅说明“结果输出到 output/”
- [x] 1.3 完成态提示：显示输出位置（`output/` 或具体路径）

## 2. 取消/暂停交互
- [x] 2.1 UI 增加“取消/暂停”按钮，仅在有活跃任务时可用
- [x] 2.2 点击后取消当前任务并停止轮询
- [x] 2.3 取消成功后 UI 显示 `cancelled` 状态与提示

## 3. 后端取消语义对齐（实现细节）
- [x] 3.1 `JobStatus.state` 明确支持 `cancelled`
- [x] 3.2 取消触发后，任务状态最终应变为 `cancelled`
- [x] 3.3 执行器与 worker：在开始处理 chunk 前检查取消，避免继续发起新的 LLM 请求

## 4. 验证
- [x] 4.1 更新/新增 pytest 用例覆盖取消路径（创建 job → cancel → 状态为 cancelled）
- [x] 4.2 `openspec validate update-ui-copy-and-cancel --strict` 通过
