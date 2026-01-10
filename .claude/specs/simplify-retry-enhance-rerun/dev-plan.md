# 简化重试逻辑并增强 Rerun 功能 - 开发计划

## 概述
移除自动重试和自动切分逻辑，删除硬编码的 max_tokens，添加 max_chunk_chars 上限校验，实现中间过程持久化，并增强 rerun 功能支持单 chunk 和批量 rerun。

## 任务分解

### Task 1: 移除自动重试和自动切分逻辑
- **ID**: task-1
- **type**: default
- **描述**: 删除 LLM 客户端和 runner 中的自动重试和自动切分相关代码
- **文件范围**: `novel_proofer/llm/client.py`, `novel_proofer/runner.py`, `novel_proofer/formatting/fixer.py`
- **依赖**: None
- **测试命令**: `python -m pytest tests/ -v --cov=novel_proofer/llm --cov=novel_proofer/runner --cov=novel_proofer/formatting --cov-report=term --cov-report=html`
- **测试重点**:
  - 验证 LLM 调用不再包含重试逻辑
  - 确认 `_split_text_in_half` 函数被完全移除
  - 测试 `call_llm_text` 直接调用路径正常工作
  - 验证 chunk 处理不再有自动切分行为
  - 确认 `format_txt` 中使用新的简单调用方式

### Task 2: 移除 max_tokens 硬编码和配置项清理
- **ID**: task-2
- **type**: quick-fix
- **描述**: 删除 `_call_openai_compatible` 中的 `"max_tokens": 4096` 硬编码，并清理 LLMConfig 中不再使用的配置项
- **文件范围**: `novel_proofer/llm/client.py`, `novel_proofer/llm/config.py`
- **依赖**: None
- **测试命令**: `python -m pytest tests/ -v --cov=novel_proofer/llm --cov-report=term`
- **测试重点**:
  - 确认 max_tokens 不再出现在请求 payload 中
  - 验证删除的配置项不会影响现有功能
  - 测试 extra_params 合并逻辑仍然正常
  - 确认 Gemini 路径不受影响

### Task 3: max_chunk_chars 上限校验和中间过程持久化
- **ID**: task-3
- **type**: default
- **描述**: 实现前后端 max_chunk_chars 上限 4000 校验，并在 runner 中保存 LLM 分片输入/输出与原始响应到工作目录
- **文件范围**: `novel_proofer/runner.py`, `novel_proofer/server.py`, `templates/index.html`
- **依赖**: None
- **测试命令**: `python -m pytest tests/ -v --cov=novel_proofer/runner --cov=novel_proofer/server --cov-report=term`
- **测试重点**:
  - 后端校验 max_chunk_chars <= 4000
  - 前端 HTML 输入框 max 属性设置为 4000
  - 验证中间过程文件正确保存到 `output/.jobs/<job_id>/pre/`, `out/`, `resp/` 目录
  - 测试文件命名包含 chunk_index
  - 确认并发场景下文件写入安全
  - 验证 LLM 响应内容正确落盘到 resp 目录

### Task 4: 增强 rerun 功能
- **ID**: task-4
- **type**: default
- **描述**: 扩展 `retry_failed_chunks` 支持指定 indices，新增 `/api/jobs/rerun_chunks` 端点，前端添加单 chunk rerun 和批量 rerun UI
- **文件范围**: `novel_proofer/runner.py`, `novel_proofer/server.py`, `templates/index.html`
- **依赖**: None
- **测试命令**: `python -m pytest tests/ -v --cov=novel_proofer/runner --cov=novel_proofer/server --cov-report=term`
- **测试重点**:
  - `retry_chunks(job_id, indices, llm)` 新函数正确实现
  - `/api/jobs/rerun_chunks` 端点正确处理 indices 参数
  - 前端复选框正确显示在 chunk 表格行首
  - 单 chunk rerun 按钮点击正确发起请求
  - 批量 rerun 按钮正确收集选中 indices 并发送
  - "一键 rerun 所有失败" 按钮正确过滤错误状态 chunks
  - 验证 rerun 过程中状态更新正确（pending → processing → done/error）
  - 测试并发 rerun 不会产生数据竞争

## 验收标准
- [ ] `call_llm_text_resilient` 和 `call_llm_text_resilient_with_meta` 函数已删除
- [ ] `_split_text_in_half` 函数已删除
- [ ] `_call_openai_compatible` 中不再有 `"max_tokens": 4096` 硬编码
- [ ] LLMConfig 中 `max_retries`, `retry_backoff_seconds`, `split_min_chars` 配置项已删除
- [ ] 前后端 max_chunk_chars 上限均为 4000
- [ ] 中间过程文件正确保存到 pre/out/resp 子目录
- [ ] 支持单 chunk rerun、多选批量 rerun、一键 rerun 所有失败
- [ ] 所有单元测试通过
- [ ] 代码覆盖率 ≥ 90%

## 技术要点
- **原子性文件写入**: 使用 `.tmp` 后缀和 `replace()` 确保并发安全
- **状态管理**: rerun 前需重置 chunk 状态为 pending（started_at=None, finished_at=None, last_error_code=None, last_error_message=None）
- **文件命名格式**: `<chunk_index>_<timestamp>.<ext>` 或 `<chunk_index>_<attempt>_<timestamp>.<ext>`
- **前端状态**: 复选框状态独立于轮询状态，需维护本地选中数组
- **API 兼容性**: 保留 `/api/jobs/retry_failed` 端点向后兼容，新增 `/api/jobs/rerun_chunks` 支持更灵活的 rerun
