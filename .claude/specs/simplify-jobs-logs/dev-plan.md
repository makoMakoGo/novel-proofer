# Simplify Jobs Logs - Development Plan

## Overview
简化 `.jobs/*` 日志系统，将冗余调试输出精简为最小必要集（pre/out/resp 三个目录），删除 req/error 目录和时间戳文件名。

## Task Breakdown

### Task 1: 核心代码简化
- **ID**: task-1
- **type**: default
- **Description**:
  - 删除冗余函数：`_llm_request_snapshot()`、`_chunk_req_path()`、`_chunk_resp_filtered_path()`、`_chunk_resp_stream_path()`、`_chunk_err_path()`
  - 简化 `_chunk_resp_raw_path()` 为 `_chunk_resp_path()`（移除 `ts_ms` 参数，改为 `resp/{index}.txt` 覆盖式写入）
  - 修改 `_llm_worker()` 函数：删除 `ts_ms` 变量、删除 req JSON 写入、简化 resp 写入（只写 `resp/{index}.txt`）、删除 filtered/stream 写入、删除 error JSON 写入、删除 `.relative_to()` 相关代码
  - 更新 `_JOB_DEBUG_README` 模板反映新目录结构
- **File Scope**: `novel_proofer/runner.py`
- **Dependencies**: None
- **Test Command**: `pytest tests/ -v`
- **Test Focus**:
  - 确保现有功能不受影响（分片处理、暂停/恢复、重试）
  - 验证删除的函数不再被调用

### Task 2: 文档同步
- **ID**: task-2
- **type**: quick-fix
- **Description**: 更新 README.md 中关于 `.jobs` 目录结构的说明，将旧的五目录结构（pre/out/req/resp/error）修改为新的三目录结构（pre/out/resp）
- **File Scope**: `README.md`
- **Dependencies**: depends on task-1
- **Test Command**: N/A（文档变更）
- **Test Focus**: N/A

### Task 3: 测试对齐
- **ID**: task-3
- **type**: default
- **Description**:
  - 新增测试断言 `resp/{index}.txt` 覆盖式写入行为
  - 新增测试确保不再生成 `req/`、`error/` 目录及带时间戳文件
  - 验证暂停/恢复和重试功能与简化后落盘行为兼容
- **File Scope**: `tests/`
- **Dependencies**: depends on task-1
- **Test Command**: `pytest tests/ --cov=novel_proofer --cov-report=term`
- **Test Focus**:
  - Happy path: `_llm_worker` 成功时只生成 `resp/{index}.txt`
  - Error path: `_llm_worker` 失败时不再生成 `error/` 目录
  - Edge case: 空白分片跳过 LLM 时无 resp 文件
  - State transitions: 暂停/恢复时 resp 文件正确覆盖

### Task 4: 可选重构
- **ID**: task-4
- **type**: quick-fix
- **Description**:
  - 合并 `_chunk_pre_path` / `_chunk_out_path` / `_chunk_resp_path` 为工厂函数 `_chunk_path(work_dir, subdir, index)`
  - 提取 `_is_whitespace_only(text)` 辅助函数简化空白判断逻辑
- **File Scope**: `novel_proofer/runner.py`
- **Dependencies**: depends on task-1
- **Test Command**: `pytest tests/ -v`
- **Test Focus**:
  - 确保重构后所有路径生成行为不变
  - 空白判断逻辑与原始 `pre.strip() == ""` 等价

## Acceptance Criteria
- [ ] `req/` 和 `error/` 目录不再生成
- [ ] `resp/` 目录下文件名为 `{index}.txt`（无时间戳），覆盖式写入
- [ ] `_JOB_DEBUG_README` 反映新目录结构
- [ ] README.md 中 `.jobs` 结构说明已更新
- [ ] 暂停/恢复、重试功能正常工作
- [ ] All unit tests pass
- [ ] Code coverage >= 90%

## Technical Notes
- **向后兼容**: 已存在的旧格式 `.jobs` 目录不需要迁移，简化只影响新任务
- **错误处理变更**: 删除 error JSON 后，错误信息仍通过 `GLOBAL_JOBS.update_chunk` 的 `last_error_code` / `last_error_message` 字段保留在内存中，UI 仍可显示
- **调试能力**: `resp/{index}.txt` 保留原始 LLM 响应用于排查；如需保留过滤后内容可通过日志或扩展实现
- **时间戳移除影响**: 多次重试同一分片时 resp 文件会被覆盖，只保留最后一次响应；历史响应不再保留
