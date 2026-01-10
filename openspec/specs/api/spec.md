# api Specification

## Purpose
对外 HTTP API 行为规范：定义版本化端点、请求/响应结构、错误语义与 job 生命周期。

## Requirements

### Requirement: API is versioned under /api/v1
系统 SHALL 将对外 API 统一置于 `/api/v1` 前缀下，以便后续演进而不破坏客户端。

#### Scenario: Client calls versioned jobs endpoint
- **WHEN** 客户端创建任务
- **THEN** 使用 `POST /api/v1/jobs`
- **AND** 不依赖任何未版本化的旧路径

### Requirement: Create job via multipart upload and JSON options
系统 SHALL 支持通过上传 TXT 文件创建异步任务：
- 请求为 `multipart/form-data`
- 必须包含 `file`（TXT）与 `options`（JSON 字符串）
- 成功时返回 job 对象，并开始异步处理

#### Scenario: Create a job successfully
- **GIVEN** 用户提供 TXT 文件与合法 options
- **WHEN** 客户端请求 `POST /api/v1/jobs`
- **THEN** 返回 `201`
- **AND** 响应体包含 `job.id`

### Requirement: Job status is queryable with optional chunk list
系统 SHALL 支持查询 job 状态，并可选返回 chunk 列表：
- 默认返回 chunk 状态列表
- 支持按 `chunk_state` 过滤与分页（`limit/offset`）

#### Scenario: Query job status with chunk filtering
- **GIVEN** 已存在一个 job
- **WHEN** 客户端请求 `GET /api/v1/jobs/{job_id}?chunk_state=error&limit=50&offset=0`
- **THEN** 响应仅包含 error 状态的 chunks（最多 50 条）

### Requirement: Job actions are exposed as explicit endpoints
系统 SHALL 提供显式 action 端点控制任务：
- cancel / pause / resume / retry-failed / cleanup-debug
- action 端点应尽可能幂等

#### Scenario: User cancels a running job
- **GIVEN** job 正在运行
- **WHEN** 客户端请求 `POST /api/v1/jobs/{job_id}/cancel`
- **THEN** job 状态最终为 `cancelled`

### Requirement: Errors use a consistent JSON envelope
系统 SHALL 用统一 JSON 结构返回错误：

```json
{"error":{"code":"...","message":"..."}}
```

#### Scenario: Job not found
- **WHEN** 客户端请求不存在的 `job_id`
- **THEN** 返回 `404`
- **AND** JSON 体包含 `error.code = "not_found"`

