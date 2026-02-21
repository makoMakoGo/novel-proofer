# 性能说明（2026-02-21）

本文记录本次性能治理的目标、改动点与实测结果，便于后续回归与二次优化。

## 1. 关键瓶颈

```text
UI 1s 轮询
  -> GET /api/v1/jobs/{id}
     -> JobStore.get()
        -> 深拷贝全部 chunk_statuses
     -> 再遍历 chunk_statuses 统计计数
```

主要问题：

1. 轮询路径读放大：每次 summary 查询都复制全量 chunk 列表。
2. 任务列表读放大：`GET /api/v1/jobs` 也走 full snapshot。
3. Debug 明细高频拉取：调试页每次轮询额外拉 chunk 明细。
4. Worker 等待循环过于频繁唤醒（`timeout=0.1`）。

## 2. 实施改动

### 2.1 JobStore 热路径拆分

- 新增 `get_summary()` / `list_summaries()`，仅返回任务级信息。
- 新增 `get_chunks_page()`，按过滤与分页返回 chunk 明细。
- `JobStatus` 增加 `chunk_counts`，在 `update_chunk()` / `init_chunks()` / `cancel()` 中增量维护。

### 2.2 API 读路径优化

- `GET /api/v1/jobs/{job_id}`：
  - `chunks=0` 走 summary 快路径。
  - `chunks=1` 才走 chunk page 路径。
- `GET /api/v1/jobs` 改为 summary 列表路径。
- `GET /api/v1/jobs/{job_id}/input-stats` 使用 summary 读取任务元信息。

### 2.3 前端轮询与调试节流

- `fetchJobSummary()` 改为 `chunks=0`。
- 轮询改为分阶段频率：
  - `process`: 1000ms
  - `validate/merge`: 1500ms
- Debug chunk 明细拉取增加最小间隔（2500ms）与强制刷新入口（切 tab / 切过滤器）。

### 2.4 其他优化

- `runner` 的 in-flight wait 超时由 `0.1s` 调整为 `0.5s`，减少 busy-wait。
- `chunking` flush 路径避免重复重扫空行索引。
- 非空白字符统计改为 `len("".join(chunk.split()))`，降低 `/input-stats` CPU 开销。

## 3. 基准结果（本地）

### 3.1 JobStore（10,000 chunks）

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| `get_avg_ms` | 7.051 | 8.563 |
| `get_summary_avg_ms` | N/A | 0.010 |
| `list_avg_ms` | 7.510 | 8.909 |
| `list_summaries_avg_ms` | N/A | 0.031 |

说明：`get/list` 保持 full snapshot 语义，因此不追求变快；核心收益来自新增 summary 路径。

### 3.2 API（10,000 chunks）

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| `GET /api/v1/jobs/{id}` summary | 11.707 ms | 1.362 ms |
| `GET /api/v1/jobs?limit=50` | 14.785 ms | 1.635 ms |
| `GET /api/v1/jobs?limit=50`（60 jobs * 1200 chunks） | 74.238 ms | 2.263 ms |

### 3.3 输入字符统计（13.73MB 文本）

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| `_count_non_whitespace_chars_from_utf8_file` | 486.86 ms | 198.42 ms |

## 4. 回归验证

已执行：

- `uv run --frozen --no-sync pytest -q`
- 结果：`124 passed`
