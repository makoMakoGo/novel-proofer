# Architecture：最终工作流模型

本文记录当前实现的最终架构，只保留仍然真实存在的模块、边界和数据流。

## 设计目标

这个重构要解决的是一个很具体的问题：浏览器页面可以来来去去，但后端任务必须保持自己的生命周期真相。

因此当前架构坚持三条规则：

1. `JobRecord` 是 durable truth。
2. `JobExecution` 是 volatile truth。
3. `UiAttachment` 只是浏览器页面关联，不驱动后端状态。

所有设计都围绕这三件事展开。

## 模块边界

```text
novel_proofer/
├── api.py
├── background.py
├── converters.py
├── executions.py
├── job_records.py
├── jobs.py
├── runner.py
├── workflow.py
├── workflow_context.py
├── formatting/
└── llm/
```

### `workflow.py`

唯一的 workflow 决策层。

- 定义 command decision
- 定义 event transition
- 校验 durable invariant
- 给 API、runner、持久化恢复复用

这里不做 HTTP，不做文件 IO，不做线程调度。

### `jobs.py`

`JobStore` 保存 durable job 状态与 chunk 状态。

- 线程安全更新
- summary/full snapshot
- 持久化到 `JobRecord`
- 重启恢复时把 in-flight workflow 收敛为 `server_recovered`

它不保存线程、Future、页面关联，也不拥有“后台还在不在跑”的判断权。

### `executions.py`

`ExecutionRegistry` 保存当前进程内的 active execution。

- 防止同一 job 重复调度
- 暴露 `queued|running|stop_requested`
- 为 API 快照提供 `execution_state`

它是易失状态，重启后为空。

### `background.py`

负责把命令提交到受控线程池，并把 worker 生命周期绑定到 execution registry。

- `submit()` 先创建 execution
- worker 真正开跑时标记 `running`
- worker 完成/崩溃后清理 execution
- `on_crash` 把 durable job 收敛到显式错误

### `api.py`

薄 API 层。

它只做四件事：

- 请求解析
- 调用 workflow command decision
- 更新 durable state
- 提交后台命令

它不再手写 retry/merge 的状态推导逻辑。

### `runner.py`

执行层。

它只负责：

- validate 阶段的分片与预处理
- process 阶段的 LLM 调度
- merge 阶段的输出合并

runner 不自己决定“这个命令是否合法”；那是 `workflow.py` 的职责。

## 三个核心概念

### `JobRecord`

持久化在 `output/.state/jobs/{job_id}.json`。

它包含：

- workflow：`state/phase/wait_reason`
- artifacts：输入/输出/工作目录/清理策略
- format snapshot
- llm diagnostics
- chunk items 与 chunk counts
- timestamps
- diagnostics

它不包含：

- 线程
- Future
- stop flag
- 页面是否打开

### `JobExecution`

只存在于当前进程内。

它包含：

- `job_id`
- `attempt_id`
- `command`
- `state=queued|running`
- `stop_requested=pause|delete|null`

它不写盘，也不会在重启后伪装成“还活着”。

### `UiAttachment`

只存在浏览器本地。

它的职责是：

- 记住最近关联的 `job_id`
- 页面刷新后自动重新关联
- 新任务时 detach

它不负责 pause、abort、delete、resume。

## 请求与命令流

### 创建任务

```text
POST /api/v1/jobs
  -> prepare_new_job()
  -> 写入输入缓存 output/.inputs/{job_id}.txt
  -> queue validate command
  -> background.submit(validate)
  -> ExecutionRegistry.begin(job_id, validate)
```

### 继续处理

```text
POST /api/v1/jobs/{id}/resume
  -> resume_decision()
  -> apply_command_state()
  -> background.submit(validate|process)
```

### 重试失败分片

```text
POST /api/v1/jobs/{id}/retry-failed
  -> decide_command(RETRY_FAILED)
  -> retry_failed_chunk_indices()
  -> apply_command_state()
  -> 仅重置 failed chunks
  -> background.submit(retry_failed, retry_targets)
```

这里的重要约束是：retry targets 由命令边界冻结，runner 不再扫描 `pending/retrying` 猜哪些该重试。

### 合并输出

```text
POST /api/v1/jobs/{id}/merge
  -> decide_command(MERGE)
  -> apply_command_state()
  -> background.submit(merge)
```

merge 的合法性由 workflow 层统一判断，failed chunks 和 incomplete chunks 使用不同 rejection code。

## validate / process / merge

### validate

`run_job()` 的 validate 部分会：

- 读取输入缓存或上传输入
- 依照 `FormatConfig` 分片
- 执行确定性预处理规则
- 把预处理文本暂存到 `JobStore` 的内存 pre-text map
- 初始化 chunk 列表
- 结束时进入 `paused + ready_to_process`

为了降低热路径小文件 IO，预处理文本默认不作为 `pre/*.txt` 持久化 truth。

### process

`resume_paused_job()` 与 `retry_failed_chunks()` 在调度 worker 前都会确保目标分片的预处理文本存在：

- 如果内存里还在，直接用
- 如果内存已丢失，但输入缓存还在，就按当前 job 的 format snapshot 重新构建目标分片预处理文本
- 如果输入缓存缺失，明确失败

这样恢复依赖 durable 输入缓存，而不是调试目录里的兼容小文件。

### merge

`merge_outputs()` 只处理已经被 workflow 层允许的任务。

- 读取 `out/*.txt`
- 统一合并段落边界
- 执行合并后的本地格式收敛
- 写入最终输出
- 按配置决定是否删除调试目录

## 输入缓存与调试目录

### 输入缓存

`output/.inputs/{job_id}.txt` 是以下能力的 durable 依赖：

- `rerun-all`
- `/input-stats`
- 重启后 `resume`
- 重启后 `retry-failed`

输入缓存缺失时，系统应该明确报错，而不是退回调试目录做 silent fallback。

### 调试目录

`output/.jobs/{job_id}/` 仍然存在，但它只是调试工位。

当前目录语义：

- `out/`：分片最终输出
- `resp/`：可选 raw 响应
- `pre/`：保留目录结构，不是恢复来源，也不是 input-stats 来源
- `README.txt`：说明文件

删除调试目录不会改变 `JobRecord` 的 durable truth；删除输入缓存才会影响恢复能力。

## 重启恢复

服务启动时：

```text
GLOBAL_JOBS.load_persisted_jobs()
  -> 读取 JobRecord
  -> queued/running => paused + server_recovered
  -> processing/retrying chunks => pending
```

恢复后的任务是“诚实可继续”的，而不是“假装还在运行”。

这意味着：

- UI 会看到 `execution_state=idle`
- `wait_reason=server_recovered`
- 用户必须显式继续

## 前端契约

前端只依赖公开快照：

- `workflow_phase`
- `execution_state`
- `wait_reason`
- `terminal_state`
- `available_commands`

前端还做两件事：

- 用 `UiAttachment` 记住当前加载的任务
- 在 `pagehide` / `beforeunload` 时停止本地 observer

前端不会：

- 在页面关闭时 pause 后端任务
- 从内部 `paused` 推导 `ready_to_process` 或 `ready_to_merge`
- 从调试目录推导任务状态

## 为什么这样更干净

这个架构的关键不是“模块更多”，而是事实来源更少：

- lifecycle 决策只在 `workflow.py`
- durable truth 只在 `JobRecord`
- volatile truth 只在 `ExecutionRegistry`
- 页面关联只在 `UiAttachment`

每层都少猜一点，整个系统就少一堆互相打架的小聪明。
