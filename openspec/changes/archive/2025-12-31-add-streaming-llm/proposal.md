# Change: 添加流式 LLM 请求支持

## Why
当前 LLM 调用使用同步 HTTP POST，存在以下问题：
1. 上游网关设定最多 60s 保活，大切片数据处理时间常超过 60s 导致连接断开
2. 无法处理模型返回的 `<think></think>` 思考标签（部分模型自动开启思考）
3. 不同模型的思考参数不统一（`thinking`、`enable_thinking`、`reasoning_effort`）

## What Changes
- **新增流式请求**：默认使用 SSE 流式请求，保持连接活跃避免网关超时
- **智能处理思考标签**：自动识别并过滤 `<think>...</think>` 内容，只返回真正输出
- **思考参数适配**：支持多种思考控制参数的智能处理

## Impact
- Affected specs: 无现有 spec 受影响（新增 llm 能力）
- Affected code:
  - `novel_proofer/llm/client.py`：新增流式请求函数
  - `novel_proofer/llm/config.py`：新增流式相关配置
