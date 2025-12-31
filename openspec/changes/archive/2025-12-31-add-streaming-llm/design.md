## Context
当前 LLM 客户端使用同步 HTTP POST 请求，在以下场景存在问题：
1. 上游网关（如 Nginx、Cloudflare）通常设置 60s 保活超时
2. 大文本切片处理时间可能超过 60s，导致 504 Gateway Timeout
3. 部分模型会自动开启思考模式，返回 `<think>...</think>` 标签包裹的思考内容

## Goals / Non-Goals
**Goals:**
- 使用流式请求（SSE）保持连接活跃，避免网关超时（强制启用，不可关闭）
- 智能识别并过滤思考标签内容（独立于参数配置）
- 支持用户透传任意额外参数（如 thinking 配置）

**Non-Goals:**
- 不改变现有重试/拆分逻辑
- 不改变 UI 层（进度仍通过 job status 轮询）
- 不支持实时流式输出到前端（仅后端流式接收）

## Decisions

### Decision 1: 强制使用流式请求
- **What**: 流式请求是强制的，不可配置关闭
- **Why**: 流式请求每收到一个 chunk 就会重置网关超时计时器，有效避免 60s 超时
- **No Fallback**: 不再保留非流式请求选项，简化代码

### Decision 2: 思考标签过滤策略（独立于参数配置）
- **What**: 在流式接收时实时检测并过滤 `<think>...</think>` 内容
- **Why**: 
  - 部分模型（如 DeepSeek）即使不传 thinking 参数也会自动思考
  - 有时透传了 thinking 参数但参数错误，模型不会返回思考内容
  - 思考内容对最终输出无用，且会干扰排版结果
- **独立性**: 过滤逻辑独立于 extra_params 配置，始终检测
- **边界条件处理**:
  - 标签可能跨多个 SSE chunk，需要状态机处理
  - 标签可能嵌套或不完整，采用贪婪匹配策略
  - 支持 `<think>` 和 `</think>` 大小写不敏感
- **可配置**: 通过 `filter_think_tags` 开关控制是否过滤（默认开启）

### Decision 3: 额外参数透传
- **What**: 在 LLMConfig 中新增 `extra_params: Optional[dict]` 配置
- **Why**: 
  - 不同 provider 使用不同参数名（thinking、enable_thinking、reasoning_effort 等）
  - 用户可能需要透传其他 provider 特定参数
  - 让用户自行输入 JSON，更加灵活和 robust
- **UI**: 在配置界面添加可选的 JSON 输入框

### Alternatives Considered
1. **增加超时时间**: 不可行，网关超时通常不受应用控制
2. **更小的切片**: 已有自动拆分机制，但不能解决根本问题
3. **WebSocket**: 过度设计，SSE 已足够
4. **预定义 thinking_mode 枚举**: 不够灵活，不同 provider 参数差异大

## Risks / Trade-offs
- **Risk**: 流式解析增加代码复杂度 → 通过状态机封装降低复杂度
- **Trade-off**: 流式请求无法获取完整 token 统计 → 可接受，当前未使用此信息
- **Trade-off**: 用户需要了解 provider 的参数格式 → 可接受，提供示例

## Migration Plan
1. 修改 `call_llm_text` 直接使用流式请求（移除非流式代码）
2. 在 LLMConfig 中添加 `extra_params` 和 `filter_think_tags`
3. 实现 ThinkTagFilter 状态机

## Open Questions
- 无
