# llm Specification

## Purpose
LLM 客户端行为规范，包括流式请求、可选参数透传、思考内容过滤等。

## Requirements

### Requirement: Mandatory streaming requests
系统 SHALL **强制**使用流式（SSE）请求调用 LLM API：
- OpenAI-compatible API 使用 `stream: true` 参数
- Gemini API 使用 `alt=sse` 查询参数
- 流式请求 SHALL 在收到每个 chunk 时保持连接活跃
- **流式请求是强制的，不可配置关闭**

#### Scenario: Large chunk processing with streaming
- **GIVEN** 一个需要处理超过 60 秒的大文本切片
- **WHEN** 系统发起 LLM 请求
- **THEN** 使用流式请求
- **AND** 连接在整个处理过程中保持活跃
- **AND** 不会因网关 60s 超时而断开

#### Scenario: All requests use streaming
- **GIVEN** 任意 LLM 请求
- **WHEN** 系统发起请求
- **THEN** 始终使用流式模式
- **AND** 无法通过配置切换为非流式

### Requirement: Optional extra parameters passthrough
系统 SHALL 支持用户通过 UI 配置可选的额外参数透传：
- 用户可在配置界面输入任意 JSON 对象
- JSON 对象将被合并到 LLM 请求体中
- 此功能用于透传 thinking 配置或其他 provider 特定参数
- 参数透传是**可选的**，默认为空

#### Scenario: User configures thinking parameters
- **GIVEN** 用户在配置界面输入 `{"enable_thinking": true, "thinking_budget": 10000}`
- **WHEN** 系统构建 LLM 请求
- **THEN** 请求体包含这些额外参数
- **AND** 参数与其他请求参数合并

#### Scenario: User configures provider-specific parameters
- **GIVEN** 用户输入 `{"temperature": 0.7, "top_p": 0.9}`
- **WHEN** 系统构建 LLM 请求
- **THEN** 这些参数被透传到请求中

#### Scenario: Empty extra parameters
- **GIVEN** 用户未配置额外参数（默认空）
- **WHEN** 系统构建 LLM 请求
- **THEN** 请求仅包含系统必需的参数
- **AND** 不添加任何额外字段

#### Scenario: Invalid JSON input
- **GIVEN** 用户输入无效的 JSON 字符串
- **WHEN** 用户尝试保存配置
- **THEN** 系统显示验证错误
- **AND** 不保存无效配置

### Requirement: Independent think tag filtering
系统 SHALL **独立于参数透传配置**自动检测并过滤思考标签内容：
- 过滤 `<think>...</think>` 标签及其包裹的内容
- 标签匹配 SHALL 大小写不敏感
- 过滤 SHALL 在流式接收时实时进行
- **此功能始终启用，独立于 extra_params 配置**
- 无论用户是否透传 thinking 参数，系统都会检测并过滤思考内容

#### Scenario: Model returns thinking content without thinking params
- **GIVEN** 用户未配置任何 thinking 参数
- **AND** 模型返回包含 `<think>思考过程...</think>实际输出` 的内容
- **WHEN** 系统处理响应
- **THEN** 最终输出仅包含"实际输出"
- **AND** 思考内容被完全过滤

#### Scenario: Model returns thinking content with thinking params
- **GIVEN** 用户配置了 `{"enable_thinking": true}`
- **AND** 模型返回包含思考标签的内容
- **WHEN** 系统处理响应
- **THEN** 思考内容仍被过滤
- **AND** 最终输出不包含 think 标签

#### Scenario: Thinking params configured but model doesn't return thinking
- **GIVEN** 用户配置了 thinking 参数但参数可能错误
- **AND** 模型未返回思考标签内容
- **WHEN** 系统处理响应
- **THEN** 系统正常处理响应
- **AND** 不会因缺少思考内容而报错

#### Scenario: Think tag spans multiple chunks
- **GIVEN** 流式响应中 `<think>` 和 `</think>` 分布在不同 chunk
- **WHEN** 系统处理响应
- **THEN** 系统正确识别跨 chunk 的标签
- **AND** 完整过滤思考内容

#### Scenario: Nested or malformed think tags
- **GIVEN** 响应包含嵌套或不完整的 think 标签
- **WHEN** 系统处理响应
- **THEN** 采用贪婪匹配策略
- **AND** 从第一个 `<think>` 到最后一个 `</think>` 之间的内容被过滤

### Requirement: Think tag filtering toggle
系统 SHALL 支持配置是否启用思考标签过滤：
- `filter_think_tags: bool = True`：是否过滤思考标签
- 默认启用，用户可选择关闭（如需保留思考内容用于调试）

#### Scenario: Filter enabled (default)
- **GIVEN** `filter_think_tags=True`（默认）
- **WHEN** 模型返回思考标签内容
- **THEN** 思考内容被过滤

#### Scenario: Filter disabled
- **GIVEN** 用户配置 `filter_think_tags=False`
- **WHEN** 模型返回思考标签内容
- **THEN** 思考内容保留在输出中
- **AND** 用户可看到完整的模型响应

### Requirement: LLM configuration structure
LLMConfig SHALL 包含以下配置项：
- `extra_params: Optional[dict] = None`：可选的额外参数透传（JSON 对象）
- `filter_think_tags: bool = True`：是否过滤思考标签
- **注意：不再包含 `use_streaming`，因为流式是强制的**

#### Scenario: Config defaults
- **WHEN** 创建默认 LLMConfig
- **THEN** `extra_params` 默认为 `None`
- **AND** `filter_think_tags` 默认为 `True`

#### Scenario: Config with extra params
- **GIVEN** 用户配置 `extra_params={"enable_thinking": true}`
- **WHEN** 系统读取配置
- **THEN** 配置正确解析
- **AND** 参数可用于请求构建
