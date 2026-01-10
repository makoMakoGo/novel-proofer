# llm Specification

## ADDED Requirements

### Requirement: Only OpenAI-compatible provider is supported
系统 SHALL 仅支持 OpenAI-compatible LLM 端点；不再支持 Gemini provider。

#### Scenario: User config selects the only provider
- **GIVEN** 用户启用 LLM
- **WHEN** 系统发起 LLM 请求
- **THEN** 使用 OpenAI-compatible SSE 流式接口

## MODIFIED Requirements

### Requirement: Mandatory streaming requests
系统 SHALL **强制**使用流式（SSE）请求调用 LLM API：
- OpenAI-compatible API 使用 `stream: true` 参数
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
