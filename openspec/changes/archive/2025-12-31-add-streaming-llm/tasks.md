## 1. Configuration
- [x] 1.1 在 `LLMConfig` 中添加 `extra_params: dict | None = None`
- [x] 1.2 在 `LLMConfig` 中添加 `filter_think_tags: bool = True`

## 2. Think Tag Filter
- [x] 2.1 实现 `ThinkTagFilter` 类，支持流式状态机过滤
- [x] 2.2 处理跨 chunk 的标签边界
- [x] 2.3 支持大小写不敏感匹配

## 3. Streaming Client
- [x] 3.1 实现 `_stream_openai_compatible()` 流式请求函数
- [x] 3.2 实现 `_stream_gemini()` 流式请求函数
- [x] 3.3 集成 ThinkTagFilter 到流式处理
- [x] 3.4 修改 `call_llm_text()` 强制使用流式请求

## 4. Extra Params Passthrough
- [x] 4.1 在请求构建时合并 extra_params 到请求体

## 5. Testing
- [x] 5.1 添加 ThinkTagFilter 单元测试
- [ ] 5.2 添加流式请求 smoke test
- [ ] 5.3 验证网关超时场景
