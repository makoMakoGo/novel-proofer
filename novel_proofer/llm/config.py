from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LLMConfig:
    enabled: bool = False

    # Provider selection
    provider: str = "openai_compatible"  # openai_compatible | gemini

    # Endpoint/auth
    base_url: str = ""  # e.g. https://juya.owl.ci
    api_key: str = ""  # do NOT hardcode in repo

    # Model parameters
    model: str = ""
    temperature: float = 0.0
    timeout_seconds: float = 180.0

    # Concurrency
    max_concurrency: int = 20

    # Resilience
    max_retries: int = 2
    retry_backoff_seconds: float = 1.0
    split_min_chars: int = 6_000

    # Streaming & Think tag filtering
    extra_params: dict | None = None  # Optional JSON passthrough for thinking config etc.
    filter_think_tags: bool = True  # Filter <think>...</think> tags from response

    # Prompt
    system_prompt: str = """\
你是小说排版校对器。输入是长篇小说的一个片段（已切分），你需要：
1. 统一标点符号（中文使用全角标点）
2. 正确分段：对话、动作描写、场景转换应各自成段，段落间用空行分隔
3. 正确缩进：每个正文段落开头用两个全角空格（　　）缩进，章节标题不缩进
4. 章节标题单独成行，前后各空一行

【正确格式示例】
第6章 初遇云韵

　　正当纳兰嫣然趴在床上惬意的享受时，房间的门突然被打开了！

　　只见一个身着青衣的秀美女子走了进来。

　　"嫣然，你的身子还好吗？"云韵问道。

　　"老师来了？"纳兰嫣然急忙从床上爬起来。

【错误格式示例 - 不要这样输出】
第6章 初遇云韵
正当纳兰嫣然趴在床上惬意的享受时，房间的门突然被打开了！
只见一个身着青衣的秀美女子走了进来。
"嫣然，你的身子还好吗？"云韵问道。
"老师来了？"纳兰嫣然急忙从床上爬起来。

错误原因：章节标题后无空行、段落间无空行、缺少缩进、对话和叙述挤在一起

只输出处理后的纯文本，不要任何解释。"""
