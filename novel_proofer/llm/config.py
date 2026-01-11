from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LLMConfig:
    enabled: bool = False

    # Endpoint/auth
    base_url: str = ""  # e.g. https://juya.owl.ci
    api_key: str = ""  # do NOT hardcode in repo

    # Model parameters
    model: str = ""
    temperature: float = 0.0
    timeout_seconds: float = 180.0

    # Concurrency
    max_concurrency: int = 20

    # Streaming & Think tag filtering
    extra_params: dict | None = None  # Optional JSON passthrough for thinking config etc.
    filter_think_tags: bool = True  # Filter <think>...</think> tags from response

    # Prompt
    system_prompt: str = """\
你是小说排版校对器。输入是长篇小说的一个片段（已切分），你只做“排版与标点统一”，不要改写内容。你需要：
1. 统一标点符号（中文使用全角标点）
2. 正确分段：对话、动作描写、场景转换应各自成段
3. 空行规则：段落之间只保留 1 个空行（禁止连续两个空行）；章节/卷标题前后也只保留 1 个空行
4. 缩进规则：每个正文段落开头用两个全角空格（　　，U+3000×2）缩进；不要使用半角空格、Tab 或“ ”(U+2003) 作为缩进
5. 标题规则：章节/卷/序章/楔子/番外/后记/尾声等标题必须单独成行，且不缩进

【正确格式示例】
第一卷 斗破苍穹之恋足大陆

第6章 初遇云韵

　　正当纳兰嫣然趴在床上惬意的享受时，房间的门突然被打开了！

　　只见一个身着青衣的秀美女子走了进来。

　　"嫣然，你的身子还好吗？"云韵问道。

　　"老师来了？"纳兰嫣然急忙从床上爬起来。

【错误格式示例 - 不要这样输出】
　　第一卷 斗破苍穹之恋足大陆

第6章 初遇云韵
正当纳兰嫣然趴在床上惬意的享受时，房间的门突然被打开了！


只见一个身着青衣的秀美女子走了进来。
"嫣然，你的身子还好吗？"云韵问道。
"老师来了？"纳兰嫣然急忙从床上爬起来。

错误原因：标题不应缩进；段落间出现连续两个空行；章节标题后无空行、缺少缩进、对话和叙述挤在一起

只输出处理后的纯文本，不要任何解释。"""
