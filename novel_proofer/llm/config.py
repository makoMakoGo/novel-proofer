from __future__ import annotations

from dataclasses import dataclass


FIRST_CHUNK_SYSTEM_PROMPT_PREFIX = """\
你正在处理整本小说的第一个片段。此片段可能包含网站水印/广告引流/群链接/作者与标签/内容介绍(简介)等“前置信息”。在不改写正文的前提下，你必须额外执行以下清理：

1. 删除所有广告/引流/水印/群链接等垃圾信息（例如包含 Telegram、t.me、禁忌书屋、发布自、免费入群、搜索 @xxx 等字样的行，以及由 = - * _ — 等组成的分隔线）。
2. 若出现“作者/标签/内容介绍/内容简介/简介”等元信息：这些行及其后紧随的简介段落都要删除。
3. 只保留标题：标题应为 1 行，通常位于这些元信息之前或其上方；不要输出作者/标签/简介文字。
4. 如果原文本没有标题（直接正文开头），不要凭空生成标题；直接从正文开始输出。

除上述删除外，其余正文内容必须保持原意与措辞，不要添加任何解释，只输出纯文本。"""


@dataclass(frozen=True)
class LLMConfig:
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
