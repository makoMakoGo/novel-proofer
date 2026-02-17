# 小说打样员 / Novel Proofer 技术文档

本文档详细记录小说打样员的处理原理（Why + How），便于理解和后续调整。

## 目录

1. [概述](#1-概述)
2. [系统架构](#2-系统架构)
3. [本地规则系统](#3-本地规则系统)
4. [分块策略](#4-分块策略)
5. [LLM 集成](#5-llm-集成)
6. [状态管理](#6-状态管理)
7. [Runner 编排](#7-runner-编排)
8. [关键设计决策汇总](#8-关键设计决策汇总)

---

## 1. 概述

小说打样员（Novel Proofer）是一个中文小说排版校对工具，采用**本地规则 + LLM** 的混合架构：

- **本地规则**：确定性的字符替换，处理标点、缩进、空行等格式问题
- **LLM**：语义级格式化，处理段落分割、对话分离、章节标题等需要理解上下文的任务

### 为什么需要混合架构？

| 任务类型 | 本地规则 | LLM |
|---------|---------|-----|
| 标点符号统一 | ✅ 确定性、快速 | ❌ 过度杀伤 |
| 段落缩进 | ✅ 规则明确 | ❌ 不需要 |
| 对话与叙述分段 | ❌ 无法理解语义 | ✅ 需要上下文 |
| 章节标题识别 | ⚠️ 部分可行 | ✅ 更准确 |
| 广告/水印清理 | ❌ 变体太多 | ✅ 语义理解 |

---

## 2. 系统架构

### 2.1 模块职责边界

```
novel_proofer/
├── server.py          # 入口：uvicorn CLI 包装
├── api.py             # REST 端点、请求验证、响应序列化
├── background.py      # 后台任务：受控线程池（job 级并发）
├── logging_setup.py   # logging 初始化：文件日志（RotatingFileHandler）
├── dotenv_store.py    # 本地 .env 读写（LLM 默认配置）
├── jobs.py            # JobStore：线程安全 + 可选快照持久化
├── runner.py          # 编排器：chunking → local rules → LLM → merge
├── formatting/
│   ├── config.py      # FormatConfig 数据类
│   ├── rules.py       # 确定性文本转换（标点、缩进）
│   ├── chunking.py    # 按行边界分片（支持文件流式）
│   └── merge.py       # 分片合并（runner/fixer 复用）
└── llm/
    ├── config.py      # LLMConfig、系统提示词
    ├── client.py      # OpenAI 兼容流式客户端（httpx 连接池）+ 重试逻辑
    └── think_filter.py # 状态机过滤 <think> 标签
```

| 模块 | 职责 | 关键方法 |
|------|------|---------|
| `api.py` | REST 端点、请求验证 | `create_job()`, `get_job()`, `pause_job()` |
| `background.py` | 后台任务线程池（job 级并发） | `submit()`, `shutdown()` |
| `dotenv_store.py` | 本地 `.env` 读写（保留未知键/注释） | `read_llm_defaults()`, `update_llm_defaults()` |
| `logging_setup.py` | 文件日志初始化 | `ensure_file_logging()` |
| `jobs.py` | 线程安全状态管理（含持久化） | `configure_persistence()`, `load_persisted_jobs()`, `update_chunk()` |
| `runner.py` | 流程编排 | `run_job()`, `_llm_worker()`, `_finalize_job()` |
| `formatting/rules.py` | 本地规则 | `apply_rules()` |
| `formatting/chunking.py` | 分片 | `chunk_by_lines_with_first_chunk_max()`, `iter_chunks_by_lines_with_first_chunk_max_from_file()` |
| `formatting/merge.py` | 合并输出 | `merge_text_chunks_to_path()`, `merge_text_parts()` |
| `llm/client.py` | LLM 调用 | `call_llm_text_resilient_with_meta_and_raw()` |
| `llm/think_filter.py` | Think 标签过滤 | `ThinkTagFilter.feed()` |

### 2.2 数据流总览

```
上传文件 (POST /api/v1/jobs)
    ↓
上传落盘（限制大小；避免整文件读入内存）
    ↓
转码为 UTF-8 输入缓存（保存到 output/.inputs/{job_id}.txt；用于“重跑全部（新任务）无需重新上传”）
    ↓
后台任务提交（受控线程池；避免阻塞 FastAPI 事件循环）
    ↓
分片（Runner 从文件流式分片：iter_chunks_by_lines_with_first_chunk_max_from_file）
    ↓
本地规则预处理 (apply_rules) → 保存到 pre/
    ↓
LLM 并发处理 (_llm_worker × N)
    ├─ 第一分片：扩展提示词（清理广告/水印）
    ├─ 流式调用 + 重试
    ├─ Think 标签过滤
    ├─ 输出验证（长度比例）
    └─ 保存到 out/
    ↓
本地规则二次收敛 (apply_rules)
    ↓
合并输出（formatting/merge.py）
    ↓
最终文件 → output/
```

同时：
- Job 状态快照：`output/.state/jobs/{job_id}.json`（用于重启恢复）
- 文件日志：`output/logs/novel-proofer.log`（滚动文件，便于排查问题）

---

## 3. 本地规则系统

文件：`novel_proofer/formatting/rules.py`

### 3.1 规则执行顺序

规则在 `apply_rules()` 中按固定顺序执行，**顺序很重要**：

| # | 规则 | Why | How |
|---|------|-----|-----|
| 1 | 换行符统一 | 后续规则依赖 `\n` | `\r\n` / `\r` → `\n` |
| 2 | 行尾空格清理 | 避免干扰缩进处理 | 正则 `[ \t]+(?=\n)` → 空 |
| 3 | 空行规范化 | 保持段落结构清晰 | 3+ 个 `\n` → 2 个 |
| 4 | 省略号统一 | 中文排版规范 | `...` / `。。。` → `……` |
| 5 | 破折号统一 | 中文排版规范 | `--` / `—` → `——` |
| 6 | CJK 标点转换 | 中文使用全角标点 | `,;:!?.` → `，；：！？。` |
| 7 | 标点间距修复 | 移除多余空格 | `你好 ，` → `你好，` |
| 8 | 引号规范化 | 直引号转弯引号 | `"` → `"` / `"`（默认关闭） |
| 9 | 段落缩进 | 段首两个全角空格 | 章节标题不缩进 |

### 3.2 为什么这个顺序？

1. **换行符必须首先统一**：后续所有规则都依赖 `\n` 作为行分隔符
2. **标点规则在缩进前**：缩进会改变行结构，如果先缩进，标点检测会变复杂
3. **缩进必须最后执行**：避免前面的规则破坏已添加的缩进

### 3.3 每条规则详解

#### 规则 1-3：基础清理

```python
# 1. 换行符统一
text.replace("\r\n", "\n").replace("\r", "\n")

# 2. 行尾空格清理
re.subn(r"[ \t]+(?=\n)", "", text)

# 3. 空行规范化（3+ 个换行 → 2 个）
re.subn(r"\n{3,}", "\n\n", text)
```

**Why**：这三个是"卫生"规则，清理格式垃圾，为后续处理创建干净的基础。

#### 规则 4-5：标点符号统一

```python
# 4. 省略号：... / 。。。 / … → ……（两个 U+2026）
_ellipsis_ascii_re.subn("……", text)   # "..." → "……"
_ellipsis_cn_re.subn("……", text)      # "。。。" → "……"
re.subn(r"…{3,}", "……", text)         # 3+ 个 → 2 个

# 5. 破折号：-- / — / ——— → ——（两个 U+2014）
_em_dash_re.subn("——", text)
```

**Why**：中文小说中省略号和破折号有标准形式。统一来自不同输入源的变体（Word、PDF、网页等）。

#### 规则 6：ASCII 标点转全角

```python
def _normalize_cjk_punctuation(text: str) -> tuple[str, int]:
    # 仅在 CJK 上下文中转换，避免破坏英文/代码/URL
    # 例如：你好,世界 → 你好，世界
    # 但：hello,world 保持不变
    # 避免小数：3.14 保持不变
```

**Why**：中文排版规范要求使用全角标点。通过检测相邻字符是否为 CJK 来决定是否转换。

#### 规则 7：CJK 与标点间的空格

```python
def _fix_cjk_punct_spacing(text: str) -> tuple[str, int]:
    # 移除 CJK 字符与标点间的空格
    # 你好 ， → 你好，
    # 你好 。 → 你好。
```

**Why**：中文排版中，CJK 字符与标点间不应有空格。这通常来自 OCR 或不规范的输入。

#### 规则 8：引号转换（默认关闭）

```python
def _normalize_quotes(text: str) -> tuple[str, int]:
    # 仅在含 CJK 的行中转换
    # 仅在引号数量为偶数时转换（确保配对）
    # 交替转换：第 1、3、5... 个 → "，第 2、4、6... 个 → "
```

**Why 默认关闭**：
- 引号转换风险高（奇数引号会破坏文本）
- 代码/URL 中的引号不应转换
- 用户可选择启用

#### 规则 9：段落缩进（最复杂）

```python
def _normalize_paragraph_indent(text: str, config: FormatConfig) -> tuple[str, bool]:
    indent = "　　"  # 两个全角空格 U+3000

    for i, line in enumerate(lines):
        # 1. 章节标题：移除缩进
        if is_chapter_title(line):
            new_line = re.sub(r"^\s+", "", line)
            continue

        # 2. 分隔符行：跳过（如 "===", "---"）
        if is_separator(line):
            continue

        # 3. 段首行（第一行或前一行为空）：添加缩进
        is_para_start = i == 0 or not lines[i - 1].strip()
        if is_para_start:
            new_line = indent + stripped_line

        # 4. 段中行（前一行非空）：移除缩进
        else:
            new_line = stripped_line
```

**章节标题检测** (`is_chapter_title`)：
- 书名号格式：`《标题》`、`【标题】`
- 章节模式：`第X章`、`序章`、`番外`、`后记`、`尾声`
- 全大写英文标题（罕见；仅当该行不含 CJK 字符时生效，避免误判中文段落里的单个字母）

---

## 4. 分块策略

文件：`novel_proofer/formatting/chunking.py`

### 4.1 为什么需要分块？

1. **LLM 上下文限制**：单次请求不能太长，否则会超时或被截断
2. **并发处理**：分片后可以多线程并发调用 LLM，提高处理速度
3. **错误隔离**：单个分片失败不影响其他分片，可以单独重试

### 4.2 分块算法设计

核心函数：`chunk_by_lines()`

```python
def chunk_by_lines(text: str, max_chars: int) -> list[str]:
    lines = text.splitlines(keepends=True)  # 保留换行符
    chunks = []
    buf = []
    size = 0
    last_blank_idx = None  # 追踪最后一个空行的位置

    for line in lines:
        # 如果加入这一行会超过预算
        if buf and size + len(line) > max_chars:
            # 优先在最后一个空行处分割（段落边界）
            if last_blank_idx is not None:
                flush_upto(last_blank_idx)
            else:
                flush_all()  # 没有空行，强制切割

        buf.append(line)
        size += len(line)

        # 追踪空行位置
        if line.strip() == "":
            last_blank_idx = len(buf) - 1
```

**设计考量**：

| 决策 | Why |
|------|-----|
| 按行分割 | 不会在行中间切割，避免破坏文本结构 |
| 优先段落边界 | 在空行处切割，保持段落完整性 |
| 贪心策略 | 尽量填满每个分片，减少分片数量 |

### 4.3 首分片特殊处理

函数：`chunk_by_lines_with_first_chunk_max()`

```python
def chunk_by_lines_with_first_chunk_max(
    text: str, *, max_chars: int, first_chunk_max_chars: int
) -> list[str]:
    # 第一遍：用 first_chunk_max_chars 分割
    first_pass = chunk_by_lines(text, max_chars=first_chunk_max_chars)
    first = first_pass[0]

    # 剩余文本用标准 max_chars 重新分割
    rest_text = "".join(first_pass[1:])
    rest_chunks = chunk_by_lines(rest_text, max_chars=max_chars)

    return [first, *rest_chunks]
```

**Why 首分片需要更大预算**：
- 小说开头通常包含广告、水印、作者信息、简介等"前置信息"
- 需要给 LLM 更多上下文来识别和清理这些内容
- 使用扩展的系统提示词（`FIRST_CHUNK_SYSTEM_PROMPT_PREFIX`）

**配置约束**（在 `runner.py` 中）：
```python
max_chars = max(200, min(4_000, max_chars))  # 限制在 200-4000
first_chunk_max_chars = min(4_000, max(max_chars, 2_000))  # 至少 2000，最多 4000
```

### 4.4 文件流式分片（降低内存）

函数：`iter_chunks_by_lines_with_first_chunk_max_from_file()`

`chunk_by_lines_with_first_chunk_max()` 适合“已有字符串”的场景（例如 `formatting/fixer.py` 直接处理文本）。但 API 上传文件时，如果把整本小说一次性读入内存再分片，既浪费内存，也会让后续扩展（更大输入、更多并发）变得更脆弱。

因此 `runner.py` 使用文件流式分片：按行读取输入缓存文件，保持与字符串版相同的“优先空行边界”的策略，同时避免加载全量文本。

```python
# 第一遍：先统计分片数，初始化 JobStore 的 chunk 列表
total = sum(1 for _ in iter_chunks_by_lines_with_first_chunk_max_from_file(path, ...))
GLOBAL_JOBS.init_chunks(job_id, total_chunks=total)

# 第二遍：逐片处理（本地规则 → 写入 pre/ → LLM 并发处理 → 合并）
for i, chunk in enumerate(iter_chunks_by_lines_with_first_chunk_max_from_file(path, ...)):
    ...
```

---

## 5. LLM 集成

文件：`novel_proofer/llm/`

### 5.1 LLM 的职责

LLM 负责**语义级别的排版与格式化**，补充本地规则的不足：

| 任务 | 说明 |
|------|------|
| 段落分割 | 识别对话、动作描写、场景转换，将其分成独立段落 |
| 对话分离 | 将混在一起的对话和叙述分开 |
| 章节标题处理 | 识别并正确格式化章节/卷/序章等标题 |
| 空行规则 | 确保段落间只有 1 个空行 |
| 缩进规则 | 每个正文段落用两个全角空格缩进 |
| 首分片清理 | 删除广告/水印/群链接/作者信息/简介 |

### 5.2 提示词设计

**基础系统提示词**（所有分片）：

```
你是小说排版校对器。输入是长篇小说的一个片段（已切分），你只做"排版与标点统一"，不要改写内容。
你需要：
1. 统一标点符号（中文使用全角标点）
2. 正确分段：对话、动作描写、场景转换应各自成段
3. 空行规则：段落之间只保留 1 个空行
4. 缩进规则：每个正文段落开头用两个全角空格缩进
5. 标题规则：章节/卷/序章等标题必须单独成行，且不缩进

【正确格式示例】...
【错误格式示例】...
```

**首分片扩展提示词**（`FIRST_CHUNK_SYSTEM_PROMPT_PREFIX`）：

```
你正在处理整本小说的第一个片段。此片段可能包含网站水印/广告引流/群链接/作者与标签/内容介绍等"前置信息"。
在不改写正文的前提下，你必须额外执行以下清理：
1. 删除所有广告/引流/水印/群链接等垃圾信息
2. 若出现"作者/标签/内容介绍"等元信息：这些行及其后紧随的简介段落都要删除
3. 只保留标题
4. 如果原文本没有标题，不要凭空生成标题
```

**设计考量**：
- **明确角色定义**：强调"只做排版，不改写内容"
- **正反例对比**：帮助 LLM 理解预期输出
- **温度设置**：`temperature: 0.0`（确定性输出）

### 5.3 错误处理与重试策略

**可重试的 HTTP 状态码**：

```python
_RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
```

| 状态码 | 含义 | 处理 |
|--------|------|------|
| 408 | Request Timeout | 重试 |
| 429 | Too Many Requests | 重试（速率限制） |
| 500-504 | 服务器错误 | 重试 |
| 401, 403 | 认证/权限错误 | 立即失败 |

**重试策略**：

```python
def call_llm_text_resilient(cfg, input_text):
    attempts = 3  # 最多 3 次尝试

    for i in range(attempts):
        try:
            return call_llm_text(cfg, input_text)
        except LLMError as e:
            if e.status_code not in _RETRYABLE_STATUS:
                raise  # 非可重试错误立即抛出

        # 指数退避：1s, 2s, 4s...
        time.sleep(1.0 * (2 ** i))
```

**输出验证**（`_validate_llm_output`）：

```python
_MIN_VALIDATE_LEN = 200
_SHORTEST_RATIO = 0.85
_LONGEST_RATIO = 1.15

def _validate_llm_output(input_text, output_text, *, allow_shorter=False):
    if len(output_text.strip()) == 0:
        raise LLMError("LLM output empty")

    if len(input_text) >= _MIN_VALIDATE_LEN:
        ratio = len(output_text) / len(input_text)
        if ratio < _SHORTEST_RATIO and not allow_shorter:
            raise LLMError(f"LLM output too short (ratio={ratio:.2f})")
        if ratio > _LONGEST_RATIO:
            raise LLMError(f"LLM output too long (ratio={ratio:.2f})")
```

**Why 首分片允许更短**：首分片可能删除大量广告/水印，输出比输入短是正常的。

### 5.4 Think 标签过滤

文件：`novel_proofer/llm/think_filter.py`

**Why 需要过滤**：
- 某些推理模型（如 DeepSeek）会输出 `<think>...</think>` 标签用于内部推理
- 这些标签不是最终输出的一部分，会污染小说文本

**实现方式**：两态状态机

```python
class _State(Enum):
    NORMAL    # 标签外
    IN_THINK  # 标签内（内容被丢弃）

class ThinkTagFilter:
    _state: _State       # 当前状态
    _buffer: str         # 末尾可能的部分标签（处理跨 chunk 边界）
    _depth: int          # 嵌套深度（支持 <think><think>...</think></think>）

    def feed(self, chunk: str) -> str:
        # NORMAL: 扫描 <think>，遇到则切换到 IN_THINK 并 _depth=1
        #         末尾不足 7 字符的 '<' 缓冲到下次（可能是部分标签）
        # IN_THINK: 扫描 </think>，遇到则 _depth-=1；归零回 NORMAL
        #           遇到嵌套 <think> 则 _depth+=1
        #           标签内全部内容丢弃
        # 大小写不敏感
```

**容错机制**：

```python
_THINK_FILTER_MIN_LEN = 200
_THINK_FILTER_MIN_RATIO = 0.2

def _maybe_filter_think_tags(cfg, raw_content, *, input_text=None):
    filtered = ThinkTagFilter().feed(raw_content)

    # 未闭合标签：回退为仅移除标签标记，保留内容
    if _looks_like_think_unclosed(raw_content):
        return _strip_think_tags_keep_content(raw_content)

    # 如果过滤后输出过短，说明可能过度过滤
    if len(filtered.strip()) < max(_THINK_FILTER_MIN_LEN, int(expected * _THINK_FILTER_MIN_RATIO)):
        return _strip_think_tags_keep_content(raw_content)

    return filtered
```

---

## 6. 状态管理

文件：`novel_proofer/jobs.py`

### 6.1 Job 状态机

```
create()
  ↓
[queued] ─────────────────────────────────────────────────┐
  ↓ run_job() starts                                      │
[running] ◄───────────────────────────────────────────┐   │
  ├─ pause() → [paused]                               │   │
  │             └─ resume() → [queued] → [running] ───┘   │
  ├─ reset() → [cancelled] → delete()                      │
  └─ LLM 处理完成                                          │
      ├─ 无错误 → [done] ✓                                │
      ├─ 有错误 → [error]                                 │
      │           └─ retry_failed_chunks() ───────────────┘
      └─ reset requested → [cancelled]

终态: done, error, cancelled
```

### 6.2 Chunk 状态机

```
init_chunks()
  ↓
[pending] ◄─────────────────────────────────────────┐
  ├─ reset/pause 时重置                              │
  └─ retry 时重置                                    │
  ↓                                                  │
[processing] ──────────────────────────────────────┐│
  ├─ LLM 调用中                                     ││
  ├─ 遇到 408/429/5xx → [retrying] ────────────────┘│
  │                      └─ 重试成功 → [done]        │
  │                      └─ 重试失败 → [error]       │
  └─ 成功 → [done] ✓                                │
  └─ 失败 → [error] ────────────────────────────────┘

终态: done, error
```

### 6.3 JobStore 实现

```python
class JobStore:
    def __init__(self):
        self._lock = threading.Lock()  # 线程安全
        self._jobs: dict[str, JobStatus] = {}
        self._cancelled: set[str] = set()
        self._paused: set[str] = set()
        self._persist_dir: Path | None = None  # 可选持久化目录（output/.state/jobs）
        self._persist_interval_s = 5.0  # 默认 5s（可用环境变量覆盖）
        self._persist_dirty = set()  # dirty job_id 集合（合并写 / 节流）

    def configure_persistence(self, *, persist_dir: Path) -> None:
        self._persist_dir = persist_dir

    def load_persisted_jobs(self) -> int:
        # 读取 output/.state/jobs/*.json 并“自愈”状态，使其在重启后可继续：
        # - queued/running -> paused（因为重启后没有 in-flight 任务）
        # - processing/retrying -> pending（让 resume/retry 可以继续跑）
        ...

    def update(self, job_id, **kwargs):
        flush_now = False
        with self._lock:
            # 已标记为 cancelled（删除任务/reset）的 job 不接受更新
            if st.state == "cancelled":
                return
            # started_at 只接受首次写入
            if k == "started_at" and st.started_at is not None:
                continue
            # paused 状态不被 running 覆盖
            if k == "state" and st.state == "paused" and v in {"queued", "running"}:
                continue

            # 标记 dirty：让后台线程批量落盘（避免每个 chunk 更新都做一次全量 snapshot + 写盘）
            self._persist_dirty.add(job_id)
            flush_now = st.state in {"done", "error", "cancelled"}

        # 终态立即落盘，降低重启后状态回退的概率
        if flush_now:
            self._flush_job(job_id)
```

**设计要点**：
- 所有状态更新通过 `update()` / `update_chunk()` 方法（受锁保护）
- 使用 snapshot 模式返回数据（避免外部修改内部状态）
- `is_cancelled()` / `is_paused()` 用于 worker 检查是否应该停止
- 可选持久化：Job 快照写入 `output/.state/jobs/{job_id}.json`，用于重启恢复（best-effort）
- 持久化节流：后台线程合并写入（默认 5s 一次，可用 `NOVEL_PROOFER_JOB_PERSIST_INTERVAL_S` 覆盖），避免高并发 chunk 更新导致频繁磁盘 IO
- 关键状态：`done/error/cancelled` 终态会触发立即落盘（降低状态回退）
- 重启自愈：将无意义的“运行中”状态收敛为可继续的 `paused/pending`

---

## 7. Runner 编排

文件：`novel_proofer/runner.py`

### 7.1 主流程

```python
def run_job(job_id, input_path, fmt, llm):
    # [1] 分片（文件流式）：先统计分片数，再初始化 chunk 列表
    total = sum(1 for _ in iter_chunks_by_lines_with_first_chunk_max_from_file(input_path, ...))
    GLOBAL_JOBS.init_chunks(job_id, total_chunks=total)

    # [2] 本地规则预处理
    for i, c in enumerate(iter_chunks_by_lines_with_first_chunk_max_from_file(input_path, ...)):
        fixed, stats = apply_rules(c, fmt)
        _atomic_write_text(work_dir / "pre" / f"{i:06d}.txt", fixed)

    # [3] LLM 并发处理
    outcome = _run_llm_for_indices(job_id, list(range(total)), work_dir, llm)

    # [4] 本地规则二次收敛
    for cs in chunk_statuses:
        if cs.state == "done":
            chunk_out = read(work_dir / "out" / f"{cs.index:06d}.txt")
            fixed, stats = apply_rules(chunk_out, fmt)
            write(work_dir / "out" / f"{cs.index:06d}.txt", fixed)

    # [5] 合并输出
    _finalize_job(job_id, work_dir, out_path, total, error_msg)
```

### 7.2 并发控制

```python
def _run_llm_for_indices(job_id, indices, work_dir, llm):
    max_workers = llm.max_concurrency  # 默认 20

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        pending_indices = list(indices)
        in_flight: dict[Future, int] = {}

        while pending_indices or in_flight:
            # 检查终止/暂停
            if GLOBAL_JOBS.is_cancelled(job_id):
                break
            if GLOBAL_JOBS.is_paused(job_id) and not in_flight:
                break

            # 逐步提交任务（避免一次性提交导致无法及时停止）
            while pending_indices and len(in_flight) < max_workers:
                i = pending_indices.pop(0)
                fut = ex.submit(_llm_worker, job_id, i, work_dir, llm)
                in_flight[fut] = i

            # 等待任务完成
            done, _ = wait(in_flight.keys(), timeout=0.1, return_when=FIRST_COMPLETED)
            for f in done:
                in_flight.pop(f)
```

**设计要点**：
- 逐步提交任务，而非一次性全部提交
- 每个 worker 检查 `is_cancelled()` / `is_paused()` 标志
- 暂停时保留 in-flight 任务完成，新任务不提交

### 7.3 输出验证与合并

**尾部换行对齐**：

```python
def _align_trailing_newlines(reference, text, *, max_newlines=3):
    # 对齐 LLM 输出的尾部换行符与输入一致
    # 防止分片边界处的段落分离不稳定
    want = min(_count_trailing_newlines(reference), max_newlines)
    have = _count_trailing_newlines(text)
    if have == want:
        return text
    return text.rstrip("\n") + ("\n" * want)
```

**合并逻辑**：

```python
def _merge_chunk_outputs(work_dir, total_chunks, out_path):
    def _iter_chunks():
        for i in range(total_chunks):
            chunk_text = read(work_dir / "out" / f"{i:06d}.txt")
            yield (chunk_text, i == total_chunks - 1)

    # 合并逻辑抽到 formatting/merge.py，runner 与 fixer 复用同一套规则
    merge_text_chunks_to_path(_iter_chunks(), out_path)
```

**Why 需要补空行**：LLM 可能在分片边界处丢失段落分隔，合并时需要恢复。

---

## 8. 关键设计决策汇总

| 决策 | Why |
|------|-----|
| **本地规则 + LLM 混合架构** | 本地规则快速确定性，LLM 处理语义任务 |
| **规则执行顺序固定** | 后续规则依赖前面规则的结果 |
| **缩进最后执行** | 避免破坏前面规则对行结构的检测 |
| **分块优先段落边界** | 保持段落完整性，减少 LLM 上下文混乱 |
| **首分片更大预算** | 给 LLM 更多上下文来清理前置信息 |
| **首分片允许更短输出** | 删除广告/水印后输出变短是正常的 |
| **引号转换默认关闭** | 风险高（奇数引号、代码中的引号） |
| **本地规则应用两次** | 确保最终输出格式一致，即使 LLM 破坏了格式 |
| **逐步提交并发任务** | 支持终止/暂停，避免资源浪费 |
| **Think 标签过滤容错** | 过度过滤时回退为保留内容 |
| **合并时补空行** | 恢复分片边界处丢失的段落分隔 |
| **后台任务使用受控线程池** | 避免阻塞 FastAPI 事件循环，且避免 job 级并发无限制增长 |
| **输入缓存落盘 + 文件流式分片** | 内存占用更稳定；支持 rerun-all 无需重新上传 |
| **Job 状态快照持久化** | 本地单机无需引入 DB，也能在重启后继续/重试 |
| **文件日志滚动** | 替代 `print()`，便于定位 LLM/IO/状态问题 |

---

## 附录：配置参数速查

### FormatConfig

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_chunk_chars` | 2000 | 分片大小上限（200-4000） |
| `paragraph_indent` | True | 启用段落缩进 |
| `indent_with_fullwidth_space` | True | 使用全角空格缩进 |
| `normalize_blank_lines` | True | 合并多个空行 |
| `trim_trailing_spaces` | True | 移除行尾空白 |
| `normalize_ellipsis` | True | 统一省略号 |
| `normalize_em_dash` | True | 统一破折号 |
| `normalize_cjk_punctuation` | True | ASCII 标点转全角 |
| `fix_cjk_punct_spacing` | True | 移除 CJK 与标点间的空格 |
| `normalize_quotes` | **False** | 直引号转弯引号 |

### LLMConfig

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `base_url` | "" | OpenAI 兼容 API 端点 |
| `api_key` | "" | 认证令牌 |
| `model` | "" | 模型名称 |
| `temperature` | 0.0 | 确定性输出 |
| `timeout_seconds` | 180.0 | 单个请求超时 |
| `max_concurrency` | 20 | 并发分片数 |
| `extra_params` | None | 额外的 OpenAI 参数透传（例如 thinking config 等） |
