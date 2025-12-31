<div align="center">

# 小说排版校对器

**Novel Proofer**

中文网络小说 `.txt` 文件的排版与标点统一工具

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

<img src="./images/UI-01.png" width="80%" alt="UI 展示">

</div>

---

## 效果对比

<div align="center">
<img src="./images/矫正前后对比.png" width="90%" alt="矫正前后对比">
</div>

---

## 功能

| 功能     | 说明                                |
| -------- | ----------------------------------- |
| 本地排版 | 修正缩进、空行、标点符号            |
| LLM 辅助 | 可选接入 OpenAI/Gemini 处理复杂标点 |
| 并发处理 | 大文件分片多线程处理                |
| 失败重试 | LLM 分片失败后可修改配置并重试失败分片，全部成功后再合并输出 |
| 本地运行 | 默认无外部数据传输                  |

> 原则：只做排版，不改内容

### 处理流程

```mermaid
flowchart TD
    A[上传 .txt 文件] --> B[按行数分片]
    B --> C[本地规则处理]

    subgraph local [本地规则]
        C --> C1[换行符统一]
        C1 --> C2[行尾空格清理]
        C2 --> C3[空行规范化]
        C3 --> C4[省略号/破折号统一]
        C4 --> C5[中文标点转换]
        C5 --> C6[引号规范化]
        C6 --> C7[段落缩进]
    end

    C7 --> D{启用 LLM?}
    D -->|否| F[合并分片]
    D -->|是| E[LLM 并发处理]

    subgraph llm [LLM 处理]
        E --> E1[对话/叙述分段]
        E1 --> E2[场景转换空行]
        E2 --> E3[章节标题格式]
    end

    E3 --> H{全部分片成功?}
    H -->|是| F
    H -->|否| R[修改 LLM 配置]
    R --> S[重试失败分片]
    S --> H
    F --> G[输出到 output/]
```

---

## 快速开始

### Windows

双击 `start.bat`

## 手动运行

```bash
# 创建虚拟环境
python -m venv .venv
.venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 启动
python -m novel_proofer.server
```

访问 http://127.0.0.1:18080

---

## 使用流程

```
上传 .txt 文件 → 自动排版处理

- 全部成功：最终结果输出到 output/
- 部分失败：可修改 LLM 配置后点击“重试失败部分”，全部成功后才会生成最终输出文件
```

---

## 已知问题

- 偶尔莫名出现乱码，经检查原文件并无乱码

---

## 待办

- [ ] 识别原文件不同编码格式并最终统一为 UTF-8

---

## 技术栈

```
Python 3 · http.server · ThreadPoolExecutor
```
