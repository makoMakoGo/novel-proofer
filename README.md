
<div align="center">

![header](https://capsule-render.vercel.app/api?type=waving&color=0:3498db,100:2c3e50&height=200&section=header&text=Novel%20Proofer&fontSize=50&fontColor=ffffff&fontAlignY=35&desc=小说打样员&descSize=20&descAlignY=55)

[![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)](https://python.org)
[![Server](https://img.shields.io/badge/Server-FastAPI%2FUvicorn-009688)](requirements.txt)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

</div>

## 功能概述

| 功能     | 说明                                         |
| -------- | -------------------------------------------- |
| 本地排版 | 修正缩进、空行、标点符号                     |
| LLM 辅助 | 接入 OpenAI-compatible 处理复杂标点          |
| 并发处理 | 大文件分片多线程处理                         |
| 失败重试 | 分片失败后可修改配置并重试，成功后再合并输出 |

校对前后对比：

<div align="center">
<img src="./images/校正前后对比.png" width="70%" alt="校正前后对比">
</div>

## 快速启动

```bash
# 一键启动
start.bat

# 或手动执行
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m novel_proofer.server
```

启动后访问 http://127.0.0.1:18080

## 处理流程

```mermaid
flowchart TD
    A[上传 .txt] --> B[分片<br/>按行累积；优先空行边界]
    B --> C[本地规则预处理<br/>换行/空格/标点/缩进]
    C --> E[LLM 处理（每分片）<br/>流式/重试/校验]
    E --> H{全部分片成功?}
    H -->|是| P[本地规则二次收敛<br/>标题/缩进/空行]
    P --> F[合并输出<br/>补齐段落空行]
    F --> G[输出到 output/]
    H -->|否| M[标记失败分片]
    M --> R[修改配置后<br/>重试失败分片（仅失败部分）]
    R --> E
```

## 文档

| 文档 | 说明 |
|------|------|
| [使用指南](docs/USAGE.md) | 安装配置、规则说明、异常处理、调试方法 |
| [技术架构](docs/ARCHITECTURE.md) | 系统设计原理（Why + How） |
| [测试用例](docs/TESTCASES.md) | 测试覆盖说明 |

## 已知问题

- 偶尔出现乱码，经检查原文件并无乱码

## 待办

- [ ] 识别原文件不同编码格式并最终统一为 UTF-8
- [ ] 补充 LLM 边缘情况的单元测试
