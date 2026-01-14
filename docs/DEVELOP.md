# 开发协作指南 / Development Guide

本文档面向协作开发（多人、多机器、多 PR）的场景，约定提交规范、分支流程、测试方式与安全注意事项。

## 目录

1. [环境与启动](#1-环境与启动)
2. [Git 协作流程](#2-git-协作流程)
3. [提交信息规范（Conventional Commits）](#3-提交信息规范conventional-commits)
4. [启用 Git hooks 与提交模板（推荐）](#4-启用-git-hooks-与提交模板推荐)
5. [测试与调试](#5-测试与调试)
6. [安全与密钥](#6-安全与密钥)

---

## 1. 环境与启动

### 1.1 环境要求

- Python 3.10+
- Windows 为主要支持平台（仓库提供 `start.bat` 一键启动）

### 1.2 启动服务

推荐直接运行：

```bash
.\start.bat
```

等价的手动方式：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m novel_proofer.server
```

---

## 2. Git 协作流程

建议流程（更稳、更少互相打断）：

1. 从 `main` 拉取最新：`git fetch origin`
2. 新功能/修复从 `main` 开分支（示例）：`feat/ui-drop-upload`、`fix/llm-timeout`
3. 保持分支小步提交，提交信息遵循 Conventional Commits（见下）
4. 提交 PR 前自测（至少跑一次 `pytest -q`）
5. 合并前尽量保持线性历史（按团队偏好：rebase 或 squash）

注：如果需要重写已推送历史（如 reword/rebase），请优先使用 `--force-with-lease`，并确保相关分支无人依赖。

---

## 3. 提交信息规范（Conventional Commits）

仓库约定使用小写 Conventional Commits：

```text
type(scope): subject
```

- `type`：`feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert`
- `scope`：可选，建议使用小写模块名；支持多个 scope（逗号分隔），如 `refactor(llm,ui,test): ...`
- `subject`：简短描述（不要 Title Case 前缀如 `UI:`/`Brand:`/`Prompt:`）

示例：

- `feat(ui): add drag-and-drop upload`
- `fix(llm): handle empty chunks`
- `refactor(llm,ui,test): align separator cleanup and ui defaults`

---

## 4. 启用 Git hooks 与提交模板（推荐）

仓库内置了可选的 Git hooks 与提交模板：

- `commit-msg`：校验提交信息（Conventional Commits）
- `pre-commit`：提交前自动执行 `ruff format` 与 `ruff check --fix`（统一代码风格）
- `commit.template`：减少提交信息格式错误

它们通过 **本地 git config** 生效，因此**每台机器都需要执行一次**。

### 4.1 Windows

```bash
.\tools\setup-git.ps1
```

### 4.2 macOS / Linux

```bash
bash tools/setup-git.sh
```

执行后会写入：

- `core.hooksPath = .githooks`
- `commit.template = .gitmessage`

如果你的提交被拒绝，请按提示修改为 `type(scope): subject` 格式后重试。

> [!NOTE]
> `pre-commit` hook 依赖仓库内的 `.venv`。首次运行请先执行一次 `.\start.bat` 创建虚拟环境并安装依赖。
> 若 hook 提示它自动修复了格式/导入顺序，请 `git add` 后重新提交即可。

---

## 5. 测试与调试

### 5.1 跑测试

在已激活虚拟环境的前提下：

```bash
pytest -q
```

也可一键跑 smoke：

```bash
.\start.bat --smoke
```

### 5.2 LLM 集成测试（可选）

若设置 `NOVEL_PROOFER_RUN_LLM_TESTS=true`（或传入 `--run-llm-tests`），会运行标记为 `llm_integration` 的真实 LLM 集成测试。

注意：这类测试需要有效的 LLM 配置与网络，可能产生费用/速率限制；不建议在默认 CI 或未配置 key 的环境里启用。

---

## 6. 安全与密钥

- `.env` 可能包含 API Key，已在 `.gitignore` 中忽略；请勿提交任何包含密钥的文件。
- 若需要共享示例配置，请更新 `.env.example` / `.env.test.example`，不要直接提交真实 key。
