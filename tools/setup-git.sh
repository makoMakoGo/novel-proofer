#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ ! -f ".githooks/commit-msg" ]]; then
  echo "Missing .githooks/commit-msg. Please run this script from a valid repo checkout." >&2
  exit 1
fi

git config core.hooksPath ".githooks"
git config commit.template ".gitmessage"
git config core.filemode true

chmod +x .githooks/commit-msg .githooks/pre-commit tools/setup-git.sh || true

if [[ ! -x ".githooks/commit-msg" || ! -x ".githooks/pre-commit" ]]; then
  echo "Failed to mark git hooks as executable. Please ensure your filesystem supports Unix permissions, then re-run:" >&2
  echo "  bash tools/setup-git.sh" >&2
  exit 1
fi

echo "Configured git:"
echo "  core.hooksPath = .githooks"
echo "  commit.template = .gitmessage"
echo "  core.filemode = true"
