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

echo "Configured git:"
echo "  core.hooksPath = .githooks"
echo "  commit.template = .gitmessage"

