#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[novel-proofer] Working dir: $(pwd)"

MODE="serve"
if [[ "${1:-}" == "--smoke" ]]; then
  MODE="smoke"
fi

VENV_DIR=".venv"
PY_LINUX="$VENV_DIR/bin/python"

PY_CMD=()

# If a Windows venv was copied into WSL, it will not be executable.
if [[ -d "$VENV_DIR" && ! -x "$PY_LINUX" && -f "$VENV_DIR/Scripts/python.exe" ]]; then
  BACKUP_DIR="${VENV_DIR}.win"
  if [[ -e "$BACKUP_DIR" ]]; then
    BACKUP_DIR="${VENV_DIR}.win.$(date +%Y%m%d%H%M%S)"
  fi
  echo "[novel-proofer] Detected Windows venv in $VENV_DIR, moving to $BACKUP_DIR ..."
  mv "$VENV_DIR" "$BACKUP_DIR"
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "[novel-proofer] uv is required but was not found."
  echo "[novel-proofer] Install uv first: https://docs.astral.sh/uv/getting-started/installation/"
  exit 1
fi

echo "[novel-proofer] Using: $(uv --version 2>&1)"

SYNC_ARGS=(sync --frozen --no-install-project)
if [[ "$MODE" == "serve" ]]; then
  SYNC_ARGS+=(--no-dev)
else
  SYNC_ARGS+=(--group dev)
fi

uv "${SYNC_ARGS[@]}"

PY_CMD=(uv run --frozen --no-sync python)
echo "[novel-proofer] Using: $("${PY_CMD[@]}" --version 2>&1)"

if [[ "$MODE" == "smoke" ]]; then
  echo "[novel-proofer] Running tests..."
  uv run --frozen --no-sync pytest -q
  echo "[novel-proofer] Tests OK."
  exit 0
fi

HOST="${NP_HOST:-127.0.0.1}"
PORT="${NP_PORT:-18080}"

is_port_free() {
  local candidate="$1"
  "${PY_CMD[@]}" - "$candidate" "$HOST" <<PY
import socket
import sys

port = int(sys.argv[1])
host = sys.argv[2]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind((host, port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

pick_port() {
  local start_port="$1"
  local end_port=$((start_port + 30))
  local p
  for ((p=start_port; p<=end_port; p++)); do
    if is_port_free "$p"; then
      PORT="$p"
      return 0
    fi
  done
  echo "[novel-proofer] No free port found in range ${start_port}..${end_port}."
  return 1
}

pick_port "$PORT"

echo "[novel-proofer] Starting server..."
echo "[novel-proofer] URL: http://${HOST}:${PORT}/"
exec "${PY_CMD[@]}" -m novel_proofer.server --host "$HOST" --port "$PORT"
