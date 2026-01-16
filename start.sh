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

if [[ -d "$VENV_DIR" && ! -x "$PY_LINUX" && -f "$VENV_DIR/Scripts/python.exe" ]]; then
  BACKUP_DIR="${VENV_DIR}.win"
  if [[ -e "$BACKUP_DIR" ]]; then
    BACKUP_DIR="${VENV_DIR}.win.$(date +%Y%m%d%H%M%S)"
  fi
  echo "[novel-proofer] Detected Windows venv in $VENV_DIR, moving to $BACKUP_DIR ..."
  mv "$VENV_DIR" "$BACKUP_DIR"
fi

NEED_VENV_CREATE=0

if [[ ! -x "$PY_LINUX" ]]; then
  NEED_VENV_CREATE=1
elif [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  NEED_VENV_CREATE=1
elif ! "$PY_LINUX" -m pip --version >/dev/null 2>&1; then
  NEED_VENV_CREATE=1
fi

if [[ "$NEED_VENV_CREATE" == "1" ]]; then
  rm -rf "$VENV_DIR"
  BASE_PYTHON=""
  if command -v python3 >/dev/null 2>&1; then
    BASE_PYTHON="python3"
  elif command -v python >/dev/null 2>&1; then
    BASE_PYTHON="python"
  else
    echo "[novel-proofer] Python not found. Please install Python 3.10+."
    exit 1
  fi

  echo "[novel-proofer] .venv not found, creating..."
  "$BASE_PYTHON" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

PY="$PY_LINUX"
echo "[novel-proofer] Using: $("$PY" --version 2>&1)"

if [[ -f "requirements.txt" ]]; then
  echo "[novel-proofer] Installing dependencies from requirements.txt..."
  "$PY" -m pip --disable-pip-version-check install -r requirements.txt
fi

if [[ "$MODE" == "smoke" ]]; then
  echo "[novel-proofer] Running tests..."
  if [[ -f "requirements-dev.txt" ]]; then
    "$PY" -m pip --disable-pip-version-check install -r requirements-dev.txt
  fi
  "$PY" -m pytest -q
  echo "[novel-proofer] Tests OK."
  exit 0
fi

HOST="${NP_HOST:-127.0.0.1}"
PORT="${NP_PORT:-18080}"

is_port_free() {
  local candidate="$1"
  "$PY" - "$candidate" "$HOST" <<'PY'
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
exec "$PY" -m novel_proofer.server --host "$HOST" --port "$PORT"
