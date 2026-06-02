#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${HERMES_OPENWEBUI_BRIDGE_PORT:-8650}"
LOG_FILE="${HERMES_OPENWEBUI_BRIDGE_LOG:-/root/.hermes/openwebui-bridge.log}"
PYTHON_BIN="${HERMES_PYTHON_BIN:-/root/.hermes/hermes-agent/venv/bin/python3}"
BRIDGE_SCRIPT="${HERMES_OPENWEBUI_BRIDGE_SCRIPT:-${SCRIPT_DIR}/hermes-openwebui-bridge.py}"
LOCAL_ONLY_KEY="${HERMES_OPENWEBUI_DEFAULT_KEY:-hermes-openwebui-local-only}"

if ss -ltn "( sport = :${PORT} )" | grep -q "${PORT}"; then
  if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    exit 0
  fi
fi

mkdir -p "$(dirname "${LOG_FILE}")"

export HERMES_REPO="${HERMES_REPO:-/root/.hermes/hermes-agent}"
export HERMES_API_BASE="${HERMES_API_BASE:-http://127.0.0.1:8642}"
export HERMES_OPENWEBUI_BRIDGE_HOST="${HERMES_OPENWEBUI_BRIDGE_HOST:-127.0.0.1}"
export HERMES_OPENWEBUI_BRIDGE_PORT="${PORT}"
export HERMES_OPENWEBUI_BRIDGE_KEY="${HERMES_OPENWEBUI_BRIDGE_KEY:-${HERMES_API_KEY:-${LOCAL_ONLY_KEY}}}"
export HERMES_API_KEY="${HERMES_API_KEY:-${HERMES_OPENWEBUI_BRIDGE_KEY}}"

setsid "${PYTHON_BIN}" "${BRIDGE_SCRIPT}" >"${LOG_FILE}" 2>&1 < /dev/null &

for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "Hermes OpenWebUI bridge failed to bind port ${PORT}; see ${LOG_FILE}" >&2
exit 1
