#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="/root/.hermes/gateway-manual.log"
HERMES_BIN="/root/.local/bin/hermes"

if ss -ltn '( sport = :8642 )' | grep -q 8642; then
  exit 0
fi

mkdir -p /root/.hermes

setsid "$HERMES_BIN" gateway run --accept-hooks >"$LOG_FILE" 2>&1 < /dev/null &

for _ in $(seq 1 180); do
  if ss -ltn '( sport = :8642 )' | grep -q 8642; then
    exit 0
  fi
  sleep 1
done

echo "Hermes gateway failed to bind port 8642" >&2
exit 1
