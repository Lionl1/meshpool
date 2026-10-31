#!/usr/bin/env bash
# Local pool: one coordinator and WORKERS agents, then the bench client.
#   benchmarks/run_meshpool.sh [bench args...]       e.g. --kind primes --tasks 32 --n 3000000
#   KILL_AFTER=1 benchmarks/run_meshpool.sh ...      SIGKILL one agent after 1 s
set -euo pipefail
cd "$(dirname "$0")/.."
WORKERS=${WORKERS:-4}
PORT=${PORT:-7711}
BIN=target/release/meshpool
URL=http://127.0.0.1:$PORT
cargo build --release --quiet --bins --examples
PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT
# Short timeouts so a dead device is noticed quickly (defaults: 30 s / 10 s heartbeat).
$BIN --url $URL coordinator --listen 127.0.0.1:$PORT --agent-timeout 2 2>/dev/null &
PIDS+=($!)
sleep 0.5
for i in $(seq 1 "$WORKERS"); do
  $BIN --url $URL agent --name w$i --heartbeat 0.5 2>/dev/null &
  PIDS+=($!)
  [ "$i" = 1 ] && VICTIM=$!
done
sleep 0.5
if [ -n "${KILL_AFTER:-}" ]; then
  (sleep "$KILL_AFTER"; kill -9 "$VICTIM"; echo "[x] killed agent pid $VICTIM at ${KILL_AFTER}s" >&2) &
fi
target/release/examples/bench --url $URL "$@"
