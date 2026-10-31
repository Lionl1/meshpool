#!/usr/bin/env bash
# Local pool: one coordinator and AGENTS agents with SLOTS slots each, then the bench client.
#   benchmarks/run_meshpool.sh [bench args...]       e.g. --kind primes --tasks 32 --n 3000000
#   KILL_AFTER=1 benchmarks/run_meshpool.sh ...      SIGKILL one agent after 1 s
#   RTT_MS=60 benchmarks/run_meshpool.sh ...         agents and client reach the coordinator
#                                                    through a proxy adding 60 ms per round trip
set -euo pipefail
cd "$(dirname "$0")/.."
AGENTS=${AGENTS:-4}
SLOTS=${SLOTS:-1}
PORT=${PORT:-7711}
BIN=target/release/meshpool
cargo build --release --quiet --bins --examples
PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT
# Short timeouts so a dead device is noticed quickly (defaults: 30 s / 10 s heartbeat).
# All agents share 127.0.0.1, so the one-replica-per-address rule is off.
$BIN coordinator --listen 127.0.0.1:$PORT --agent-timeout 2 --allow-same-ip 2>/dev/null &
PIDS+=($!)
URL=http://127.0.0.1:$PORT
if [ -n "${RTT_MS:-}" ]; then
  python3 benchmarks/delay_proxy.py --listen $((PORT + 1)) --target $PORT --rtt-ms "$RTT_MS" &
  PIDS+=($!)
  URL=http://127.0.0.1:$((PORT + 1))
fi
sleep 0.5
VICTIM=
for i in $(seq 1 "$AGENTS"); do
  $BIN --url $URL agent --name w$i --heartbeat 0.5 --slots "$SLOTS" 2>/dev/null &
  PIDS+=($!)
  if [ -z "$VICTIM" ]; then VICTIM=$!; fi
done
sleep 0.5
if [ -n "${KILL_AFTER:-}" ]; then
  (sleep "$KILL_AFTER"; kill -9 "$VICTIM"; echo "[x] killed agent pid $VICTIM at ${KILL_AFTER}s" >&2) &
fi
target/release/examples/bench --url $URL "$@"
