#!/usr/bin/env bash
# All scenarios on meshpool, Ray and Dask, REPEAT times each. Output: "scenario<TAB>json" lines.
set -uo pipefail
cd "$(dirname "$0")/.."
REPEAT=${REPEAT:-3}
export RAY_ENABLE_UV_RUN_RUNTIME_ENV=0  # do not ship this repo to Ray workers
PY=(uv run -q --python 3.12 --with ray --with 'dask[distributed]' --with wasmtime --with psutil benchmarks/compare.py)
PRIMES=(--kind primes --n 3000000)

emit() { local name=$1; shift; "$@" 2>&1 | grep '^{' | sed "s/^/$name\t/"; }
others() { local name=$1; shift; for sys in ray dask; do emit "$name" "${PY[@]}" --system $sys "$@"; done; }
mesh() { emit "$@"; }

for _ in $(seq "$REPEAT"); do
  # 4 workers on each system
  others noop --kind noop --tasks 1000 --workers 4
  mesh noop benchmarks/run_meshpool.sh --kind noop --tasks 1000
  others primes "${PRIMES[@]}" --tasks 32 --workers 4
  mesh primes benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 32
  others kill "${PRIMES[@]}" --tasks 32 --workers 4 --kill-after 1
  KILL_AFTER=1 mesh kill benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 32
  others data --kind data --tasks 32 --mb 4 --workers 4
  mesh data benchmarks/run_meshpool.sh --kind data --tasks 32 --mb 4
  # The whole machine (8 cores): one meshpool device with 8 slots
  others full "${PRIMES[@]}" --tasks 64 --workers 8
  AGENTS=1 SLOTS=8 mesh full benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 64
  # meshpool only: scaling, verification, network latency
  for s in 1 2 4 8; do AGENTS=1 SLOTS=$s mesh "slots$s" benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 64; done
  for r in 1 2 3; do AGENTS=4 SLOTS=2 mesh "replicas$r" benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 32 --replicas $r; done
  for rtt in 0 20 60; do
    RTT_MS=$rtt mesh "rtt${rtt}_noop" benchmarks/run_meshpool.sh --kind noop --tasks 1000
    RTT_MS=$rtt mesh "rtt${rtt}_primes" benchmarks/run_meshpool.sh "${PRIMES[@]}" --tasks 32
  done
done
