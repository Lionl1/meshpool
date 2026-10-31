#!/usr/bin/env bash
# Every workload on meshpool, Ray and Dask, REPEAT times each (results: JSON lines).
set -uo pipefail
cd "$(dirname "$0")/.."
REPEAT=${REPEAT:-3}
export RAY_ENABLE_UV_RUN_RUNTIME_ENV=0  # do not ship this repo to workers
PY=(uv run -q --python 3.12 --with ray --with 'dask[distributed]' --with wasmtime --with psutil benchmarks/compare.py)
for _ in $(seq "$REPEAT"); do
  for sys in ray dask; do
    "${PY[@]}" --system $sys --kind noop --tasks 1000 2>/dev/null | grep '^{'
    "${PY[@]}" --system $sys --kind primes --tasks 32 --n 3000000 2>/dev/null | grep '^{'
    "${PY[@]}" --system $sys --kind primes --tasks 32 --n 3000000 --kill-after 1 2>&1 | grep -E '^\{|\[x\]'
  done
  benchmarks/run_meshpool.sh --kind noop --tasks 1000
  benchmarks/run_meshpool.sh --kind noop --tasks 1000 --replicas 2
  benchmarks/run_meshpool.sh --kind primes --tasks 32 --n 3000000
  KILL_AFTER=1 benchmarks/run_meshpool.sh --kind primes --tasks 32 --n 3000000 2>&1 | grep -E '^\{|\[x\]'
done
