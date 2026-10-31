# Benchmarks: meshpool vs Ray vs Dask

One MacBook (M1, 8 cores), 4 workers per system, median of 3 runs. Reproduce with
`benchmarks/run_all.sh` (needs `uv`; Ray and Dask are installed into a throwaway environment).
Raw runs: [benchmarks/results_m1.jsonl](../benchmarks/results_m1.jsonl).

To compare the systems rather than languages, **all three run the same WebAssembly module**:
meshpool in its sandbox, Ray and Dask through wasmtime-py with the same host ABI.

| Workload | meshpool | Ray 2.x | Dask distributed |
|---|---|---|---|
| 1000 tiny tasks, tasks/s (scheduling overhead) | 3 075 | 4 766 | 1 242 |
| same, every task on 2 devices + vote | ~2 000 | — | — |
| 32 CPU tasks (primes < 3M, ~0.29 s each), s (ideal 2.29) | 2.52 | 2.62 | 2.62 |
| same, one worker SIGKILLed at 1 s, s | 3.36 | 3.41 | 3.21 |
| results correct after the kill | 32/32 | 32/32 | 32/32 |

Sandbox cost (`cargo run --release --example bench -- --local`): counting primes below 3M
takes 0.32 s in native Rust and 0.29 s inside the sandbox with fuel metering — no overhead.

## How to read it

- **Speed is on par.** On one machine meshpool keeps up with Ray and beats Dask, although
  every task is a separate HTTP job (submit, long-poll, blob fetch) while Ray and Dask use
  in-process fast paths. Ray is still faster on tiny tasks.
- **Recovery is on par**, but for different reasons: Ray and Dask see a local process die
  at once; meshpool waits for a missed heartbeat (2 s here, 30 s by default) — which is what
  any system has to do once devices are on other networks.
- **Verification costs ~35%** on tiny tasks (2 replicas, twice the work and requests); on
  real tasks it costs exactly R× the compute.

## What the numbers do not show

| | meshpool | Ray | Dask | BOINC | Golem / io.net-style markets |
|---|---|---|---|---|---|
| Devices behind NAT, outbound-only | yes | no (nodes must reach each other) | no | yes | partly |
| Runs untrusted code safely | WASM sandbox, fuel, memory caps | no (pickled Python, full access) | no | per-project apps, signed | VM/containers |
| Checks results of untrusted devices | replicas + quorum + strikes | no | no | replication | partly / reputation |
| Any device can join | yes (optional token) | no | no | yes | yes |
| Arbitrary Python libraries | no (WASM only, for now) | yes | yes | no | yes |
| GPU | planned (WebGPU) | yes | yes | yes | yes |

Ray and Dask are the right tools inside one trusted cluster; meshpool targets the case they
leave out: many devices you do not control, connected over the internet.
