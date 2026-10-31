# Benchmarks: meshpool vs Ray vs Dask

One MacBook (M1, 8 cores), median of 3 runs, every output checked. Reproduce with
`benchmarks/run_all.sh` (needs `uv`; Ray 2.x and Dask distributed go into a throwaway
environment). Raw runs: [benchmarks/results_m1.tsv](../benchmarks/results_m1.tsv).

To compare the systems rather than languages, **all three run the same WebAssembly module**:
meshpool in its sandbox, Ray and Dask through wasmtime-py with the same host ABI. Times are
end to end: uploading inputs, submitting, waiting, fetching and checking every output.

## Head to head

| Scenario | meshpool | Ray | Dask |
|---|---|---|---|
| 1000 tiny tasks, 4 workers, tasks/s (scheduling overhead) | **9 282** | 6 572 | 1 394 |
| 32 CPU tasks (primes < 3M, ~0.29 s each), 4 workers, s (ideal 2.29) | **2.41** | 2.50 | 2.53 |
| same, one worker SIGKILLed at 1 s, s | 3.29 | 3.20 | **3.14** |
| 32 tasks × 4 MiB distinct input → 4 MiB output, 4 workers, s | 0.25 | **0.21** | 0.34 |
| 64 CPU tasks on the whole machine (8 slots / 8 workers), s | **4.02** | 4.17 | 4.23 |

Every run returned every result correctly, including after the kill.

- **Throughput and CPU work: on par or ahead.** On tiny tasks meshpool now beats Ray (batch
  submission, a lease for every free slot in one request, results reported together with
  the vote). On CPU work all three are within 5% of each other and of the ideal.
- **Moving data: Ray is ahead** (shared-memory object store on one machine; meshpool sends
  every input and output over HTTP through the coordinator — the peer-to-peer data plane is
  on the roadmap).
- **Recovery: on par**, for different reasons: Ray and Dask see a local process die at once;
  meshpool waits for a missed heartbeat (2 s here, 30 s by default) — which is what any
  system has to do once devices are on other networks.

## meshpool only

**Scaling on one device** (64 prime tasks, one agent):

| Slots | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| Time, s | 17.96 | 9.29 | 4.82 | 4.08 |
| Speed-up | 1.0× | 1.93× | 3.73× | 4.40× |

Linear up to the 4 performance cores of the M1; slots 5–8 land on efficiency cores.

**Verification** (32 prime tasks, 4 devices × 2 slots):

| Replicas | 1 | 2 | 3 |
|---|---|---|---|
| Time, s | 2.04 | 4.02 | 6.01 |

Checking results of untrusted devices costs exactly R× the compute, and nothing on top.

**Network latency** (agents and client reach the coordinator through a proxy that adds the
round-trip time to every request; 4 devices × 1 slot unless noted):

| RTT | 0 ms | 20 ms | 60 ms | 60 ms, 4 × 8 slots |
|---|---|---|---|---|
| 1000 tiny tasks, tasks/s | 4 890 | 152 | 59 | 218 |
| 32 CPU tasks, s | 2.42 | 2.75 | 3.34 | — |

On a real network a slot costs about one round trip per task (the lease; the report
overlaps the next task), so tiny tasks scale with slots × 1/RTT, and real tasks (≥ 0.3 s)
lose little. Ray and Dask were not measured here: they need every node to reach every other
node directly, which devices behind NAT cannot.

**Sandbox cost** (`cargo run --release --example bench -- --local`): primes below 3M take
0.32 s in native Rust and 0.29 s inside the sandbox with fuel metering.

## Changes since 0.1

| | 0.1 | 0.2 |
|---|---|---|
| 1000 tiny tasks, tasks/s | 3 075 | 9 282 |
| 32 CPU tasks, s | 2.52 | 2.41 |
| 1000 tiny tasks at 60 ms RTT, tasks/s | 20 | 59 (218 with 8 slots per device) |
| Tasks lost to dropped connections at 60 ms RTT | 3–7% | 0 (requests that are safe to repeat are retried) |

## What the numbers do not show

| | meshpool | Ray | Dask | BOINC | Golem / io.net-style markets |
|---|---|---|---|---|---|
| Devices behind NAT, outbound-only | yes | no (nodes must reach each other) | no | yes | partly |
| Runs untrusted code safely | WASM sandbox, fuel, memory and table caps | no (pickled Python, full access) | no | per-project apps, signed | VM/containers |
| Checks results of untrusted devices | replicas on distinct addresses + quorum + strikes | no | no | replication | partly / reputation |
| Any device can join | yes (optional token) | no | no | yes | yes |
| Arbitrary Python libraries | no (WASM only, for now) | yes | yes | no | yes |
| GPU | planned (WebGPU) | yes | yes | yes | yes |

Ray and Dask are the right tools inside one trusted cluster; meshpool targets the case they
leave out: many devices you do not control, connected over the internet.
