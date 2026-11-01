# meshpool architecture

meshpool pools the compute of arbitrary devices — laptops, desktops, servers, home GPUs,
later phones — into one queue of work. Anyone may contribute a device; nobody has to trust
the devices. This document fixes the design; the code follows it phase by phase.

## What it is (and is not)

Devices on the internet are a million times further apart than cores in one machine
(milliseconds and megabits instead of nanoseconds and hundreds of GB/s). So meshpool is a
**throughput** machine: it runs many independent tasks (map, parameter sweeps, rendering,
batch inference, data processing) on whatever devices are online. It does not pretend to be
one shared-memory computer, and tightly coupled jobs (MPI, large-model training) are out of
scope except inside one LAN.

## Trust model

Three parties, none trusting the others more than necessary:

| Threat | Defence |
|---|---|
| A task harms the device that runs it | Tasks are WebAssembly modules run by wasmtime: no files, network, clock or randomness; memory and CPU (fuel) are capped |
| A device returns a wrong result (bug, overclock, cheating) | Every task runs on several devices (replicas); a result is accepted only when a quorum of them agree bit for bit; dissenters get strikes and are banned after 3 |
| A device impersonates another one | Each device gets a secret at registration; every agent call carries it, over HTTPS (`--tls-cert`) outside a trusted LAN |
| A device sheds a ban by registering again | Strikes also count per IP address: once an address has 3, all its devices are banned and it cannot register new ones |
| A device reads the task's data | **Not defended**: contributed devices see their inputs. Confidential workloads need TEEs (later) or trusted pools |
| One person runs many devices (Sybil) to win the quorum | Partly: replicas of a job go to distinct devices *and distinct IP addresses*. Later: reputation weighting, random assignment, owner identity/stake |
| Someone uses the coordinator as free storage | Only clients upload blobs; a device can upload just the output of a task it holds, at most 64 MiB, and unreferenced blobs expire |

The per-address rules (`distinct_ips`, on by default) assume devices reach the coordinator
directly. For a pool on one machine or behind one NAT address, start the coordinator with
`--allow-same-ip`.

Bitwise agreement needs determinism, so the sandbox is configured for it: NaNs are
canonicalised, relaxed SIMD and threads are off, and all limits (fuel, memory, tables) come
from the job or are fixed, never from the device. Even failures are voted on: "ran out of
fuel" or "trapped" is a result the replicas must agree on. Fuel used is part of every vote,
which makes it a verifiable cost unit for credits. Problems of the device itself (it cannot
fetch a blob or allocate an instance) are not votes: the task goes back to the queue and
nobody is blamed.

## Components

```
 client (CLI / SDK) ──HTTP──▶ coordinator ◀──HTTP (outbound only)── agents on devices
                               │ jobs, tasks, votes, blobs, credits
```

- **Coordinator** (`src/coordinator.rs`): job queue, task leases, quorum, strikes and
  credits, content-addressed blob store (SHA-256). One process; with `--data-dir` its state (`state.json`) and blobs (`blobs/<sha256>`)
  are saved every second and reloaded on start (`src/store.rs`). Leases survive a restart:
  devices that keep running their tasks report to the restarted coordinator.
- **Agent** (`src/agent.rs`): registers the device and its capabilities, runs `slots`
  tasks at once (one per core by default), leases as many tasks as it has free slots in one
  long-poll, fetches module and input by hash (verified; modules and small inputs cached),
  runs them in the sandbox and reports the output in the same request as the vote. A slot is
  freed before the report goes out, so on a slow network the next task overlaps it.
  Heartbeats renew the leases of running tasks. Agents only make outbound requests, so
  they work behind any NAT or firewall.
- **Sandbox** (`src/sandbox.rs`): wasmtime with fuel, memory limits and a three-function
  host ABI (`env.input_len`, `env.read_input`, `env.write_output`; the module exports
  `memory` and `run`).
- **Client** (`src/client.rs`, `meshpool submit|job|agents`): uploads blobs, submits jobs,
  waits for results.

## Task lifecycle

1. The client uploads the module and the input (blobs) and submits a job: module hash,
   input hash, fuel, memory, replicas *R*.
2. The coordinator creates *R* tasks. An agent leasing work gets a task of a job it has not
   touched yet, so the replicas of a job land on distinct devices.
3. Each agent reports an outcome: `ok` (output hash + fuel used), `failed` (deterministic
   kind) or `abandoned` (the device could not run it — the task is re-queued, no vote).
4. As soon as ⌊R/2⌋+1 votes agree, the job is done; agreeing devices earn credits (fuel),
   dissenting ones get a strike. If all replicas have voted without a quorum, one more
   replica is added (up to R+2), then the job fails with "no consensus".
5. A lease lives as long as its device's heartbeats renew it, so tasks may run for hours.
   If the device goes silent, or keeps beating without renewing the lease (it hangs), the
   task is re-queued; the same device may take it again, so small pools do not stall.
6. Decided jobs are kept for an hour, then forgotten with their tasks; blobs no job refers
   to expire after an hour; devices silent for a day are forgotten (their address keeps
   its strikes).

Clients submit many jobs in one request (`POST /v1/jobs/batch`) and long-poll each job;
only that job's decision wakes the request.

## Roadmap

1. **Core (this phase):** coordinator, agent, WASM CPU sandbox, replicas and quorum, strikes,
   credits, tokens, CLI, tests with cheating and vanishing devices, benchmarks against
   existing systems.
2. **GPU for untrusted devices:** a WebGPU host API (wgpu → Metal / Vulkan / DX12) so a WASM
   task can dispatch WGSL compute shaders. Shaders are validated (naga) and can touch only
   their own buffers. GPU floats are not bitwise reproducible across vendors, so GPU jobs vote
   with a tolerance or on integer results.
3. **Trusted pools:** native containers / CUDA on devices whose owner opts in, for workloads
   WASM cannot express (PyTorch, distllm stages).
4. **Data plane:** large blobs peer-to-peer (QUIC, NAT hole punching) with the coordinator
   as fallback relay; locality-aware scheduling.
5. **Scale and economy:** several coordinators, reputation-weighted and
   randomised assignment, credit accounting between owners.
6. **SDKs and devices:** Python and Rust SDKs (`map`, DAGs), agents for Windows services,
   Android/iOS (the WASM tasks run unchanged).
