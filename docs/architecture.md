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
| A device impersonates another one | Each device gets a secret at registration; every agent call carries it |
| A device reads the task's data | **Not defended**: contributed devices see their inputs. Confidential workloads need TEEs (later) or trusted pools |
| One person runs many devices (Sybil) to win the quorum | Partly: replicas go to distinct devices only. Later: reputation weighting, random assignment, owner identity/stake |

Bitwise agreement needs determinism, so the sandbox is configured for it: NaNs are
canonicalised, relaxed SIMD and threads are off, and all limits (fuel, memory) come from the
job, not from the device. Even failures are voted on: "ran out of fuel" or "trapped" is a
result the replicas must agree on. Fuel used is part of the vote too, which makes it a
verifiable cost unit for credits.

## Components

```
 client (CLI / SDK) ──HTTP──▶ coordinator ◀──HTTP (outbound only)── agents on devices
                               │ jobs, tasks, votes, blobs, credits
```

- **Coordinator** (`src/coordinator.rs`): job queue, task leases, quorum, strikes and
  credits, content-addressed blob store (SHA-256). One process with state in memory for now.
- **Agent** (`src/agent.rs`): registers the device and its capabilities, heartbeats,
  long-polls for a lease, fetches module and input by hash (verified, modules cached),
  runs them in the sandbox, uploads the output and reports the outcome. Agents only make
  outbound requests, so they work behind any NAT or firewall.
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
5. A lease that is not answered in time, or whose device stops heartbeating, is re-queued.

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
5. **Scale and economy:** persistent state, several coordinators, reputation-weighted and
   randomised assignment, credit accounting between owners.
6. **SDKs and devices:** Python and Rust SDKs (`map`, DAGs), agents for Windows services,
   Android/iOS (the WASM tasks run unchanged).
