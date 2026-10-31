# Changelog

## Unreleased

- Python client (`sdk/python`, standard library only): `Pool.map` / `map_json` run a task
  over many inputs and return the verified outputs in order; `JobFailed` when a job has no
  agreed output. Tests run it against a real coordinator and agents.

## 0.2.0

Fixes from the first review:
- Long tasks: heartbeats renew the leases of running tasks, so a task may run longer than
  the lease timeout (60 s by default now, was a hard 300 s).
- Small pools no longer stall: a device that gives a task back (or loses its lease) may
  take it again.
- Sandbox: tables are capped (1M elements, 16 tables); modules are checked against the ABI
  at compile time; device failures (allocation, fetch) are given back instead of voted on,
  so honest devices are not struck for them.
- Agreed failures carry and are credited with the fuel they burned.
- Strikes count per IP address and replicas go to distinct addresses (`--allow-same-ip`
  to turn off), so re-registering does not shed a ban.
- Only clients upload blobs; a device uploads just the output of a task it holds. Blobs
  are hashed outside the coordinator lock.
- Decided jobs, their tasks, unreferenced blobs and long-gone devices are pruned.
- A replica given back after its job is decided is not run again.
- The CLI no longer spawns `hostname` for every command.

Faster:
- Agents run one task per core (`--slots`), lease as many tasks as they have free slots in
  one request, report the output together with the vote, and overlap reports with the next
  task.
- Batch submission (`POST /v1/jobs/batch`) and per-job wake-ups for long-polling clients.
- Agents cache small inputs.
- Clients retry requests that are safe to repeat when the connection drops (a retried
  report that already arrived counts as done).

Measured (docs/benchmarks.md): 1000 tiny tasks 3 075 → 9 282 tasks/s (Ray: 6 572); at
60 ms RTT 20 → 59 tasks/s with 4 slots, 218 with 32; no tasks lost to dropped connections.

Protocol: `meshpool/0.2` (lease returns a list; heartbeats carry running tasks; outputs are
reported with `POST .../tasks/{task}/output?fuel_used=N`).

## 0.1.0

First version: coordinator, agent, WASM sandbox, replicas and quorum, strikes, credits,
CLI, benchmarks against Ray and Dask.
