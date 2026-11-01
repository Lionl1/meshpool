# Changelog

## Unreleased

- Graceful shutdown: on Ctrl-C or SIGTERM the coordinator stops accepting, lets requests
  finish (open long polls are cut after `SHUTDOWN_GRACE`, 2 s) and saves its state one
  last time. Library: `coordinator::serve_until(listener, cfg, tls, shutdown)`.
- Persistence: `meshpool coordinator --data-dir` saves the pool (devices, credits, strikes,
  jobs, tasks, queue) to `state.json` and blobs to `blobs/<sha256>` every second, written
  atomically, blobs before the state referring to them; reloaded on start. Leases survive a
  restart. Background tasks stop with the server.
- Rust task SDK (`sdk/rust`, crate `meshpool-task`): `task!` turns `fn(&[u8]) -> impl
  AsRef<[u8]>` into a task, `task_json!` does the same for serde types. Examples: primes,
  word count, Mandelbrot strips; tests build them to wasm32 and run them in a pool.
- Python client (`sdk/python`, standard library only): `Pool.map` / `map_json` run a task
  over many inputs and return the verified outputs in order; `JobFailed` when a job has no
  agreed output. Tests run it against a real coordinator and agents.
- HTTPS: `meshpool coordinator --tls-cert --tls-key` (rustls); clients and agents trust an
  extra CA with `--ca-cert` / `MESHPOOL_CA_CERT`. The coordinator warns when it serves plain
  HTTP on a non-loopback address. `coordinator::serve` takes an `Option<Tls>`.

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
