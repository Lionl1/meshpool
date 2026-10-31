# meshpool

Pool the compute of many devices — laptops, desktops, servers, home GPUs — into one queue
of work. Anyone can lend a device; nobody has to trust the devices:

- tasks are **WebAssembly** modules run in a sandbox with no file, network, clock or
  randomness access, capped in memory and CPU (fuel);
- every task runs on **several devices**, and a result counts only when a **quorum agrees
  bit for bit**; devices that disagree get strikes and are banned;
- devices only make **outbound** HTTP requests, so they work behind any NAT or firewall;
- devices earn **credits** in fuel, a deterministic and verifiable unit of work.

Status: early (phase 1 of [the roadmap](docs/architecture.md#roadmap)). CPU tasks work;
GPU via WebGPU is next.

## Quick start

```bash
cargo build --release
./target/release/meshpool coordinator                 # on a reachable machine
./target/release/meshpool agent                       # on every device (MESHPOOL_URL=...), one task per core
./target/release/meshpool submit examples/tasks/reverse.wat --input "hello, pool"
./target/release/meshpool agents                      # devices, strikes, credits
```

`submit` uploads the module and input, waits for the agreed result and prints it.
Options: `--replicas` (default 2), `--fuel`, `--memory-mb`, `--input-file`, `--detach`.
Tokens: `MESHPOOL_TOKEN` for clients, `MESHPOOL_AGENT_TOKEN` to restrict who may join.
Replicas of a job go to devices with distinct IP addresses; to try a pool on one machine,
start the coordinator with `--allow-same-ip`.

From Python, with the [client](sdk/python) (standard library only):

```python
from meshpool import Pool
pool = Pool("http://coordinator:7700")
outputs = pool.map(pool.upload("my_task.wasm"), [b"a", b"b", b"c"])
```

## Writing a task

A task is a WASM module exporting `memory` and `run()`, with three host functions in
module `env`: `input_len() -> i32`, `read_input(ptr)`, `write_output(ptr, len)`.
See [examples/tasks](examples/tasks). Any language that compiles to `wasm32` works.

## Docs

- [Architecture, trust model, roadmap](docs/architecture.md)
- [Benchmarks vs Ray and Dask](docs/benchmarks.md)
- [Changelog](CHANGELOG.md)

## License

MIT
