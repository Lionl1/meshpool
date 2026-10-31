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

## Writing a task

In Rust, with the [SDK](sdk/rust): write an ordinary function and mark it as the task.

```rust
// Cargo.toml: crate-type = ["cdylib"], meshpool-task = { path = ".../sdk/rust" }
fn reverse(input: &[u8]) -> Vec<u8> {
    input.iter().rev().copied().collect()
}
meshpool_task::task!(reverse);
// or, with feature "json": fn f(x: In) -> Out, both serde types
// meshpool_task::task_json!(f);
```

```bash
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
meshpool submit target/wasm32-unknown-unknown/release/my_task.wasm --input "hello"
```

On the host the same functions are plain Rust, so unit tests need no pool. Tasks must be
deterministic (replicas compare outputs bit for bit): iterate `BTreeMap`s rather than
`HashMap`s and derive any randomness from the input; floats are fine. Examples:
[primes, word count (JSON), Mandelbrot strips](sdk/rust/examples).

Under the hood a task is a WASM module exporting `memory` and `run()`, with three host
functions in module `env`: `input_len() -> i32`, `read_input(ptr)`, `write_output(ptr, len)`.
Any language that compiles to `wasm32` can implement it; see the hand-written
[examples/tasks](examples/tasks).

## Docs

- [Architecture, trust model, roadmap](docs/architecture.md)
- [Benchmarks vs Ray and Dask](docs/benchmarks.md)
- [Changelog](CHANGELOG.md)

## License

MIT
