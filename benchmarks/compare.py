"""Ray and Dask on the same workloads as meshpool (examples/bench.rs).

To compare scheduling and fault tolerance rather than languages, Ray and Dask run the
very same WebAssembly module through wasmtime-py with the same host ABI.

    uv run --python 3.12 --with ray --with 'dask[distributed]' --with wasmtime --with psutil \
        benchmarks/compare.py --system ray --kind primes --tasks 32 --n 3000000 --kill-after 1
"""

import argparse
import json
import os
import signal
import struct
import threading
import time
from pathlib import Path

TASKS = Path(__file__).resolve().parent.parent / "examples" / "tasks"
_modules = {}


def run_wasm(name: str, data: bytes) -> bytes:
    import wasmtime

    if name not in _modules:  # compile once per worker process, as meshpool agents cache modules
        engine = wasmtime.Engine()
        _modules[name] = (engine, wasmtime.Module(engine, (TASKS / name).read_text()))
    engine, module = _modules[name]
    store = wasmtime.Store(engine)
    out = {}
    i32 = wasmtime.ValType.i32()

    def input_len(caller):
        return len(data)

    def read_input(caller, ptr):
        caller["memory"].write(caller, data, ptr)

    def write_output(caller, ptr, length):
        out["data"] = caller["memory"].read(caller, ptr, ptr + length)

    linker = wasmtime.Linker(engine)
    linker.define_func("env", "input_len", wasmtime.FuncType([], [i32]), input_len, access_caller=True)
    linker.define_func("env", "read_input", wasmtime.FuncType([i32], []), read_input, access_caller=True)
    linker.define_func("env", "write_output", wasmtime.FuncType([i32, i32], []), write_output, access_caller=True)
    instance = linker.instantiate(store, module)
    instance.exports(store)["run"](store)
    return bytes(out["data"])


def count_primes(n: int) -> int:
    sieve = bytearray([1]) * n
    sieve[:2] = b"\0\0"
    for i in range(2, int(n**0.5) + 1):
        if sieve[i]:
            sieve[i * i :: i] = bytearray(len(range(i * i, n, i)))
    return sum(sieve)


def workload(kind: str, n: int):
    if kind == "primes":
        return "primes.wat", struct.pack("<I", n), struct.pack("<I", count_primes(n))
    return "reverse.wat", b"0123456789abcdef", b"fedcba9876543210"


def kill_later(delay: float, pick_pid):
    def go():
        time.sleep(delay)
        pid = pick_pid()
        if pid:
            os.kill(pid, signal.SIGKILL)
            print(f"[x] killed worker pid {pid} at {delay}s", flush=True)

    threading.Thread(target=go, daemon=True).start()


def bench_ray(args, name, data):
    import psutil
    import ray

    ray.init(num_cpus=args.workers, include_dashboard=False, logging_level="ERROR")
    task = ray.remote(max_retries=3)(run_wasm)
    ray.get([task.remote("reverse.wat", b"warm") for _ in range(args.workers * 4)])  # start workers

    def busy_worker():
        for p in psutil.process_iter(["cmdline"]):
            if any("ray::run_wasm" in c for c in (p.info["cmdline"] or [])):
                return p.pid

    start = time.perf_counter()
    if args.kill_after:
        kill_later(args.kill_after, busy_worker)
    results = ray.get([task.remote(name, data) for _ in range(args.tasks)])
    secs = time.perf_counter() - start
    ray.shutdown()
    return results, secs


def bench_dask(args, name, data):
    from dask.distributed import Client, LocalCluster

    cluster = LocalCluster(n_workers=args.workers, threads_per_worker=1, processes=True, dashboard_address=None)
    client = Client(cluster)
    client.gather(client.map(run_wasm, ["reverse.wat"] * args.workers * 4, [b"warm"] * args.workers * 4, pure=False))
    pids = list(client.run(os.getpid).values())
    start = time.perf_counter()
    if args.kill_after:
        kill_later(args.kill_after, lambda: pids[0])
    futures = client.map(run_wasm, [name] * args.tasks, [data] * args.tasks, pure=False, retries=3)
    results = client.gather(futures)
    secs = time.perf_counter() - start
    client.close()
    cluster.close()
    return results, secs


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--system", choices=["ray", "dask"], required=True)
    p.add_argument("--kind", default="noop", choices=["noop", "primes"])
    p.add_argument("--tasks", type=int, default=1000)
    p.add_argument("--n", type=int, default=2_000_000)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--kill-after", type=float, default=0.0, help="SIGKILL one worker after N seconds")
    args = p.parse_args()
    name, data, expected = workload(args.kind, args.n)
    results, secs = (bench_ray if args.system == "ray" else bench_dask)(args, name, data)
    ok = sum(r == expected for r in results)
    print(json.dumps({
        "system": args.system, "kind": args.kind, "tasks": args.tasks, "killed": bool(args.kill_after),
        "ok": ok, "seconds": round(secs, 3), "tasks_per_s": round(args.tasks / secs, 1),
    }))


if __name__ == "__main__":
    main()
