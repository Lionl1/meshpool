"""Python client for meshpool: run WebAssembly tasks on a pool of untrusted devices.

    from meshpool import Pool
    pool = Pool("http://coordinator:7700")
    module = pool.upload("task.wasm")
    outputs = pool.map(module, [b"a", b"b", b"c"])

Standard library only. Every output is checked against its SHA-256 hash.
"""

from .client import JobFailed, Pool

__all__ = ["JobFailed", "Pool"]
