# meshpool for Python

Run WebAssembly tasks on a [meshpool](../../README.md) pool from Python. Standard library
only, Python 3.9+.

```bash
pip install ./sdk/python        # from the repository root
```

```python
from meshpool import Pool, JobFailed

pool = Pool("http://coordinator:7700")        # or MESHPOOL_URL / MESHPOOL_TOKEN
# HTTPS pool with a self-signed certificate: Pool(url, ca_cert="cert.pem") or MESHPOOL_CA_CERT
module = pool.upload("my_task.wasm")          # a path or the module's bytes
outputs = pool.map(module, [b"a", b"b", b"c"], replicas=2)   # outputs in input order
wordcount = pool.upload("wordcount.wasm")     # a Rust task_json! task
counts = pool.map_json(wordcount, ["some text", "more text"])
print(pool.agents())                          # devices, strikes, credits
```

`map` uploads the inputs (each distinct input once), submits all jobs in batches and waits
for them in parallel; every output is checked against its hash. A job that fails on every
replica or finds no quorum raises `JobFailed` with the coordinator's view of the job.
Options: `replicas`, `fuel`, `memory_mb`, `timeout` (seconds).

Lower level: `put`/`get` for blobs, `submit` → job ids, `wait`, `result`, `job`.

Tests start a local coordinator and two agents built from this repository:

```bash
cd sdk/python && python -m unittest discover -s tests
```
