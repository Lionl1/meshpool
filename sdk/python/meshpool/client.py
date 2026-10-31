import hashlib
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable, List, Optional, Union

Data = Union[bytes, bytearray, str]

# Server limit on jobs per batch request.
MAX_BATCH = 10_000
ATTEMPTS = 5


class JobFailed(Exception):
    """A job ended without an agreed output: the task failed on every replica, or the
    devices did not agree. `view` is the job as the coordinator reports it."""

    def __init__(self, view: dict):
        self.view = view
        outcome = view.get("outcome") or {}
        reason = view.get("error") or f"{outcome.get('kind')} ({outcome.get('detail')})"
        super().__init__(f"job {view['id']} {view['status']}: {reason}")


class HttpError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        super().__init__(f"HTTP {status}: {body}")


def blob_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _bytes(data: Data) -> bytes:
    return data.encode() if isinstance(data, str) else bytes(data)


class Pool:
    """A client of one coordinator. Safe to share between threads."""

    def __init__(self, url: Optional[str] = None, token: Optional[str] = None, workers: int = 32,
                 ca_cert: Optional[str] = None):
        """`ca_cert`: an extra CA certificate (PEM path) to trust, e.g. the pool's
        self-signed one; default `MESHPOOL_CA_CERT`."""
        self.url = (url or os.environ.get("MESHPOOL_URL", "http://127.0.0.1:7700")).rstrip("/")
        self.token = token or os.environ.get("MESHPOOL_TOKEN")
        self.workers = workers
        self.ssl = ssl.create_default_context()
        ca_cert = ca_cert or os.environ.get("MESHPOOL_CA_CERT")
        if ca_cert:
            self.ssl.load_verify_locations(cafile=ca_cert)

    def _request(self, method: str, path: str, body: Optional[bytes] = None,
                 content_type: Optional[str] = None, retry: bool = True) -> bytes:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if content_type:
            headers["Content-Type"] = content_type
        for attempt in range(ATTEMPTS):
            req = urllib.request.Request(self.url + path, data=body, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=120, context=self.ssl) as resp:
                    return resp.read()
            except urllib.error.HTTPError as e:
                raise HttpError(e.code, e.read().decode(errors="replace")) from None
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                # Only requests that are safe to repeat are retried.
                if not retry or attempt == ATTEMPTS - 1:
                    raise
                time.sleep(0.1 * 2**attempt)
        raise AssertionError("unreachable")

    def _json(self, method: str, path: str, value: Any = None, retry: bool = True) -> Any:
        body = None if value is None else json.dumps(value).encode()
        return json.loads(self._request(method, path, body, "application/json", retry))

    # Blobs

    def put(self, data: Data) -> str:
        """Upload a blob; returns its hash. Uploading the same data twice is harmless."""
        return json.loads(self._request("POST", "/v1/blobs", _bytes(data)))["id"]

    def get(self, hash: str) -> bytes:
        data = self._request("GET", f"/v1/blobs/{hash}")
        if blob_hash(data) != hash:
            raise ValueError(f"blob {hash} does not match its hash")
        return data

    def upload(self, module: Union[str, os.PathLike, bytes]) -> str:
        """Upload a task module (a `.wasm` path or the module's bytes); returns its hash."""
        if isinstance(module, (bytes, bytearray)):
            return self.put(module)
        with open(module, "rb") as f:
            return self.put(f.read())

    # Jobs

    def submit(self, module: str, inputs: Iterable[Data], replicas: int = 2,
               fuel: int = 10_000_000_000, memory_mb: int = 64) -> List[str]:
        """Upload the inputs and submit one job per input; returns job ids in order.
        `module` is a hash from `upload`."""
        datas = [_bytes(d) for d in inputs]
        unique = {blob_hash(d): d for d in datas}
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(self.put, unique.values()))
        specs = [{"module": module, "input": blob_hash(d), "fuel": fuel,
                  "memory_mb": memory_mb, "replicas": replicas} for d in datas]
        ids: List[str] = []
        for i in range(0, len(specs), MAX_BATCH):
            # Not retried: a lost response would submit the jobs twice.
            ids += self._json("POST", "/v1/jobs/batch", specs[i:i + MAX_BATCH], retry=False)
        return ids

    def job(self, id: str, wait: float = 0) -> dict:
        return self._json("GET", f"/v1/jobs/{id}?wait={wait}")

    def wait(self, id: str, timeout: Optional[float] = None) -> dict:
        """Wait until the job is decided (or `timeout` seconds pass); returns the job."""
        until = None if timeout is None else time.monotonic() + timeout
        while True:
            left = 30.0 if until is None else max(0.0, min(30.0, until - time.monotonic()))
            view = self.job(id, left)
            if view["status"] != "pending" or (until is not None and left == 0):
                return view

    def result(self, id: str, timeout: Optional[float] = None) -> bytes:
        """The agreed output of a job; raises `JobFailed` if there is none."""
        view = self.wait(id, timeout)
        outcome = view.get("outcome") or {}
        if view["status"] != "done" or outcome.get("status") != "ok":
            raise JobFailed(view)
        return self.get(outcome["output"])

    def map(self, module: str, inputs: Iterable[Data], timeout: Optional[float] = None,
            **spec) -> List[bytes]:
        """Run the task on every input; returns the outputs in input order. Keyword
        arguments go to `submit` (replicas, fuel, memory_mb)."""
        ids = self.submit(module, inputs, **spec)
        with ThreadPoolExecutor(self.workers) as ex:
            return list(ex.map(lambda id: self.result(id, timeout), ids))

    def map_json(self, module: str, values: Iterable[Any], **kwargs) -> List[Any]:
        """`map` for tasks that take and return JSON (e.g. Rust `task_json!` tasks)."""
        inputs = [json.dumps(v, separators=(",", ":")) for v in values]
        return [json.loads(out) for out in self.map(module, inputs, **kwargs)]

    def run(self, module: str, input: Data = b"", **kwargs) -> bytes:
        return self.map(module, [input], **kwargs)[0]

    # Devices

    def agents(self) -> List[dict]:
        return self._json("GET", "/v1/agents")
