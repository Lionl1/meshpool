"""Runs the client against a real local pool: one coordinator and two agents built from
this repository (`cargo build`; MESHPOOL_BIN overrides the binary).
reverse.wasm and spin.wasm are examples/tasks/*.wat in binary form."""

import os
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
from pathlib import Path

from meshpool import JobFailed, Pool

HERE = Path(__file__).parent
ROOT = HERE.parents[2]
BIN = os.environ.get("MESHPOOL_BIN", str(ROOT / "target" / "debug" / "meshpool"))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class PoolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "MESHPOOL_BIN" not in os.environ:
            subprocess.run(["cargo", "build", "--quiet", "--bin", "meshpool"], cwd=ROOT, check=True)
        url = f"http://127.0.0.1:{free_port()}"
        cls.procs = [subprocess.Popen([BIN, "coordinator", "--listen", url[7:], "--allow-same-ip"],
                                      stderr=subprocess.DEVNULL)]
        cls.pool = Pool(url)
        for _ in range(100):  # wait for the coordinator
            try:
                cls.pool.agents()
                break
            except OSError:
                time.sleep(0.05)
        for name in ["a", "b"]:
            cls.procs.append(subprocess.Popen(
                [BIN, "--url", url, "agent", "--name", name, "--slots", "2", "--heartbeat", "0.5"],
                stderr=subprocess.DEVNULL))

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs:
            p.kill()
            p.wait()

    def test_map_returns_outputs_in_order(self):
        module = self.pool.upload(HERE / "reverse.wasm")
        inputs = [f"item {i}" for i in range(50)] + ["item 0"]  # duplicates are fine
        outputs = self.pool.map(module, inputs, timeout=30)
        self.assertEqual(outputs, [s[::-1].encode() for s in inputs])
        self.assertEqual(self.pool.run(module, b"pool", timeout=30), b"loop")
        self.assertTrue(all(a["credits"] > 0 for a in self.pool.agents()))

    def test_map_json(self):
        module = self.pool.upload(HERE / "reverse.wasm")
        # Reversing a JSON string's bytes keeps it a JSON string.
        self.assertEqual(self.pool.map_json(module, ["abc", "xy"], timeout=30), ["cba", "yx"])

    def test_failure_raises(self):
        module = self.pool.upload((HERE / "spin.wasm").read_bytes())
        with self.assertRaises(JobFailed) as e:
            self.pool.map(module, [b""], fuel=100_000, timeout=30)
        self.assertEqual(e.exception.view["outcome"]["kind"], "out_of_fuel")


class TlsTest(unittest.TestCase):
    """A coordinator with a self-signed certificate: trusted with `ca_cert`, refused without."""

    def test_https_pool(self):
        if "MESHPOOL_BIN" not in os.environ:
            subprocess.run(["cargo", "build", "--quiet", "--bin", "meshpool"], cwd=ROOT, check=True)
        with tempfile.TemporaryDirectory() as tmp:
            cert, key = f"{tmp}/cert.pem", f"{tmp}/key.pem"
            subprocess.run(
                ["openssl", "req", "-x509", "-newkey", "ec",
                 "-pkeyopt", "ec_paramgen_curve:P-256", "-pkeyopt", "ec_param_enc:named_curve",
                 "-nodes", "-days", "1", "-subj", "/CN=localhost", "-keyout", key, "-out", cert,
                 "-addext", "subjectAltName=DNS:localhost",
                 "-addext", "extendedKeyUsage=serverAuth",
                 # OpenSSL 3 marks it a CA otherwise; webpki (Linux) refuses that as a server's.
                 "-addext", "basicConstraints=critical,CA:FALSE"],
                check=True, capture_output=True)
            port = free_port()
            url = f"https://localhost:{port}"
            procs = [subprocess.Popen(
                [BIN, "coordinator", "--listen", f"127.0.0.1:{port}", "--allow-same-ip",
                 "--tls-cert", cert, "--tls-key", key], stderr=subprocess.DEVNULL)]
            try:
                pool = Pool(url, ca_cert=cert)
                for _ in range(100):
                    try:
                        pool.agents()
                        break
                    except OSError:
                        time.sleep(0.05)
                procs.append(subprocess.Popen(
                    [BIN, "--url", url, "--ca-cert", cert, "agent", "--name", "t"],
                    stderr=subprocess.DEVNULL))
                module = pool.upload(HERE / "reverse.wasm")
                self.assertEqual(pool.run(module, "tls", replicas=1, timeout=30), b"slt")
                with self.assertRaises(urllib.error.URLError):
                    Pool(url).agents()
            finally:
                for p in procs:
                    p.kill()
                    p.wait()


if __name__ == "__main__":
    unittest.main()
