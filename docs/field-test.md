# Trying a pool on real networks

Unit and end-to-end tests run every device on one machine. Two tools take a pool out of
the lab.

## Real devices: `scripts/field-test.sh`

On a machine the devices can reach (a VPS, or a home machine with a forwarded port):

```bash
cargo build --release
scripts/field-test.sh serve pool.example.com     # or its public IP
```

It creates a self-signed certificate, a client token and a device token, starts the
coordinator with HTTPS and `--data-dir`, and prints the command to run on each device:
install meshpool with `cargo install`, copy `cert.pem`, start `meshpool agent`. Devices only
make outbound connections: laptops at home, phones on mobile data (Termux), office machines
behind NAT all work without any port forwarding on their side.

Then:

```bash
scripts/field-test.sh check                      # 64 jobs × 2 replicas, ~0.05 s each
scripts/field-test.sh check --tasks 500 --n 5000000
```

`meshpool check` submits jobs whose answers it computes itself (prime counts, a different
input per job), waits for all of them, fails if the pool agreed on a wrong answer, and
prints per device how many replicas it ran, its state and its strikes:

```
[v] 32 jobs × 2 replicas, every answer right, in 0.28 s (116.4 jobs/s)
device                             name             state      ran strikes  os/arch, gpu
f7f6598f6a6c4eb7a1e57ca6db7afd66   phone            online      32       0  macos/aarch64
33bef57a18824bfb8856c12eca7014b1   laptop           online      32       0  macos/aarch64
```

Worth trying while a `check` runs: switch a phone from Wi-Fi to mobile data, close a
laptop's lid, `kill -9` an agent, `kill` the coordinator and start `serve` again (its state
is in `field-test/data`). Every job should still finish with the right answer; a device that
comes back keeps its credits.

Several devices behind one router share one public address, and replicas of a job go to
distinct addresses: set `ALLOW_SAME_IP=1` before `serve` for a single-LAN test.

## Simulated networks: `deploy/netsim`

Docker Compose with three devices on separate networks, each with its own latency, jitter
and loss (`tc netem`): home Wi-Fi (15 ms, 0.1 %), mobile data (80 ± 30 ms, 2 %) and a far
office (120 ms). Each device has its own address, so the per-address rules stay on.

```bash
docker compose -f deploy/netsim/docker-compose.yml up -d --build
docker compose -f deploy/netsim/docker-compose.yml run --rm check
docker compose -f deploy/netsim/docker-compose.yml stop mobile        # a device leaves
docker compose -f deploy/netsim/docker-compose.yml restart coordinator
docker compose -f deploy/netsim/docker-compose.yml down -v
```
