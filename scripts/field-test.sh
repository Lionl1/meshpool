#!/usr/bin/env bash
# Try meshpool on real devices on real networks.
#
#   scripts/field-test.sh serve HOST [PORT]   on a machine the devices can reach (HOST: its
#                                             public name or IP); starts a pool with HTTPS,
#                                             tokens and saved state, prints how to join
#   scripts/field-test.sh check [ARGS]        once devices joined: run jobs with known answers
#                                             and show what each device did (meshpool check)
#
# Files go to $FIELD_DIR (default ./field-test): cert.pem/key.pem, env, data/, coordinator.log.
# ALLOW_SAME_IP=1 lets devices share one address (one LAN behind one NAT, or a local try).
set -euo pipefail

FIELD_DIR=${FIELD_DIR:-field-test}
if [ -n "${MESHPOOL_BIN:-}" ]; then
  BIN=$MESHPOOL_BIN
elif command -v meshpool >/dev/null; then
  BIN=meshpool
else
  BIN="$(dirname "$0")/../target/release/meshpool"
fi

serve() {
  local host=${1:?usage: field-test.sh serve HOST [PORT]} port=${2:-7700}
  mkdir -p "$FIELD_DIR"
  if [ ! -f "$FIELD_DIR/cert.pem" ]; then
    local san="DNS:$host"
    [[ $host =~ ^[0-9.]+$ || $host == *:* ]] && san="IP:$host"
    # Named curve and serverAuth: what rustls requires (LibreSSL's defaults are not).
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
      -pkeyopt ec_param_enc:named_curve -nodes -days 30 -subj "/CN=$host" \
      -keyout "$FIELD_DIR/key.pem" -out "$FIELD_DIR/cert.pem" \
      -addext "subjectAltName=$san" -addext extendedKeyUsage=serverAuth 2>/dev/null
  fi
  if [ ! -f "$FIELD_DIR/env" ]; then
    cat >"$FIELD_DIR/env" <<ENV
MESHPOOL_URL=https://$host:$port
MESHPOOL_TOKEN=$(openssl rand -hex 16)
MESHPOOL_AGENT_TOKEN=$(openssl rand -hex 16)
MESHPOOL_CA_CERT=$(cd "$FIELD_DIR" && pwd)/cert.pem
ENV
  fi
  set -a; source "$FIELD_DIR/env"; set +a
  local same_ip=()
  [ -n "${ALLOW_SAME_IP:-}" ] && same_ip=(--allow-same-ip)
  "$BIN" coordinator --listen "0.0.0.0:$port" --tls-cert "$FIELD_DIR/cert.pem" \
    --tls-key "$FIELD_DIR/key.pem" --data-dir "$FIELD_DIR/data" ${same_ip[@]+"${same_ip[@]}"} \
    >>"$FIELD_DIR/coordinator.log" 2>&1 &
  echo "$!" >"$FIELD_DIR/coordinator.pid"
  sleep 0.5
  kill -0 "$(cat "$FIELD_DIR/coordinator.pid")" || { cat "$FIELD_DIR/coordinator.log"; exit 1; }
  cat <<MSG
Pool running at $MESHPOOL_URL (pid $(cat "$FIELD_DIR/coordinator.pid"), log $FIELD_DIR/coordinator.log).
Open TCP port $port to this machine if devices come from outside its network.

On every device (needs Rust; add --features gpu to lend a GPU):
  cargo install --locked --git https://github.com/Lionl1/meshpool meshpool
copy $FIELD_DIR/cert.pem to the device, then:
  MESHPOOL_URL=$MESHPOOL_URL MESHPOOL_CA_CERT=cert.pem \\
  MESHPOOL_AGENT_TOKEN=$MESHPOOL_AGENT_TOKEN meshpool agent --name \$(hostname)

When devices have joined:  scripts/field-test.sh check            (64 jobs × 2 replicas)
Longer jobs, more of them: scripts/field-test.sh check --tasks 500 --n 5000000
Stop:                      kill \$(cat $FIELD_DIR/coordinator.pid)   (state is saved)
MSG
}

check() {
  set -a; source "$FIELD_DIR/env"; set +a
  "$BIN" check "$@"
}

case "${1:-}" in
  serve) shift; serve "$@" ;;
  check) shift; check "$@" ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
