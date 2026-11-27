#!/usr/bin/env bash
# Chaos test against the Docker cluster (deploy/docker-compose.yml):
# kvchaos writes for 40 s; at 10 s this script hard-kills (SIGKILL) the
# leader container, at 25 s it starts it again. kvchaos then checks that no
# acknowledged write was lost and that all 3 nodes converge.
set -euo pipefail
cd "$(dirname "$0")/.."

leader_container() {
  for i in 1 2 3; do
    port=$((2111 + i))
    if curl -s "localhost:${port}/metrics" | grep -q '^kv_raft_is_leader 1'; then
      echo "kv-node${i}"; return
    fi
  done
}

go build -o bin/kvchaos ./cmd/kvchaos
./bin/kvchaos -duration 40s "$@" &
CHAOS=$!

sleep 10
L=$(leader_container)
echo ">>> $(date +%T) killing leader ${L} (SIGKILL)"
docker kill "${L}" >/dev/null
sleep 15
echo ">>> $(date +%T) restarting ${L}"
docker start "${L}" >/dev/null

wait "${CHAOS}"
