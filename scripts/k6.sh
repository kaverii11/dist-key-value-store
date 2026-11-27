#!/usr/bin/env bash
# Run the k6 gRPC load test inside Docker, on the cluster's network.
#   VUS=32 DURATION=30s ./scripts/k6.sh
set -euo pipefail
cd "$(dirname "$0")/.."
docker run --rm --network kv_default \
  -e VUS="${VUS:-32}" -e DURATION="${DURATION:-30s}" \
  -v "$PWD/load:/scripts:ro" -v "$PWD/proto:/proto:ro" \
  grafana/k6:1.1.0 run /scripts/k6-kv.js
