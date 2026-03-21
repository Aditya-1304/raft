#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: ./scripts/snapshot.sh <node-id>" >&2
  exit 1
fi

node_id="$1"

ensure_binary
build_cluster_args

(
  cd "${PROJECT_ROOT}"
  exec "${RAFT_BIN}" snapshot --node-id "${node_id}" "${CLUSTER_ARGS[@]}"
)
