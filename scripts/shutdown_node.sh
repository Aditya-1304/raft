#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: ./scripts/shutdown_node.sh <node-id>" >&2
  exit 1
fi

node_id="$1"

ensure_binary
build_cluster_args

(
  cd "${PROJECT_ROOT}"
  "${RAFT_BIN}" shutdown --node-id "${node_id}" "${CLUSTER_ARGS[@]}"
)

sleep 0.2
remove_node_pid "${node_id}"
