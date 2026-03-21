#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_binary
build_cluster_args

for node_id in "${RAFT_NODES[@]}"; do
  echo "requesting graceful shutdown for node ${node_id}"
  (
    cd "${PROJECT_ROOT}"
    "${RAFT_BIN}" shutdown --node-id "${node_id}" "${CLUSTER_ARGS[@]}"
  ) || true

  sleep 0.1

  if is_node_running "${node_id}"; then
    echo "node ${node_id} still running, sending SIGTERM"
    stop_node_process "${node_id}" TERM || true
  fi

  remove_node_pid "${node_id}"
done

echo "cluster stop requested"
