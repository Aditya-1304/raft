#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_runtime_dirs
ensure_binary

echo "starting Raft cluster"
print_cluster_banner

for node_id in "${RAFT_NODES[@]}"; do
  start_node_process "${node_id}"
done

echo
echo "cluster start requested"
echo "watch status with: ./scripts/watch.sh"
echo "submit command with: ./scripts/propose.sh 5"
echo "kill current leader with: ./scripts/kill_leader.sh"
