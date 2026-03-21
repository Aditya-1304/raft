#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ $# -ne 1 ]]; then
  echo "usage: ./scripts/restart_node.sh <node-id>" >&2
  exit 1
fi

node_id="$1"

if is_node_running "${node_id}"; then
  echo "node ${node_id} is already running with pid $(node_pid "${node_id}")" >&2
  exit 1
fi

start_node_process "${node_id}"
