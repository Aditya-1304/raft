#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

leader_id="$(find_leader_id || true)"

if [[ -z "${leader_id}" ]]; then
  echo "could not determine current leader" >&2
  exit 1
fi

echo "current leader is node ${leader_id}"

if is_node_running "${leader_id}"; then
  stop_node_process "${leader_id}" KILL
  sleep 0.1
  remove_node_pid "${leader_id}"
  echo "sent SIGKILL to node ${leader_id}"
else
  echo "leader node ${leader_id} is not tracked by pid file or already exited" >&2
  exit 1
fi
