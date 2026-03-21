#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ $# -lt 1 ]]; then
  echo "usage: ./scripts/propose.sh <u64-command> [--target-node <id>]" >&2
  exit 1
fi

command="$1"
shift

ensure_binary
build_cluster_args

args=(
  propose
  --command "${command}"
  --attempts "${RAFT_PROPOSE_ATTEMPTS}"
  --retry-ms "${RAFT_PROPOSE_RETRY_MS}"
  "${CLUSTER_ARGS[@]}"
  "$@"
)

(
  cd "${PROJECT_ROOT}"
  exec "${RAFT_BIN}" "${args[@]}"
)
