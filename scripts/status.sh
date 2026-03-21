#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

ensure_binary
build_cluster_args

(
  cd "${PROJECT_ROOT}"
  exec "${RAFT_BIN}" status "${CLUSTER_ARGS[@]}"
)
