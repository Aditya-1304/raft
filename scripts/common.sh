#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RAFT_BIN="${RAFT_BIN:-${PROJECT_ROOT}/target/debug/raft}"

RAFT_NODES_TEXT="${RAFT_NODES:-1 2 3 4 5}"
read -r -a RAFT_NODES <<< "${RAFT_NODES_TEXT}"

RAFT_HOST="${RAFT_HOST:-127.0.0.1}"
RAFT_BASE_PORT="${RAFT_BASE_PORT:-7000}"
RAFT_ADMIN_BASE_PORT="${RAFT_ADMIN_BASE_PORT:-8000}"

RAFT_RUNTIME_ROOT="${RAFT_RUNTIME_ROOT:-${PROJECT_ROOT}/data/runtime-cluster}"
RAFT_LOG_DIR="${RAFT_LOG_DIR:-${RAFT_RUNTIME_ROOT}/logs}"
RAFT_PID_DIR="${RAFT_PID_DIR:-${RAFT_RUNTIME_ROOT}/pids}"

RAFT_TICK_MS="${RAFT_TICK_MS:-100}"
RAFT_ELECTION_TIMEOUT="${RAFT_ELECTION_TIMEOUT:-10}"
RAFT_HEARTBEAT_INTERVAL="${RAFT_HEARTBEAT_INTERVAL:-3}"
RAFT_SNAPSHOT_EVERY="${RAFT_SNAPSHOT_EVERY:-5}"

RAFT_PROPOSE_ATTEMPTS="${RAFT_PROPOSE_ATTEMPTS:-20}"
RAFT_PROPOSE_RETRY_MS="${RAFT_PROPOSE_RETRY_MS:-250}"
RAFT_WATCH_INTERVAL_MS="${RAFT_WATCH_INTERVAL_MS:-1000}"

ensure_runtime_dirs() {
  mkdir -p "${RAFT_RUNTIME_ROOT}" "${RAFT_LOG_DIR}" "${RAFT_PID_DIR}"
}

ensure_binary() {
  if [[ ! -x "${RAFT_BIN}" ]]; then
    (cd "${PROJECT_ROOT}" && cargo build)
  fi
}

raft_port() {
  local node_id="$1"
  echo $((RAFT_BASE_PORT + node_id))
}

admin_port() {
  local node_id="$1"
  echo $((RAFT_ADMIN_BASE_PORT + node_id))
}

cluster_member() {
  local node_id="$1"
  echo "${node_id}=${RAFT_HOST}:$(raft_port "${node_id}")@${RAFT_HOST}:$(admin_port "${node_id}")"
}

build_cluster_args() {
  CLUSTER_ARGS=()
  local node_id
  for node_id in "${RAFT_NODES[@]}"; do
    CLUSTER_ARGS+=(--cluster "$(cluster_member "${node_id}")")
  done
}

node_data_dir() {
  local node_id="$1"
  echo "${RAFT_RUNTIME_ROOT}/node${node_id}"
}

node_log_file() {
  local node_id="$1"
  echo "${RAFT_LOG_DIR}/node${node_id}.log"
}

node_pid_file() {
  local node_id="$1"
  echo "${RAFT_PID_DIR}/node${node_id}.pid"
}

is_pid_alive() {
  local pid="$1"
  kill -0 "${pid}" 2>/dev/null
}

node_pid() {
  local node_id="$1"
  local pid_file
  pid_file="$(node_pid_file "${node_id}")"

  if [[ -f "${pid_file}" ]]; then
    cat "${pid_file}"
  fi
}

is_node_running() {
  local node_id="$1"
  local pid
  pid="$(node_pid "${node_id}" || true)"

  [[ -n "${pid}" ]] && is_pid_alive "${pid}"
}

write_node_pid() {
  local node_id="$1"
  local pid="$2"
  echo "${pid}" > "$(node_pid_file "${node_id}")"
}

remove_node_pid() {
  local node_id="$1"
  rm -f "$(node_pid_file "${node_id}")"
}

start_node_process() {
  local node_id="$1"

  ensure_runtime_dirs
  ensure_binary
  build_cluster_args

  if is_node_running "${node_id}"; then
    echo "node ${node_id} already running with pid $(node_pid "${node_id}")"
    return 0
  fi

  mkdir -p "$(node_data_dir "${node_id}")"

  local log_file
  log_file="$(node_log_file "${node_id}")"

  (
    cd "${PROJECT_ROOT}"
    exec "${RAFT_BIN}" node \
      --id "${node_id}" \
      --data-dir "$(node_data_dir "${node_id}")" \
      --tick-ms "${RAFT_TICK_MS}" \
      --election-timeout "${RAFT_ELECTION_TIMEOUT}" \
      --heartbeat-interval "${RAFT_HEARTBEAT_INTERVAL}" \
      --snapshot-every "${RAFT_SNAPSHOT_EVERY}" \
      "${CLUSTER_ARGS[@]}"
  ) >>"${log_file}" 2>&1 &

  local pid=$!
  write_node_pid "${node_id}" "${pid}"

  echo "started node ${node_id} pid=${pid} raft=${RAFT_HOST}:$(raft_port "${node_id}") admin=${RAFT_HOST}:$(admin_port "${node_id}") log=${log_file}"
}

stop_node_process() {
  local node_id="$1"
  local signal="${2:-TERM}"
  local pid
  pid="$(node_pid "${node_id}" || true)"

  if [[ -z "${pid}" ]]; then
    echo "no pid file for node ${node_id}"
    return 1
  fi

  if ! is_pid_alive "${pid}"; then
    echo "node ${node_id} pid ${pid} is already gone"
    remove_node_pid "${node_id}"
    return 1
  fi

  kill "-${signal}" "${pid}"
}

find_leader_id() {
  ensure_binary
  build_cluster_args

  local output
  output="$(
    cd "${PROJECT_ROOT}" &&
      "${RAFT_BIN}" status "${CLUSTER_ARGS[@]}"
  )"

  local leader_line
  leader_line="$(printf '%s\n' "${output}" | grep 'role=Leader' | head -n 1 || true)"

  if [[ -z "${leader_line}" ]]; then
    return 1
  fi

  printf '%s\n' "${leader_line}" | sed -E 's/^node ([0-9]+) \|.*/\1/'
}

print_cluster_banner() {
  echo "cluster nodes: ${RAFT_NODES[*]}"
  local node_id
  for node_id in "${RAFT_NODES[@]}"; do
    echo "  node ${node_id}: raft=${RAFT_HOST}:$(raft_port "${node_id}") admin=${RAFT_HOST}:$(admin_port "${node_id}") data=$(node_data_dir "${node_id}")"
  done
}
