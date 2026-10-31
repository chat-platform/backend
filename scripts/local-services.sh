#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_DIR="$ROOT_DIR/.local"
BIN_DIR="$LOCAL_DIR/bin"
RUN_DIR="$LOCAL_DIR/run"
LOG_DIR="$LOCAL_DIR/logs"

services=(core-api message-service ws-gateway relay notification-service)

usage() {
  echo "Usage: $0 {start|stop|status|logs}" >&2
  exit 2
}

ensure_dirs() {
  mkdir -p "$BIN_DIR" "$RUN_DIR" "$LOG_DIR"
}

pid_file() { echo "$RUN_DIR/$1.pid"; }
log_file() { echo "$LOG_DIR/$1.log"; }

is_running() {
  local service="$1" pidfile pid command
  pidfile="$(pid_file "$service")"
  [[ -f "$pidfile" ]] || return 1
  pid="$(cat "$pidfile")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"$BIN_DIR/$service"* ]]
}

build_services() {
  echo "Building backend services..."
  for service in "${services[@]}"; do
    echo "  $service"
    if ! (cd "$ROOT_DIR" && go build -o "$BIN_DIR/$service" "./services/$service/cmd/$service"); then
      echo "ERROR: failed to build $service; no services were started." >&2
      exit 1
    fi
  done
}

start_service() {
  local service="$1" pidfile logfile pid
  pidfile="$(pid_file "$service")"
  logfile="$(log_file "$service")"
  if is_running "$service"; then
    pid="$(cat "$pidfile")"
    echo "$service already running (pid $pid)"
    return
  fi
  rm -f "$pidfile"
  case "$service" in
    core-api)
      nohup "$BIN_DIR/$service" >>"$logfile" 2>&1 </dev/null & ;;
    message-service)
      nohup "$BIN_DIR/$service" >>"$logfile" 2>&1 </dev/null & ;;
    ws-gateway)
      nohup "$BIN_DIR/$service" >>"$logfile" 2>&1 </dev/null & ;;
    relay)
      nohup "$BIN_DIR/$service" >>"$logfile" 2>&1 </dev/null & ;;
    notification-service)
      nohup "$BIN_DIR/$service" >>"$logfile" 2>&1 </dev/null & ;;
    *) echo "Unknown service: $service" >&2; return 1 ;;
  esac
  pid=$!
  echo "$pid" > "$pidfile"
  sleep 0.2
  if is_running "$service"; then
    echo "Started $service (pid $pid); log: $logfile"
  else
    rm -f "$pidfile"
    echo "ERROR: $service exited during startup; see $logfile" >&2
    return 1
  fi
}

start_all() {
  cd "$ROOT_DIR"
  ensure_dirs
  if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: Go is required to build the services." >&2
    exit 1
  fi
  build_services
  echo "Starting services using process environment, .env, and application defaults."
  echo "Ensure both local dependencies are running and the database is migrated."
  for service in "${services[@]}"; do
    start_service "$service" || exit 1
  done
  echo "All backend services started. Use 'make logs-all' to follow logs."
}

stop_all() {
  local service pidfile pid
  for ((i=${#services[@]}-1; i>=0; i--)); do
    service="${services[$i]}"
    pidfile="$(pid_file "$service")"
    if is_running "$service"; then
      pid="$(cat "$pidfile")"
      kill -TERM "$pid" 2>/dev/null || true
      echo "Stopped $service (pid $pid)"
    elif [[ -f "$pidfile" ]]; then
      echo "$service is not running (removing stale PID file)"
    else
      echo "$service is not running"
    fi
    rm -f "$pidfile"
  done
}

status_all() {
  local service pid
  for service in "${services[@]}"; do
    if is_running "$service"; then
      pid="$(cat "$(pid_file "$service")")"
      echo "$service running (pid $pid)"
    else
      echo "$service stopped"
    fi
  done
}

logs_all() {
  local files=()
  for service in "${services[@]}"; do
    [[ -f "$(log_file "$service")" ]] && files+=("$(log_file "$service")")
  done
  if ((${#files[@]} == 0)); then
    echo "No service logs found. Start services with 'make run-all' first." >&2
    exit 1
  fi
  tail -F "${files[@]}"
}

logs_without_relay() {
  local files=()
  for service in "${services[@]}"; do
    [[ "$service" == "relay" ]] && continue
    [[ -f "$(log_file "$service")" ]] && files+=("$(log_file "$service")")
  done
  if ((${#files[@]} == 0)); then
    echo "No service logs found. Start services with 'make run-all' first." >&2
    exit 1
  fi
  tail -F "${files[@]}"
}

case "${1:-}" in
  start) start_all ;;
  stop) stop_all ;;
  status) status_all ;;
  logs) logs_all ;;
  logs-without-relay) logs_without_relay ;;
  *) usage ;;
esac
