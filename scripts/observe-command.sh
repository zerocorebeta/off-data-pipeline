#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 LOG_FILE [--metrics-interval-seconds N] -- COMMAND [ARG ...]" >&2
}

log_file="${1:-}"
[[ -n "$log_file" ]] || { usage; exit 2; }
shift
interval=30
if [[ "${1:-}" == --metrics-interval-seconds ]]; then
  [[ "${2:-}" =~ ^[0-9]+$ ]] || { usage; exit 2; }
  interval="$2"
  shift 2
fi
[[ "$interval" -ge 5 && "$interval" -le 3600 ]] || {
  echo 'metrics interval must be between 5 and 3600 seconds' >&2
  exit 2
}
[[ "${1:-}" == -- ]] || { usage; exit 2; }
shift
[[ "$#" -gt 0 ]] || { usage; exit 2; }

mkdir -p "$(dirname "$log_file")"
touch "$log_file"
sentinel="${TMPDIR:-/tmp}/off-observe.$$.active"
: > "$sentinel"
monitor_pid=""
started_at="$SECONDS"

emit() {
  printf '[%s] [runner] %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$log_file"
}

emit_processes() {
  local title="$1"
  local limit="$2"
  local filter="$3"
  shift 3
  emit "$title pid ppid state elapsed cpu_percent memory_percent rss_kb command"
  local lines
  lines="$(ps "$@" 2>/dev/null | sed -n "1,${limit}p" || true)"
  if [[ -n "$filter" ]]; then
    lines="$(grep -E "$filter" <<< "$lines" || true)"
  fi
  if [[ -z "$lines" ]]; then
    emit 'unavailable=true'
    return
  fi
  while IFS= read -r line; do
    [[ -n "$line" ]] && emit "$line"
  done <<< "$lines"
}

runner_metrics() {
  while [[ -e "$sentinel" ]]; do
    local elapsed disk_free_kb mem_total_kb mem_available_kb mem_used_kb
    local loadavg cpu_count
    elapsed=$((SECONDS - started_at))
    disk_free_kb="$(df -Pk "${RUNNER_TEMP:-/tmp}" 2>/dev/null | awk 'NR==2 {print $4}' || echo unknown)"
    mem_total_kb="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
    mem_available_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
    if [[ "$mem_total_kb" =~ ^[0-9]+$ && "$mem_available_kb" =~ ^[0-9]+$ ]]; then
      mem_used_kb=$((mem_total_kb - mem_available_kb))
    else
      mem_used_kb=unknown
    fi
    loadavg="$(awk '{print $1","$2","$3}' /proc/loadavg 2>/dev/null || uptime)"
    cpu_count="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    emit "elapsed_seconds=${elapsed} disk_free_kb=${disk_free_kb} memory_total_kb=${mem_total_kb} memory_used_kb=${mem_used_kb} memory_available_kb=${mem_available_kb} cpu_count=${cpu_count} load_average=${loadavg}"
    emit_processes top_processes 13 '' -eo pid=,ppid=,stat=,etime=,%cpu=,%mem=,rss=,comm=,args= --sort=-%cpu
    emit_processes pipeline_processes 20 '[c]argo|[o]ff-data-pipeline|[r]ustc' \
      -eo pid=,ppid=,stat=,etime=,%cpu=,%mem=,rss=,comm=,args=
    sleep "$interval"
  done
}

runner_metrics &
monitor_pid="$!"
set +e
"$@" 2>&1 \
  | while IFS= read -r line || [[ -n "$line" ]]; do
      printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$line"
    done \
  | tee -a "$log_file"
statuses=("${PIPESTATUS[@]}")
command_status="${statuses[0]:-1}"
set -e
rm -f -- "$sentinel"
if [[ -n "$monitor_pid" ]]; then
  kill "$monitor_pid" 2>/dev/null || true
  wait "$monitor_pid" 2>/dev/null || true
fi
exit "$command_status"
