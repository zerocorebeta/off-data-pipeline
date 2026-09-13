#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 LOG_FILE [--metrics-interval-seconds N] [--status-file PATH] -- COMMAND [ARG ...]" >&2
}

log_file="${1:-}"
[[ -n "$log_file" ]] || { usage; exit 2; }
shift
interval=30
status_file=""
while [[ "${1:-}" == --metrics-interval-seconds || "${1:-}" == --status-file ]]; do
  case "$1" in
    --metrics-interval-seconds)
      [[ "${2:-}" =~ ^[0-9]+$ ]] || { usage; exit 2; }
      interval="$2"
      shift 2
      ;;
    --status-file)
      [[ -n "${2:-}" && "${2:-}" != -* ]] || { usage; exit 2; }
      status_file="$2"
      shift 2
      ;;
  esac
done
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
progress_state=""
metrics_state=""
if [[ -n "$status_file" ]]; then
  command -v jq >/dev/null 2>&1 || {
    echo 'jq is required when --status-file is used' >&2
    rm -f -- "$sentinel"
    exit 2
  }
  mkdir -p "$(dirname "$status_file")"
  progress_state="${status_file}.progress"
  metrics_state="${status_file}.metrics"
  rm -f -- "$progress_state" "$metrics_state"
fi

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

read_state() {
  local path="$1"
  if [[ -n "$path" && -s "$path" ]]; then
    cat "$path"
  else
    printf '{}\n'
  fi
}

progress_json() {
  local line="$1"
  [[ "$line" == OFF_PROGRESS* ]] || { printf '{}\n'; return; }
  jq -Rn --arg line "$line" '
    ($line | split(" ") | map(select(length > 0)) | .[1:])
    | map(select(index("=") != null)
        | (split("=") as $parts
           | {key: $parts[0], value: ($parts[1:] | join("="))}))
    | from_entries
    | with_entries(
        if (.value | type) == "string"
        and (.value | test("^-?[0-9]+(\\.[0-9]+)?$"))
        then .value |= tonumber
        else .
        end
      )
  '
}

atomic_write() {
  local path="$1"
  local content="$2"
  local temporary
  temporary="$(mktemp "${path}.tmp.XXXXXX")" || return 1
  if ! printf '%s\n' "$content" > "$temporary"; then
    rm -f -- "$temporary"
    return 1
  fi
  mv -f -- "$temporary" "$path"
}

write_status() {
  [[ -n "$status_file" ]] || return 0
  local resources_json="${1-}"
  local state="${2:-running}"
  local exit_code="${3-}"
  [[ -n "$resources_json" ]] || resources_json='{}'
  local latest_line=""
  if [[ -s "$progress_state" ]]; then
    latest_line="$(< "$progress_state")"
  fi
  local counters_json phase temporary
  counters_json="$(progress_json "$latest_line")"
  phase="$(jq -r '.phase // "unknown"' <<< "$counters_json")"
  temporary="$(mktemp "${status_file}.tmp.XXXXXX")" || return 1
  if ! jq -n \
    --arg run_id "${GITHUB_RUN_ID:-local}" \
    --arg run_attempt "${GITHUB_RUN_ATTEMPT:-1}" \
    --arg timestamp "$(date -u +%FT%TZ)" \
    --arg state "$state" \
    --arg phase "$phase" \
    --arg exit_code "$exit_code" \
    --argjson counters "$counters_json" \
    --argjson resources "$resources_json" \
    '{
      runId: $run_id,
      runAttempt: $run_attempt,
      timestamp: $timestamp,
      state: $state,
      phase: $phase,
      counters: $counters,
      resources: $resources
    }
    + (if $exit_code == "" then {} else {exitCode: ($exit_code | tonumber)} end)' \
    > "$temporary"; then
    rm -f -- "$temporary"
    return 1
  fi
  mv -f -- "$temporary" "$status_file"
}

processes_json() {
  ps -eo pid=,ppid=,stat=,etime=,pcpu=,pmem=,rss=,comm= 2>/dev/null \
    | awk 'NF >= 8 { printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", $1,$2,$3,$4,$5,$6,$7,$8 }' \
    | jq -Rsc '
        split("\n") | map(select(length > 0) | split("\t") | {
          pid: (.[0] | try tonumber catch null),
          ppid: (.[1] | try tonumber catch null),
          state: .[2],
          elapsed: .[3],
          cpuPercent: (.[4] | try tonumber catch null),
          memoryPercent: (.[5] | try tonumber catch null),
          rssKb: (.[6] | try tonumber catch null),
          command: .[7]
        })
      '
}

runner_metrics() {
  while [[ -e "$sentinel" ]]; do
    local elapsed disk_free_kb mem_total_kb mem_available_kb mem_used_kb
    local loadavg cpu_count cpu_percent resources process_list
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
    if cpu_percent="$(ps -A -o pcpu= 2>/dev/null | awk '{sum += $1} END {printf "%.2f", sum + 0}')"; then
      :
    else
      cpu_percent=unknown
    fi
    if process_list="$(processes_json)"; then
      :
    else
      process_list='[]'
    fi
    resources="$(jq -n \
      --arg elapsed "$elapsed" \
      --arg disk_free_kb "$disk_free_kb" \
      --arg mem_total_kb "$mem_total_kb" \
      --arg mem_used_kb "$mem_used_kb" \
      --arg mem_available_kb "$mem_available_kb" \
      --arg cpu_count "$cpu_count" \
      --arg cpu_percent "$cpu_percent" \
      --arg load_average "$loadavg" \
      --argjson processes "$process_list" \
      '{
        elapsedSeconds: ($elapsed | try tonumber catch null),
        diskFreeKb: ($disk_free_kb | try tonumber catch null),
        memoryTotalKb: ($mem_total_kb | try tonumber catch null),
        memoryUsedKb: ($mem_used_kb | try tonumber catch null),
        memoryAvailableKb: ($mem_available_kb | try tonumber catch null),
        cpuCount: ($cpu_count | try tonumber catch null),
        cpuPercent: ($cpu_percent | try tonumber catch null),
        loadAverage: $load_average,
        processes: $processes
      }')"
    emit "elapsed_seconds=${elapsed} disk_free_kb=${disk_free_kb} memory_total_kb=${mem_total_kb} memory_used_kb=${mem_used_kb} cpu_count=${cpu_count} cpu_percent=${cpu_percent} load_average=${loadavg}"
    emit_processes top_processes 13 '' -eo pid=,ppid=,stat=,etime=,%cpu=,%mem=,rss=,comm=,args= --sort=-%cpu
    emit_processes pipeline_processes 20 '[c]argo|[o]ff-data-pipeline|[r]ustc' -eo pid=,ppid=,stat=,etime=,%cpu=,%mem=,rss=,comm=,args=
    if [[ -n "$metrics_state" ]]; then
      atomic_write "$metrics_state" "$resources" || emit 'heartbeat resources write failed'
      write_status "$resources" running || emit 'heartbeat status write failed'
    fi
    sleep "$interval"
  done
}

if [[ -n "$status_file" ]]; then
  write_status '{}' running || { rm -f -- "$sentinel"; exit 2; }
fi
runner_metrics &
monitor_pid="$!"
set +e
"$@" 2>&1 \
  | while IFS= read -r line || [[ -n "$line" ]]; do
      printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$line"
      if [[ -n "$progress_state" && "$line" == OFF_PROGRESS* ]]; then
        atomic_write "$progress_state" "$line" || true
        write_status "$(read_state "$metrics_state")" running || true
      fi
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
if [[ -n "$status_file" ]]; then
  if [[ "$command_status" == 0 ]]; then
    write_status "$(read_state "$metrics_state")" success "${command_status}" || true
  else
    write_status "$(read_state "$metrics_state")" failure "${command_status}" || true
  fi
  rm -f -- "$progress_state" "$metrics_state"
fi
exit "$command_status"
