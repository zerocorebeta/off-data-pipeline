#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 STATUS_JSON [--state running|success|failure|cancelled] [--exit-code N]" >&2
}

status_file="${1:-}"
[[ -n "$status_file" && -s "$status_file" ]] || { usage; exit 2; }
shift
state_override=""
exit_code_override=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --state)
      [[ "${2:-}" =~ ^(running|success|failure|cancelled)$ ]] || { usage; exit 2; }
      state_override="$2"
      shift 2
      ;;
    --exit-code)
      [[ "${2:-}" =~ ^[0-9]+$ ]] || { usage; exit 2; }
      exit_code_override="$2"
      shift 2
      ;;
    *) usage; exit 2 ;;
  esac
done
command -v sftp >/dev/null || { echo 'missing required command: sftp' >&2; exit 2; }
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
command -v jq >/dev/null || { echo 'missing required command: jq' >&2; exit 2; }
[[ -f "$SCRIPT_DIR/progress-schema.jq" ]] || { echo 'missing progress schema' >&2; exit 2; }
# Keep this schema deliberately closed: status files contain telemetry only,
# never credentials, environment values, or arbitrary runner output.
jq -e -f "$SCRIPT_DIR/progress-schema.jq" "$status_file" >/dev/null

run_id="$(jq -r '.runId' "$status_file")"
run_attempt="$(jq -r '.runAttempt' "$status_file")"
[[ "$run_id" =~ ^[A-Za-z0-9_.-]+$ && "$run_attempt" =~ ^[A-Za-z0-9_.-]+$ ]] || {
  echo 'heartbeat run identity is unsafe.' >&2
  exit 2
}
if [[ -n "${GITHUB_RUN_ID:-}" && "$run_id" != "$GITHUB_RUN_ID" ]]; then
  echo 'heartbeat runId does not match GITHUB_RUN_ID.' >&2
  exit 2
fi
if [[ -n "${GITHUB_RUN_ATTEMPT:-}" && "$run_attempt" != "$GITHUB_RUN_ATTEMPT" ]]; then
  echo 'heartbeat runAttempt does not match GITHUB_RUN_ATTEMPT.' >&2
  exit 2
fi

remote_dir="${SFTP_REMOTE_DIR:-}"
: "${SFTP_HOST:?SFTP_HOST is required}"
: "${SFTP_USER:?SFTP_USER is required}"
: "${SFTP_PRIVATE_KEY:?SFTP_PRIVATE_KEY is required}"
: "${SFTP_KNOWN_HOSTS:?SFTP_KNOWN_HOSTS is required}"
: "${remote_dir:?SFTP_REMOTE_DIR is required}"
[[ "$remote_dir" == /incoming ]] || {
  echo 'SFTP_REMOTE_DIR must be /incoming inside the OFF chroot.' >&2
  exit 2
}
[[ "$SFTP_HOST" =~ ^[A-Za-z0-9._:-]+$ && "$SFTP_USER" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || {
  echo 'SFTP host or user contains unsafe characters.' >&2
  exit 2
}
if [[ -n "${SFTP_PORT:-}" ]]; then
  [[ "$SFTP_PORT" =~ ^[0-9]{1,5}$ ]] || { echo 'SFTP_PORT must be numeric.' >&2; exit 2; }
fi

key="${TMPDIR:-/tmp}/off-progress-key.$$"
known_hosts="${TMPDIR:-/tmp}/off-progress-known-hosts.$$"
commands="${TMPDIR:-/tmp}/off-progress-commands.$$"
upload_file="$status_file"
cleanup() {
  rm -f -- "$key" "$known_hosts" "$commands" "$upload_file.publish.$$"
}
trap cleanup EXIT
umask 077
printf '%s\n' "$SFTP_PRIVATE_KEY" > "$key"
printf '%s\n' "$SFTP_KNOWN_HOSTS" > "$known_hosts"

if [[ -n "$state_override" || -n "$exit_code_override" ]]; then
  upload_file="${status_file}.publish.$$"
  jq -n \
    --slurpfile status "$status_file" \
    --arg state "${state_override:-}" \
    --arg exit_code "${exit_code_override:-}" \
    --arg timestamp "$(date -u +%FT%TZ)" \
    '($status[0] + {timestamp: $timestamp})
     | (if $state == "" then . else .state = $state end)
     | (if $exit_code == "" then . else .exitCode = ($exit_code | tonumber) end)' \
    > "$upload_file"
  jq -e -f "$SCRIPT_DIR/progress-schema.jq" "$upload_file" >/dev/null
fi

remote_run="off-progress-${run_id}-${run_attempt}.json"
remote_run_tmp="${remote_run}.part"
remote_latest_tmp="off-progress.json.part-${run_id}-${run_attempt}"
# SFTP's rename is atomic within the chroot filesystem. Both the immutable
# run/attempt name and the latest pointer are replaced only after a complete put.
printf 'cd %s\n' "$remote_dir" > "$commands"
printf 'put "%s" "%s"\n' "${upload_file//\\/\\\\}" "$remote_run_tmp" >> "$commands"
printf 'rename "%s" "%s"\n' "$remote_run_tmp" "$remote_run" >> "$commands"
printf 'put "%s" "%s"\n' "${upload_file//\\/\\\\}" "$remote_latest_tmp" >> "$commands"
printf 'rename "%s" "%s"\n' "$remote_latest_tmp" 'off-progress.json' >> "$commands"

sftp_args=(
  sftp -q -i "$key" -oIdentitiesOnly=yes -oBatchMode=yes
  -oStrictHostKeyChecking=yes -oUserKnownHostsFile="$known_hosts"
)
[[ -n "${SFTP_PORT:-}" ]] && sftp_args+=(-P "$SFTP_PORT")
sftp_args+=("${SFTP_USER}@${SFTP_HOST}")
run_sftp() {
  if command -v timeout >/dev/null 2>&1; then
    timeout --foreground --signal=TERM --kill-after=10s 45s "${sftp_args[@]}"
  else
    "${sftp_args[@]}"
  fi
}

set +e
run_sftp < "$commands"
status=$?
set -e
if [[ "$status" -ne 0 ]]; then
  # Cleanup is restricted to this run/attempt's temporary names and is best effort.
  cleanup_commands="${commands}.cleanup"
  printf 'cd %s\nrm "%s"\nrm "%s"\n' "$remote_dir" "$remote_run_tmp" "$remote_latest_tmp" > "$cleanup_commands"
  run_sftp < "$cleanup_commands" >/dev/null 2>&1 || true
  rm -f -- "$cleanup_commands"
  exit "$status"
fi
