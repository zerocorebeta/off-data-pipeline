#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
command -v jq >/dev/null || { echo 'progress test: missing jq' >&2; exit 1; }
command -v bash >/dev/null || { echo 'progress test: missing bash' >&2; exit 1; }

temp="$(mktemp -d "${TMPDIR:-/tmp}/off-progress-test.XXXXXX")"
cleanup() { rm -rf -- "$temp"; }
trap cleanup EXIT
status="$temp/status.json"
log="$temp/build.log"
GITHUB_RUN_ID=fixture-run GITHUB_RUN_ATTEMPT=7 \
  "$SCRIPT_DIR/observe-command.sh" "$log" --metrics-interval-seconds 5 --status-file "$status" -- \
  bash -c 'printf "%s\\n" "OFF_PROGRESS phase=ingest elapsed_ms=10 throughput_records_per_sec=10.5 compressed_bytes=2/10 input_bytes=3 input_lines=4 raw_parsed=3 accepted=2 invalid_lines=1 skipped=0 skipped_invalid_barcode=0 skipped_missing_name=0 skipped_missing_nutrition=0 skipped_validation=0 sort_runs=1 deduped_docs=0 indexed_docs=0 phase_bytes=0 phase_files=0" "OFF_PROGRESS phase=merge elapsed_ms=20 throughput_records_per_sec=20.5 compressed_bytes=10/10 input_bytes=30 input_lines=40 raw_parsed=30 accepted=20 invalid_lines=1 skipped=9 skipped_invalid_barcode=2 skipped_missing_name=3 skipped_missing_nutrition=4 skipped_validation=0 sort_runs=5 deduped_docs=18 indexed_docs=18 phase_bytes=0 phase_files=0"; sleep 6'

jq -e -f "$SCRIPT_DIR/progress-schema.jq" "$status" >/dev/null
jq -e '
  .runId == "fixture-run" and .runAttempt == "7" and .state == "success" and .phase == "merge"
  and .counters.raw_parsed == 30 and .counters.accepted == 20
  and .counters.skipped_missing_nutrition == 4 and .counters.deduped_docs == 18
  and (.resources | has("cpuPercent") and has("processes") and has("diskFreeKb"))
' "$status" >/dev/null

fakebin="$temp/bin"
mkdir -p "$fakebin"
capture="$temp/sftp.commands"
received="$temp/received.json"
cat > "$fakebin/sftp" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
: "${CAPTURE:?}"
cat > "$CAPTURE"
while IFS= read -r line; do
  if [[ "$line" == put\ * ]]; then
    source="${line#put \"}"
    source="${source%%\" \"*}"
    cp -- "$source" "$RECEIVED"
  fi
done < "$CAPTURE"
FAKE
chmod 755 "$fakebin/sftp"
export PATH="$fakebin:$PATH" CAPTURE="$capture" RECEIVED="$received"
export GITHUB_RUN_ID=fixture-run GITHUB_RUN_ATTEMPT=7
export SFTP_HOST=fixture.example SFTP_USER=fixture_u SFTP_PRIVATE_KEY='not-written-to-command-stream' SFTP_KNOWN_HOSTS='fixture.example ssh-ed25519 AAAA' SFTP_REMOTE_DIR=/incoming
"$SCRIPT_DIR/publish-progress.sh" "$status"
grep -Fq 'put "' "$capture"
grep -Fq 'off-progress-fixture-run-7.json.part" "off-progress-fixture-run-7.json"' "$capture"
grep -Fq 'off-progress.json.part-fixture-run-7" "off-progress.json"' "$capture"
! grep -Fq 'not-written-to-command-stream' "$capture"
jq -e -f "$SCRIPT_DIR/progress-schema.jq" "$received" >/dev/null

"$SCRIPT_DIR/publish-progress.sh" "$status" --state cancelled --exit-code 143
jq -e '.state == "cancelled" and .exitCode == 143' "$received" >/dev/null

bad="$temp/bad.json"
jq '. + {SFTP_PRIVATE_KEY: "must reject"}' "$status" > "$bad"
if "$SCRIPT_DIR/publish-progress.sh" "$bad" >/dev/null 2>&1; then
  echo 'progress test: secret-bearing status was accepted' >&2
  exit 1
fi

echo 'progress telemetry parser/schema/publisher test passed.'
