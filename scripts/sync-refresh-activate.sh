#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO="${1:-zerocorebeta/off-data-pipeline}"
REMOTE_HOST="${2:-root@178.104.130.88}"
WORKFLOW="off-data-pipeline.yml"
ACTIVATOR="/usr/local/sbin/macrocodex-food-search-service-activate-off-index"
INCOMING="/var/lib/macrocodex-food-search-service/off/incoming"
STATUS_URL="https://search.macrocodex.app/v1/status"
VERIFY_BARCODE="${VERIFY_BARCODE:-8906007283120}"

fail() { echo "OFF release: $*" >&2; exit 1; }
for command in curl gh git jq mktemp rsync ssh; do
  command -v "$command" >/dev/null 2>&1 || fail "missing command: $command"
done
[[ "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail 'unsafe GitHub repository'
[[ "$REMOTE_HOST" =~ ^[A-Za-z0-9_.:-]+@[A-Za-z0-9_.:-]+$ ]] || fail 'unsafe SSH target'

if [[ -z "${GH_TOKEN:-}" ]]; then
  # Bootstrap from an existing gh login once, then force all subsequent gh
  # calls through the environment so the release flow itself is headless.
  GH_TOKEN="$(gh auth token 2>/dev/null)" || fail 'set GH_TOKEN or authenticate gh once'
  export GH_TOKEN
fi
gh api user --jq .login >/dev/null
workdir="$(mktemp -d "${TMPDIR:-/tmp}/off-release.XXXXXX")"
cleanup() { rm -rf -- "$workdir"; }
trap cleanup EXIT

gh repo clone "$REPO" "$workdir/repo" -- --quiet
rsync -a --delete \
  --exclude='.git/' --exclude='target/' --exclude='.build/' \
  "$PIPELINE_DIR/" "$workdir/repo/"
git -C "$workdir/repo" diff --check
git -C "$workdir/repo" add --all
if ! git -C "$workdir/repo" diff --cached --quiet; then
  git -C "$workdir/repo" \
    -c user.name='MacroCodex OFF release' \
    -c user.email='noreply@macrocodex.app' \
    commit -m 'Refresh OFF pipeline and importer'
  git -C "$workdir/repo" push origin HEAD:main
fi

head_sha="$(git -C "$workdir/repo" rev-parse HEAD)"
dataset="$(date -u +%Y-%m-%d-%H%M%S)"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
gh workflow run "$WORKFLOW" --repo "$REPO" --ref main \
  -f mode=full -f dataset_version="$dataset" -f trace_barcode="$VERIFY_BARCODE"

run_id=''
for _ in $(seq 1 30); do
  run_id="$(gh run list --repo "$REPO" --workflow "$WORKFLOW" \
    --event workflow_dispatch --limit 20 \
    --json databaseId,headSha,createdAt \
    --jq "map(select(.headSha == \"$head_sha\" and .createdAt >= \"$started_at\"))[0].databaseId // empty")"
  [[ -n "$run_id" ]] && break
  sleep 2
done
[[ -n "$run_id" ]] || fail 'could not resolve the dispatched workflow run'
echo "Watching GitHub workflow run $run_id..."
gh run watch "$run_id" --repo "$REPO" --exit-status

attempt="$(gh api "repos/$REPO/actions/runs/$run_id" --jq '.run_attempt')"
artifact="off-index-${dataset}-${run_id}-${attempt}.tar.zst"
artifact_path="${INCOMING}/${artifact}"
echo "Activating ${artifact_path} on ${REMOTE_HOST}..."
ssh -- "$REMOTE_HOST" "$ACTIVATOR '$artifact_path' '${artifact_path}.sha256'"

status="$(curl --fail --silent --show-error --retry 5 --retry-delay 2 "$STATUS_URL")"
printf '%s\n' "$status" | jq -e '.off.available == true or .offAvailable == true' >/dev/null || {
  printf '%s\n' "$status" >&2
  fail 'OFF status did not report available'
}
curl --fail --silent --show-error --retry 5 --retry-delay 2 \
  "https://search.macrocodex.app/v1/products/barcode/${VERIFY_BARCODE}" | jq -e . >/dev/null
echo "OFF release ${dataset} is active; barcode ${VERIFY_BARCODE} is available."
