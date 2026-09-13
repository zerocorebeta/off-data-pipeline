#!/usr/bin/env bash
set -euo pipefail

# Safe local operator entry point. It never runs deployment or writes to a
# production host; publish/configure-secrets are explicit human checkpoints.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PIPELINE_DIR}/.." && pwd)"
WORKFLOW="${PIPELINE_DIR}/.github/workflows/off-data-pipeline.yml"
ROLLBACK_SCRIPT="${REPO_ROOT}/search-service/deploy/rollback-off-index.sh"
ACTIVATE_SCRIPT="${REPO_ROOT}/search-service/deploy/activate-off-index.sh"
SFTP_SCRIPT="${REPO_ROOT}/search-service/deploy/setup-off-sftp.sh"
READ_PROGRESS_SCRIPT="${REPO_ROOT}/search-service/deploy/read-off-progress.sh"

usage() {
  cat <<USAGE
usage:
  $0 preflight
  $0 fixture
  $0 prepare EXPORT_DIR
  $0 publish OWNER/REPO EXPORT_DIR --confirm-public
  $0 configure-secrets OWNER/REPO
  $0 refresh OWNER/REPO
  $0 progress SSH_TARGET [--run-id ID --run-attempt N] [--max-age-seconds N]

prepare copies only the standalone off-data-pipeline tree into a fresh directory,
excluding VCS/build/dump output, and scans it for credentials. publish creates a
new public repository from that fresh directory; it refuses an existing repo.
configure-secrets reads values from environment variables and key files; it
never accepts credentials as command-line arguments or stores them locally.
progress invokes the installed root-only reader over an existing SSH/admin path;
it never uses the write-only SFTP account or the public HTTP service.
refresh dispatches the full GitHub Actions workflow, which downloads, builds,
verifies, packages, checksums, and uploads a new OFF index. It does not activate
the uploaded index on production.
USAGE
}

fail() { echo "off operator: $*" >&2; exit 1; }
need_command() { command -v "$1" >/dev/null || fail "missing required command: $1"; }

validate_repo() {
  [[ "${1:-}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] ||
    fail 'repository must be OWNER/NAME';
}

validate_export() {
  local dest="$1" path
  [[ -d "$dest" && ! -e "$dest/.git" ]] || fail "export must be a fresh directory without .git: $dest"
  if path="$(find "$dest" -type f \( -name '*.jsonl' -o -name '*.tar.zst' \) ! -path '*/tests/fixtures/products.jsonl.gz' -print -quit)"; then
    [[ -z "$path" ]] || fail "export contains a dump or release artifact: $path"
  fi
  if find "$dest" -type f -print0 | xargs -0 grep -IEn -m1 -E 'BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|gho_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+' >/dev/null 2>&1; then
    fail 'export contains a private key or GitHub token pattern'
  fi
}

prepare_export() {
  local requested="$1" dest rel parent
  [[ -n "$requested" ]] || fail 'EXPORT_DIR is required'
  parent="$(cd "$(dirname "$requested")" && pwd)"
  dest="${parent}/$(basename "$requested")"
  [[ "$dest" != "$PIPELINE_DIR" && "$dest" != "$PIPELINE_DIR"/* ]] ||
    fail 'EXPORT_DIR must be outside off-data-pipeline'
  [[ ! -e "$dest" ]] || fail "refusing to overwrite existing export: $dest"
  mkdir "$dest"
  while IFS= read -r -d '' path; do
    rel="${path#"$PIPELINE_DIR/"}"
    case "$rel" in
      tests/fixtures/products.jsonl.gz) ;;
      .git/*|target/*|.build/*|*.jsonl|*.jsonl.gz|*.tar.zst) continue ;;
    esac
    mkdir -p "$dest/$(dirname "$rel")"
    cp -p "$path" "$dest/$rel"
  done < <(find "$PIPELINE_DIR" -type f -not -path "$PIPELINE_DIR/.git/*" -not -path "$PIPELINE_DIR/target/*" -not -path "$PIPELINE_DIR/.build/*" -print0)
  validate_export "$dest"
  printf '%s\n' "$dest"
}

check_local_tools() {
  local command
  for command in bash cargo cp find grep git jq mktemp xargs; do need_command "$command"; done
  [[ -f "$WORKFLOW" ]] || fail "missing workflow: $WORKFLOW"
  [[ -x "$ACTIVATE_SCRIPT" ]] || fail "missing executable: $ACTIVATE_SCRIPT"
  [[ -x "$SFTP_SCRIPT" ]] || fail "missing executable: $SFTP_SCRIPT"
  [[ -x "$ROLLBACK_SCRIPT" ]] || fail "missing executable: $ROLLBACK_SCRIPT"
  [[ -x "$SCRIPT_DIR/observe-command.sh" ]] || fail "missing executable: $SCRIPT_DIR/observe-command.sh"
  [[ -x "$SCRIPT_DIR/publish-progress.sh" ]] || fail "missing executable: $SCRIPT_DIR/publish-progress.sh"
  [[ -x "$READ_PROGRESS_SCRIPT" ]] || fail "missing executable: $READ_PROGRESS_SCRIPT"
  [[ -x "$SCRIPT_DIR/progress-test.sh" ]] || fail "missing executable: $SCRIPT_DIR/progress-test.sh"
  bash -n "$SCRIPT_DIR/operator.sh" "$SCRIPT_DIR/observe-command.sh" "$SCRIPT_DIR/publish-progress.sh" "$ACTIVATE_SCRIPT" "$SFTP_SCRIPT" "$ROLLBACK_SCRIPT" "$READ_PROGRESS_SCRIPT" "$SCRIPT_DIR/progress-test.sh"
}

run_fixture() {
  local temp
  temp="$(mktemp -d "${TMPDIR:-/tmp}/off-fixture.XXXXXX")"
  trap 'rm -rf -- "$temp"' RETURN
  cargo fmt --manifest-path "$PIPELINE_DIR/Cargo.toml" --check
  cargo clippy --locked --manifest-path "$PIPELINE_DIR/Cargo.toml" --all-targets -- -D warnings
  cargo test --locked --manifest-path "$PIPELINE_DIR/Cargo.toml"
  cargo run --locked --manifest-path "$PIPELINE_DIR/Cargo.toml" -- build \
    --input "$PIPELINE_DIR/tests/fixtures/products.jsonl.gz" \
    --output "$temp/release" --dataset-version fixture-local \
    --artifact "$temp/off-index.tar.zst"
  cargo run --locked --manifest-path "$PIPELINE_DIR/Cargo.toml" -- verify --release "$temp/release"
  "$SCRIPT_DIR/progress-test.sh"
  echo 'fixture validation passed; no network or upload was performed.'
}

preflight() {
  local temp export_dir
  check_local_tools
  need_command gh
  gh auth status
  git -C "$REPO_ROOT" diff --check
  run_fixture
  temp="$(mktemp -d "${TMPDIR:-/tmp}/off-export-check.XXXXXX")"
  rmdir "$temp"
  export_dir="$(prepare_export "$temp")"
  rm -rf -- "$export_dir"
  echo 'preflight passed; external repository, secrets, SFTP, and production remain unchanged.'
}


read_progress() {
  local target="${1:-}" run_id="" run_attempt="" max_age=""
  shift || true
  [[ "$target" =~ ^[A-Za-z0-9_.:-]+@[A-Za-z0-9_.:-]+$ || "$target" =~ ^[A-Za-z0-9_.:-]+$ ]] ||
    fail 'SSH_TARGET must be a host or user@host without shell metacharacters'
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --run-id)
        [[ "${2:-}" =~ ^[A-Za-z0-9_.-]+$ ]] || fail 'run id is unsafe'
        run_id="$2"; shift 2 ;;
      --run-attempt)
        [[ "${2:-}" =~ ^[A-Za-z0-9_.-]+$ ]] || fail 'run attempt is unsafe'
        run_attempt="$2"; shift 2 ;;
      --max-age-seconds)
        [[ "${2:-}" =~ ^[0-9]+$ ]] || fail 'max age must be numeric'
        max_age="$2"; shift 2 ;;
      *) fail "unknown progress option: $1" ;;
    esac
  done
  [[ ( -z "$run_id" && -z "$run_attempt" ) || ( -n "$run_id" && -n "$run_attempt" ) ]] || fail '--run-id and --run-attempt must be supplied together'
  need_command ssh
  local remote_args=(/usr/local/sbin/macrocodex-food-search-service-read-off-progress)
  [[ -n "$run_id" ]] && remote_args+=(--run-id "$run_id" --run-attempt "$run_attempt")
  [[ -n "$max_age" ]] && remote_args+=(--max-age-seconds "$max_age")
  # All option values are constrained above; ssh receives a fixed root command.
  ssh -- "$target" sudo -n -- "${remote_args[@]}"
}

configure_secrets() {
  local repo="$1" name value file
  validate_repo "$repo"
  need_command gh
  gh auth status
  [[ "${OFF_SFTP_REMOTE_DIR:-}" == /incoming ]] || fail 'OFF_SFTP_REMOTE_DIR must be /incoming'
  [[ "${OFF_SFTP_HOST:-}" =~ ^[A-Za-z0-9._:-]+$ ]] || fail 'set OFF_SFTP_HOST to the approved host'
  [[ "${OFF_SFTP_USER:-}" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || fail 'set a safe OFF_SFTP_USER'
  [[ -z "${OFF_SFTP_PORT:-}" || "${OFF_SFTP_PORT}" =~ ^[0-9]{1,5}$ ]] || fail 'OFF_SFTP_PORT must be numeric'
  for name in OFF_SFTP_PRIVATE_KEY OFF_SFTP_KNOWN_HOSTS; do
    file="${name}_FILE"
    file="${!file:-}"
    [[ -n "$file" && -f "$file" && ! -L "$file" ]] || fail "set ${name}_FILE to a regular file"
    [[ -s "$file" ]] || fail "${name}_FILE is empty"
  done
  if ! grep -q 'BEGIN .*PRIVATE KEY' "$OFF_SFTP_PRIVATE_KEY_FILE"; then
    fail 'OFF_SFTP_PRIVATE_KEY_FILE does not look like a private key'
  fi
  for name in OFF_SFTP_HOST OFF_SFTP_USER OFF_SFTP_REMOTE_DIR OFF_SFTP_PORT; do
    value="${!name:-}"
    [[ -n "$value" ]] || { [[ "$name" == OFF_SFTP_PORT ]] && continue || fail "set $name"; }
    printf '%s' "$value" | gh secret set "$name" --repo "$repo"
  done
  gh secret set OFF_SFTP_PRIVATE_KEY --repo "$repo" < "$OFF_SFTP_PRIVATE_KEY_FILE"
  gh secret set OFF_SFTP_KNOWN_HOSTS --repo "$repo" < "$OFF_SFTP_KNOWN_HOSTS_FILE"
  echo "configured OFF Actions secrets for $repo (secret values were not printed)."
}

refresh_index() {
  local repo="$1"
  validate_repo "$repo"
  need_command gh
  gh auth status
  gh workflow run off-data-pipeline.yml --repo "$repo" -f mode=full
  echo "full OFF index refresh dispatched for $repo; activation remains a separate checkpoint."
}

publish_repo() {
  local repo="$1" export_dir="$2"
  validate_repo "$repo"
  [[ "${3:-}" == --confirm-public ]] || fail 'publishing requires --confirm-public'
  need_command gh
  need_command git
  gh auth status
  [[ -d "$export_dir" && ! -e "$export_dir/.git" ]] || fail 'publish requires a prepared fresh export directory'
  validate_export "$export_dir"
  if gh repo view "$repo" >/dev/null 2>&1; then
    fail "refusing to publish over existing repository: $repo"
  fi
  git -C "$export_dir" init -q
  git -C "$export_dir" add --all
  git -C "$export_dir" -c user.name='OFF pipeline publisher' -c user.email='noreply@github.com' commit -q -m 'Initial standalone OFF pipeline'
  git -C "$export_dir" branch -M main
  gh repo create "$repo" --public --source "$export_dir" --remote origin --push
  echo "published $repo from a fresh history containing only off-data-pipeline files."
}

command="${1:-help}"
case "$command" in
  preflight) [[ "$#" == 1 ]] || fail 'preflight takes no arguments'; preflight ;;
  fixture) [[ "$#" == 1 ]] || fail 'fixture takes no arguments'; check_local_tools; run_fixture ;;
  prepare) [[ "$#" == 2 ]] || fail 'prepare requires EXPORT_DIR'; check_local_tools; prepare_export "$2" ;;
  publish) [[ "$#" == 4 ]] || fail 'publish requires OWNER/REPO EXPORT_DIR --confirm-public'; publish_repo "$2" "$3" "$4" ;;
  configure-secrets) [[ "$#" == 2 ]] || fail 'configure-secrets requires OWNER/REPO'; configure_secrets "$2" ;;
  refresh) [[ "$#" == 2 ]] || fail 'refresh requires OWNER/REPO'; refresh_index "$2" ;;
  progress) [[ "$#" -ge 2 ]] || fail 'progress requires SSH_TARGET'; read_progress "$2" "${@:3}" ;;
  help|-h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
