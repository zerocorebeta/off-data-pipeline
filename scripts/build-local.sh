#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT="${1:?usage: $0 /path/to/openfoodfacts-products.jsonl.gz [dataset-version] [output-dir] [artifact]}"
DATASET_VERSION="${2:-$(date -u +%Y-%m)}"
OUTPUT="${3:-${ROOT_DIR}/.build/off-release-${DATASET_VERSION}}"
ARTIFACT="${4:-${ROOT_DIR}/.build/off-index-${DATASET_VERSION}.tar.zst}"

cargo run --locked --manifest-path "${ROOT_DIR}/Cargo.toml" -- build \
  --input "${INPUT}" --output "${OUTPUT}" --dataset-version "${DATASET_VERSION}" --artifact "${ARTIFACT}"
cargo run --locked --manifest-path "${ROOT_DIR}/Cargo.toml" -- verify --release "${OUTPUT}"
printf 'release=%s artifact=%s\n' "${OUTPUT}" "${ARTIFACT}"
