# off-data-pipeline

Small release builder for the hosted Open Food Facts barcode lookup. It reads the official `.jsonl.gz` dump through a gzip stream, keeps only the fields used by the app, validates nutrition and barcode values, and builds a dedicated Tantivy index. The uncompressed dump is never written to disk.

Dedupe is deliberately deterministic. The reader writes bounded sorted JSONL runs (50,000 products by default), then merges those runs and keeps the highest-quality record for each barcode. Ties use the stable serialized record as a lexical tie-breaker. Peak memory is the run size plus one record per run; run files are temporary and removed after the process exits.

A release directory contains `index/` and `off-index-manifest.json`. The manifest includes schema/pipeline/dataset versions, input and normalized-stream checksums, a checksum list for every Tantivy file, and the document count. `verify` rechecks the manifest, every file checksum, required fields, and the Tantivy document count. `package` creates a compressed `.tar.zst` for the server's separate OFF area.

## Local build

```sh
cd off-data-pipeline
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked -- build \
  --input /path/to/openfoodfacts-products.jsonl.gz \
  --output /tmp/off-release \
  --dataset-version 2026-01 \
  --artifact /tmp/off-index-2026-01.tar.zst
cargo run --locked -- verify --release /tmp/off-release
```

The official dump is published at `https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz`. Check its current terms and availability before a refresh. Do not commit dumps, release directories, or credentials.

## Automation

`.github/workflows/off-data-pipeline.yml` is intentionally manual or scheduled. It streams the compressed dump to runner scratch space, checks free disk before and during download, runs the full Rust checks, verifies the release, and uploads only the compressed release over SFTP. It does not use durable GitHub artifact storage. The SFTP key and known-hosts value are injected from secrets; the server-side account should be restricted to the OFF incoming directory and have no shell access.


## Operator path

From this nested checkout, use `scripts/operator.sh preflight` for the upload-free fixture and clean-export checks. Use `scripts/operator.sh prepare` followed by the explicit `publish ... --confirm-public` checkpoint to create a standalone public repository, and `scripts/operator.sh configure-secrets` to send SFTP values/files directly to GitHub Actions secrets without printing them. The full bootstrap, restricted SFTP setup, fixture/manual workflow dispatch, activation, rollback, and external prerequisites are documented in [`../docs/off-data.md`](../docs/off-data.md).
