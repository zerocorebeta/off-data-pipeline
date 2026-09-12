# off-data-pipeline

Small release builder for the hosted Open Food Facts barcode lookup. It reads the official `.jsonl.gz` dump through a gzip stream, keeps only the fields used by the app, validates nutrition and barcode values, and builds a dedicated Tantivy index. The uncompressed dump is never written to disk.

Dedupe is deliberately deterministic. The reader writes bounded sorted JSONL runs (50,000 products by default), then merges those runs and keeps the highest-quality record for each barcode. Ties use the stable serialized record as a lexical tie-breaker. Peak memory is the run size plus one record per run; run files are temporary and removed after the process exits.

A release directory contains `index/` and `off-index-manifest.json`. The manifest includes schema/pipeline/dataset versions, input and normalized-stream checksums, a checksum list for every Tantivy file, and the document count. It also records parsed/accepted/skipped counters, skip reasons, external-sort run count, compressed input bytes, and deduped/indexed document counts. `verify` rechecks the manifest, every file checksum, required fields, and the Tantivy document count. `package` creates a compressed `.tar.zst` for the server's separate OFF area.

Build, verify, and package commands emit flushed `OFF_PROGRESS` records to stderr. By default they report every 100,000 records and every 30 seconds; tune this with `--progress-records`, `--progress-interval-ms`, or `--progress-interval-seconds` (the corresponding `OFF_PROGRESS_*` environment variables are also supported). Heartbeats continue during Tantivy commit, hashing, verification, and archive compression.

## Local build

```sh
cd off-data-pipeline
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked -- build \
  --input /path/to/openfoodfacts-products.jsonl.gz \
  --output /tmp/off-release \
  --dataset-version 2026-01 --worker-threads 4 --index-memory-mb 2048 \
  --artifact /tmp/off-index-2026-01.tar.zst
cargo run --locked -- verify --release /tmp/off-release
```

The official dump is published at `https://openfoodfacts-ds.s3.eu-west-3.amazonaws.com/openfoodfacts-products.jsonl.gz`. Check its current terms and availability before a refresh. Do not commit dumps, release directories, or credentials.

## Automation

`.github/workflows/off-data-pipeline.yml` is intentionally manual or scheduled. It downloads the compressed dump from the OFF S3 endpoint with 16-way `aria2c` range transfers, checks free disk before and during download, compiles once in release mode, uses every runner CPU for parse/sort/index work, and gives Tantivy a 2 GiB memory budget. The separate `ci.yml` workflow runs formatting, clippy, and tests for pushes and pull requests. The release builder verifies the index before packaging and uploads only the compressed release over SFTP. The observer also logs bounded runner disk, memory, load/CPU, and pipeline process snapshots while commands run; the manifest counters are copied to `GITHUB_STEP_SUMMARY`. Full runs atomically replace the dedicated `/incoming/off-progress.json` heartbeat with run id/attempt, counters, and resources; this name is not an archive or checksum and cannot be selected by activation. Job and command timeouts prevent an opaque hang. It does not use durable GitHub artifact storage. The SFTP key and known-hosts value are injected from secrets; the server-side account should be restricted to the OFF incoming directory and have no shell access.


## Operator path

From this nested checkout, use `scripts/operator.sh preflight` for the upload-free fixture and clean-export checks. Use `scripts/operator.sh prepare` followed by the explicit `publish ... --confirm-public` checkpoint to create a standalone public repository, and `scripts/operator.sh configure-secrets` to send SFTP values/files directly to GitHub Actions secrets without printing them. The full bootstrap, restricted SFTP setup, fixture/manual workflow dispatch, activation, rollback, and external prerequisites are documented in [`../docs/off-data.md`](../docs/off-data.md).
