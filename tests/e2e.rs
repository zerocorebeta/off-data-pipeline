use off_data_pipeline::{BuildOptions, build_release, verify_release};
use std::path::PathBuf;

#[test]
fn fixture_builds_deduped_checksum_verified_release_and_archive() {
    let root = tempfile::tempdir().expect("temporary root");
    let output = root.path().join("release");
    let artifact = root.path().join("off-index.tar.zst");
    let manifest = build_release(&BuildOptions {
        input: PathBuf::from("tests/fixtures/products.jsonl.gz"),
        output: output.clone(),
        artifact: Some(artifact.clone()),
        dataset_version: "fixture-2026-01".into(),
        chunk_size: 1,
    })
    .expect("build succeeds");
    assert_eq!(manifest.record_count, 2);
    assert_eq!(manifest.input_lines, 6);
    assert_eq!(manifest.raw_parsed, 5);
    assert_eq!(manifest.accepted_products, 3);
    assert_eq!(manifest.invalid_lines, 1);
    assert_eq!(manifest.skipped_products, 2);
    assert_eq!(manifest.skipped_invalid_barcode, 1);
    assert_eq!(manifest.skipped_missing_nutrition, 1);
    assert_eq!(manifest.sort_runs, 3);
    assert_eq!(manifest.deduped_docs, 2);
    assert_eq!(manifest.indexed_docs, 2);
    assert!(artifact.is_file());
    let verified = verify_release(&output).expect("release verifies");
    assert_eq!(verified.record_count, 2);
}
