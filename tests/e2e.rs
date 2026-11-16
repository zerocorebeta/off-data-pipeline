use off_data_pipeline::{BuildOptions, build_release, verify_release};
use std::path::PathBuf;

#[test]
fn icon_bulk_fixture_builds_categories_without_changing_nutrition_basis() {
    use std::io::Write;
    use tantivy::schema::Value as _;
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("products.jsonl.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(&input).unwrap(),
        flate2::Compression::default(),
    );
    gzip.write_all(include_bytes!("fixtures/icon-products.jsonl"))
        .unwrap();
    gzip.finish().unwrap();
    let output = root.path().join("release");
    let manifest = build_release(&BuildOptions {
        input,
        output: output.clone(),
        artifact: None,
        dataset_version: "icons-fixture".into(),
        chunk_size: 2,
        worker_threads: 2,
        index_memory_bytes: 64 * 1024 * 1024,
    })
    .unwrap();
    assert_eq!(manifest.record_count, 6);
    let index = tantivy::Index::open_in_dir(output.join("index")).unwrap();
    let schema = index.schema();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    for (barcode, expected_tags, unit) in [
        ("99000001", "en:dairies\u{1f}en:yogurts", "g"),
        (
            "99000002",
            "en:cheeses\u{1f}en:dairies\u{1f}en:fresh-cheeses",
            "g",
        ),
        ("99000003", "en:dairies\u{1f}en:milks", "ml"),
        ("99000006", "", "g"),
    ] {
        let query = tantivy::query::TermQuery::new(
            tantivy::Term::from_field_text(schema.get_field("barcode").unwrap(), barcode),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let hits = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(1))
            .unwrap();
        let doc = searcher
            .doc::<tantivy::schema::TantivyDocument>(hits[0].1)
            .unwrap();
        let text = |key| {
            doc.get_first(schema.get_field(key).unwrap())
                .unwrap()
                .as_str()
                .unwrap()
        };
        assert_eq!(text("category_tags"), expected_tags);
        assert_eq!(text("nutrition_basis_unit"), unit);
    }
}

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
        worker_threads: 2,
        index_memory_bytes: 64 * 1024 * 1024,
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
