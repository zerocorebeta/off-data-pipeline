//! Streaming Open Food Facts dump normalisation and release packaging.
//!
//! The input is consumed through `flate2::MultiGzDecoder`; the uncompressed dump
//! is never materialised. Dedupe uses sorted temporary JSONL runs and a k-way
//! merge, so memory is bounded by `chunk_size` plus one record per run.

use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Component, Path, PathBuf};
use tantivy::collector::Count;
use tantivy::schema::{Field, STORED, STRING, TEXT};
use tantivy::{Index, doc};
use thiserror::Error;

pub const INDEX_SCHEMA_VERSION: u32 = 1;
pub const SOURCE: &str = "openfoodfacts";
pub const MANIFEST_FILE: &str = "off-index-manifest.json";
pub const DEFAULT_CHUNK_SIZE: usize = 50_000;
const MAX_LINE_BYTES: usize = 16 * 1024 * 1024;

fn hex_encode<T: AsRef<[u8]>>(bytes: T) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .as_ref()
        .iter()
        .flat_map(|byte| [HEX[(byte >> 4) as usize], HEX[(byte & 0x0f) as usize]])
        .map(char::from)
        .collect()
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Tantivy: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("invalid release: {0}")]
    InvalidRelease(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OffProduct {
    pub product_id: String,
    pub barcode: String,
    pub name: String,
    pub brand: String,
    pub generic_name: String,
    pub energy_kcal_100g: Option<f64>,
    pub protein_100g: Option<f64>,
    pub carbohydrates_100g: Option<f64>,
    pub fat_100g: Option<f64>,
    pub fiber_100g: Option<f64>,
    pub sugars_100g: Option<f64>,
    pub saturated_fat_100g: Option<f64>,
    pub salt_100g: Option<f64>,
    pub serving_size: String,
    pub serving_quantity: Option<f64>,
    pub quantity: String,
    pub countries: Vec<String>,
    pub languages: Vec<String>,
    pub image_url: String,
    pub off_product_id: String,
    pub off_last_modified: Option<i64>,
    pub source: String,
}

impl OffProduct {
    pub fn validate(&self) -> Result<(), String> {
        if self.product_id.trim().is_empty()
            || self.off_product_id.trim().is_empty()
            || self.barcode.trim().is_empty()
            || self.name.trim().is_empty()
        {
            return Err("product_id, off_product_id, barcode, and name are required".into());
        }
        if !valid_barcode(&self.barcode) {
            return Err("barcode must contain 8-32 digits".into());
        }
        if self.source != SOURCE {
            return Err("source must be openfoodfacts".into());
        }
        let numbers = [
            ("energy_kcal_100g", self.energy_kcal_100g),
            ("protein_100g", self.protein_100g),
            ("carbohydrates_100g", self.carbohydrates_100g),
            ("fat_100g", self.fat_100g),
            ("fiber_100g", self.fiber_100g),
            ("sugars_100g", self.sugars_100g),
            ("saturated_fat_100g", self.saturated_fat_100g),
            ("salt_100g", self.salt_100g),
            ("serving_quantity", self.serving_quantity),
        ];
        if !numbers.iter().any(|(_, value)| value.is_some()) {
            return Err("at least one nutrition value is required".into());
        }
        for (name, value) in numbers {
            if let Some(value) = value
                && (!value.is_finite() || value < 0.0)
            {
                return Err(format!("{name} must be finite and non-negative"));
            }
        }
        if self.countries.len() > 256 || self.languages.len() > 256 {
            return Err("country/language lists are too large".into());
        }
        Ok(())
    }

    fn quality_rank(&self) -> usize {
        let strings = [
            self.name.as_str(),
            self.brand.as_str(),
            self.generic_name.as_str(),
            self.serving_size.as_str(),
            self.quantity.as_str(),
            self.image_url.as_str(),
        ];
        strings
            .iter()
            .filter(|value| !value.trim().is_empty())
            .count()
            + self.countries.len()
            + self.languages.len()
            + [
                self.energy_kcal_100g,
                self.protein_100g,
                self.carbohydrates_100g,
                self.fat_100g,
                self.fiber_100g,
                self.sugars_100g,
                self.saturated_fat_100g,
                self.salt_100g,
            ]
            .iter()
            .filter(|value| value.is_some())
            .count()
    }

    fn canonical_json(&self) -> String {
        // Struct field order is fixed by declaration order, making this a
        // stable tie-breaker across runs and input ordering.
        serde_json::to_string(self).expect("OffProduct is serializable")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema_version: u32,
    pub pipeline_version: String,
    pub dataset_version: String,
    pub source: String,
    pub record_count: usize,
    pub input_sha256: String,
    pub normalized_sha256: String,
    pub index_sha256: String,
    pub input_lines: u64,
    pub invalid_lines: u64,
    pub skipped_products: u64,
    pub files: Vec<ManifestFile>,
}

#[derive(Clone, Debug)]
pub struct BuildOptions {
    pub input: PathBuf,
    pub output: PathBuf,
    pub artifact: Option<PathBuf>,
    pub dataset_version: String,
    pub chunk_size: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildStats {
    pub input_lines: u64,
    pub invalid_lines: u64,
    pub skipped_products: u64,
    pub record_count: usize,
}

#[derive(Clone, Copy)]
struct IndexFields {
    product_id: Field,
    barcode: Field,
    name: Field,
    name_exact: Field,
    brand: Field,
    brand_exact: Field,
    generic_name: Field,
    energy_kcal_100g: Field,
    protein_100g: Field,
    carbohydrates_100g: Field,
    fat_100g: Field,
    fiber_100g: Field,
    sugars_100g: Field,
    saturated_fat_100g: Field,
    salt_100g: Field,
    serving_size: Field,
    serving_quantity: Field,
    quantity: Field,
    countries: Field,
    languages: Field,
    image_url: Field,
    off_product_id: Field,
    off_last_modified: Field,
    source: Field,
}

fn build_schema() -> (tantivy::schema::Schema, IndexFields) {
    let mut b = tantivy::schema::Schema::builder();
    let product_id = b.add_text_field("product_id", STORED | STRING);
    let barcode = b.add_text_field("barcode", STORED | STRING);
    let name = b.add_text_field("name", STORED | TEXT);
    let name_exact = b.add_text_field("name_exact", STORED | STRING);
    let brand = b.add_text_field("brand", STORED | TEXT);
    let brand_exact = b.add_text_field("brand_exact", STORED | STRING);
    let generic_name = b.add_text_field("generic_name", STORED | TEXT);
    let energy_kcal_100g = b.add_f64_field("energy_kcal_100g", STORED);
    let protein_100g = b.add_f64_field("protein_100g", STORED);
    let carbohydrates_100g = b.add_f64_field("carbohydrates_100g", STORED);
    let fat_100g = b.add_f64_field("fat_100g", STORED);
    let fiber_100g = b.add_f64_field("fiber_100g", STORED);
    let sugars_100g = b.add_f64_field("sugars_100g", STORED);
    let saturated_fat_100g = b.add_f64_field("saturated_fat_100g", STORED);
    let salt_100g = b.add_f64_field("salt_100g", STORED);
    let serving_size = b.add_text_field("serving_size", STORED);
    let serving_quantity = b.add_f64_field("serving_quantity", STORED);
    let quantity = b.add_text_field("quantity", STORED);
    let countries = b.add_text_field("countries", STORED);
    let languages = b.add_text_field("languages", STORED);
    let image_url = b.add_text_field("image_url", STORED);
    let off_product_id = b.add_text_field("off_product_id", STORED | STRING);
    let off_last_modified = b.add_i64_field("off_last_modified", STORED);
    let source = b.add_text_field("source", STORED | STRING);
    let schema = b.build();
    (
        schema,
        IndexFields {
            product_id,
            barcode,
            name,
            name_exact,
            brand,
            brand_exact,
            generic_name,
            energy_kcal_100g,
            protein_100g,
            carbohydrates_100g,
            fat_100g,
            fiber_100g,
            sugars_100g,
            saturated_fat_100g,
            salt_100g,
            serving_size,
            serving_quantity,
            quantity,
            countries,
            languages,
            image_url,
            off_product_id,
            off_last_modified,
            source,
        },
    )
}

fn add_product(
    writer: &mut tantivy::IndexWriter,
    f: IndexFields,
    product: &OffProduct,
) -> Result<(), PipelineError> {
    let join = |values: &[String]| values.join("\u{1f}");
    let mut document = doc!(
        f.product_id => product.product_id.clone(),
        f.barcode => product.barcode.clone(),
        f.name => product.name.clone(),
        f.name_exact => product.name.to_ascii_lowercase(),
        f.brand => product.brand.clone(),
        f.brand_exact => product.brand.to_ascii_lowercase(),
        f.generic_name => product.generic_name.clone(),
        f.serving_size => product.serving_size.clone(),
        f.quantity => product.quantity.clone(),
        f.countries => join(&product.countries),
        f.languages => join(&product.languages),
        f.image_url => product.image_url.clone(),
        f.off_product_id => product.off_product_id.clone(),
        f.source => product.source.clone(),
    );
    for (field, value) in [
        (f.energy_kcal_100g, product.energy_kcal_100g),
        (f.protein_100g, product.protein_100g),
        (f.carbohydrates_100g, product.carbohydrates_100g),
        (f.fat_100g, product.fat_100g),
        (f.fiber_100g, product.fiber_100g),
        (f.sugars_100g, product.sugars_100g),
        (f.saturated_fat_100g, product.saturated_fat_100g),
        (f.salt_100g, product.salt_100g),
        (f.serving_quantity, product.serving_quantity),
    ] {
        if let Some(value) = value {
            document.add_f64(field, value);
        }
    }
    if let Some(value) = product.off_last_modified {
        document.add_i64(f.off_last_modified, value);
    }
    writer.add_document(document)?;
    Ok(())
}

pub fn valid_barcode(value: &str) -> bool {
    let value = value.trim();
    (8..=32).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn string_value(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| match value {
            Value::String(value) => Some(value.trim().to_owned()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn number_value(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| value.get(*key))
        .and_then(|value| {
            let parsed = match value {
                Value::Number(value) => value.as_f64(),
                Value::String(value) => value.trim().parse::<f64>().ok(),
                _ => None,
            }?;
            parsed.is_finite().then_some(parsed)
        })
}

fn non_negative_number(value: &Value, keys: &[&str]) -> Option<f64> {
    number_value(value, keys).filter(|value| *value >= 0.0)
}

fn list_value(value: &Value, keys: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    for key in keys {
        let Some(raw) = value.get(*key) else { continue };
        match raw {
            Value::Array(items) => values.extend(items.iter().filter_map(|item| match item {
                Value::String(item) => Some(item.clone()),
                Value::Number(item) => Some(item.to_string()),
                _ => None,
            })),
            Value::String(item) => values.extend(item.split([',', ';']).map(str::to_owned)),
            _ => {}
        }
        if !values.is_empty() {
            break;
        }
    }
    values
        .into_iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalized_number(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

fn country_values(value: &Value) -> Vec<String> {
    list_value(value, &["countries_tags", "countries"])
        .into_iter()
        .map(|country| {
            let bytes = country.as_bytes();
            if bytes.len() > 3
                && bytes[2] == b':'
                && bytes[..2].iter().all(|byte| byte.is_ascii_lowercase())
            {
                country[3..].to_owned()
            } else {
                country
            }
        })
        .filter(|country| !country.is_empty())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Convert one raw OFF object into the stable, intentionally small product model.
/// Products with no usable name, barcode, or nutrition are omitted.
pub fn normalize_product(raw: &Value) -> Option<OffProduct> {
    let barcode = string_value(raw, &["code", "barcode", "_id"]);
    if !valid_barcode(&barcode) {
        return None;
    }
    let generic_name = string_value(raw, &["generic_name", "generic_name_en"]);
    let name = string_value(
        raw,
        &["product_name", "product_name_en", "product_name:en", "name"],
    );
    let name = if name.is_empty() {
        generic_name.clone()
    } else {
        name
    };
    if name.is_empty() {
        return None;
    }
    let nutriments = raw.get("nutriments").unwrap_or(raw);
    let energy_kcal_100g = non_negative_number(
        nutriments,
        &["energy-kcal_100g", "energy-kcal_value", "energy-kcal"],
    )
    .or_else(|| {
        non_negative_number(nutriments, &["energy-kj_100g", "energy-kj"]).map(|value| value / 4.184)
    });
    let protein_100g =
        non_negative_number(nutriments, &["proteins_100g", "protein_100g", "proteins"]);
    let carbohydrates_100g = non_negative_number(
        nutriments,
        &["carbohydrates_100g", "carbohydrates_value", "carbohydrates"],
    );
    let fat_100g = non_negative_number(nutriments, &["fat_100g", "fat_value", "fat"]);
    let fiber_100g = non_negative_number(nutriments, &["fiber_100g", "fiber"]);
    let sugars_100g = non_negative_number(nutriments, &["sugars_100g", "sugars"]);
    let saturated_fat_100g = non_negative_number(
        nutriments,
        &["saturated-fat_100g", "saturated_fat_100g", "saturated-fat"],
    );
    let salt_100g = non_negative_number(nutriments, &["salt_100g", "salt"]);
    if [
        energy_kcal_100g,
        protein_100g,
        carbohydrates_100g,
        fat_100g,
        fiber_100g,
        sugars_100g,
        saturated_fat_100g,
        salt_100g,
    ]
    .iter()
    .all(Option::is_none)
    {
        return None;
    }
    let off_product_id = string_value(raw, &["_id", "id", "code"]);
    let off_product_id = if off_product_id.is_empty() {
        barcode.clone()
    } else {
        off_product_id
    };
    let serving_quantity = normalized_number(number_value(
        raw,
        &["serving_quantity", "serving_quantity_value"],
    ));
    let product = OffProduct {
        product_id: off_product_id.clone(),
        barcode: barcode.clone(),
        name,
        brand: string_value(raw, &["brands", "brand_owner", "brand"]),
        generic_name,
        energy_kcal_100g,
        protein_100g,
        carbohydrates_100g,
        fat_100g,
        fiber_100g,
        sugars_100g,
        saturated_fat_100g,
        salt_100g,
        serving_size: string_value(raw, &["serving_size", "serving_size_en"]),
        serving_quantity,
        quantity: string_value(raw, &["quantity"]),
        countries: country_values(raw),
        languages: list_value(raw, &["languages_codes", "languages"]),
        image_url: string_value(raw, &["image_front_url", "image_front_small_url"]),
        off_product_id,
        off_last_modified: number_value(raw, &["last_modified_t", "last_modified"])
            .map(|value| value as i64),
        source: SOURCE.into(),
    };
    product.validate().ok()?;
    Some(product)
}

fn product_sort(a: &OffProduct, b: &OffProduct) -> Ordering {
    a.barcode
        .cmp(&b.barcode)
        .then_with(|| b.quality_rank().cmp(&a.quality_rank()))
        .then_with(|| a.canonical_json().cmp(&b.canonical_json()))
}

fn hash_file(path: &Path) -> Result<String, PipelineError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_encode(hasher.finalize()))
}

fn write_run(path: &Path, products: &mut Vec<OffProduct>) -> Result<(), PipelineError> {
    products.sort_by(product_sort);
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for product in products.drain(..) {
        serde_json::to_writer(&mut writer, &product)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn next_product(reader: &mut BufReader<File>) -> Result<Option<OffProduct>, PipelineError> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            continue;
        }
        return Ok(Some(serde_json::from_str(line.trim_end())?));
    }
}

struct HeapItem {
    sort_key: String,
    run: usize,
    product: OffProduct,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key && self.run == other.run
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is max-first; reverse to pop the lexicographically
        // smallest product_sort key first.
        other
            .sort_key
            .cmp(&self.sort_key)
            .then_with(|| other.run.cmp(&self.run))
    }
}

fn product_sort_key(product: &OffProduct) -> String {
    format!(
        "{}\0{:020}\0{}",
        product.barcode,
        usize::MAX - product.quality_rank(),
        product.canonical_json()
    )
}

fn discard_until_newline<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(());
        }
        if let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            reader.consume(position + 1);
            return Ok(());
        }
        let length = buffer.len();
        reader.consume(length);
    }
}

/// Read at most `MAX_LINE_BYTES` plus one sentinel byte, then discard an
/// oversized line before returning. This keeps a malformed input line from
/// defeating the otherwise bounded-memory ingestion strategy.
fn read_bounded_line<R: BufRead>(reader: &mut R, line: &mut String) -> io::Result<usize> {
    let mut bytes = Vec::with_capacity(MAX_LINE_BYTES.min(8 * 1024));
    let mut too_long = false;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            break;
        }
        if let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
            let length = position + 1;
            if !too_long {
                let remaining = MAX_LINE_BYTES + 1 - bytes.len();
                bytes.extend_from_slice(&buffer[..length.min(remaining)]);
                too_long = bytes.len() > MAX_LINE_BYTES;
            }
            reader.consume(length);
            break;
        }
        if !too_long {
            let remaining = MAX_LINE_BYTES + 1 - bytes.len();
            bytes.extend_from_slice(&buffer[..buffer.len().min(remaining)]);
            too_long = bytes.len() > MAX_LINE_BYTES;
        }
        let length = buffer.len();
        reader.consume(length);
        if too_long {
            discard_until_newline(reader)?;
            return Ok(MAX_LINE_BYTES + 1);
        }
    }
    if too_long {
        return Ok(MAX_LINE_BYTES + 1);
    }
    if bytes.is_empty() {
        return Ok(0);
    }
    *line = String::from_utf8(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(line.len())
}

fn collect_runs(
    input: &Path,
    temp: &Path,
    chunk_size: usize,
) -> Result<(Vec<PathBuf>, BuildStats), PipelineError> {
    let input_file = File::open(input)?;
    let decoder = MultiGzDecoder::new(BufReader::new(input_file));
    let mut reader = BufReader::new(decoder);
    let mut products = Vec::with_capacity(chunk_size);
    let mut runs = Vec::new();
    let mut stats = BuildStats::default();
    let mut line = String::new();
    loop {
        let line_len = read_bounded_line(&mut reader, &mut line)?;
        if line_len == 0 {
            break;
        }
        stats.input_lines += 1;
        if line_len > MAX_LINE_BYTES {
            stats.invalid_lines += 1;
            continue;
        }
        let raw = match serde_json::from_str::<Value>(line.trim_end()) {
            Ok(value) => value,
            Err(_) => {
                stats.invalid_lines += 1;
                continue;
            }
        };
        if let Some(product) = normalize_product(&raw) {
            products.push(product);
            if products.len() >= chunk_size {
                let path = temp.join(format!("run-{:08}.jsonl", runs.len()));
                write_run(&path, &mut products)?;
                runs.push(path);
            }
        } else {
            stats.skipped_products += 1;
        }
    }
    if !products.is_empty() {
        let path = temp.join(format!("run-{:08}.jsonl", runs.len()));
        write_run(&path, &mut products)?;
        runs.push(path);
    }
    Ok((runs, stats))
}

fn merge_runs(
    runs: &[PathBuf],
    index: &Index,
    fields: IndexFields,
) -> Result<(usize, String), PipelineError> {
    let mut readers = runs
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(product) = next_product(reader)? {
            heap.push(HeapItem {
                sort_key: product_sort_key(&product),
                run,
                product,
            });
        }
    }
    let mut writer = index.writer(128_000_000)?;
    let mut count = 0;
    let mut normalized_hasher = Sha256::new();
    while let Some(item) = heap.pop() {
        let barcode = item.product.barcode.clone();
        let selected = item.product;
        let canonical = serde_json::to_vec(&selected)?;
        normalized_hasher.update(&canonical);
        normalized_hasher.update(b"\n");
        add_product(&mut writer, fields, &selected)?;
        count += 1;
        if let Some(product) = next_product(&mut readers[item.run])? {
            heap.push(HeapItem {
                sort_key: product_sort_key(&product),
                run: item.run,
                product,
            });
        }
        // A run is sorted by barcode and quality. The heap contains the best
        // candidate first; discard every remaining candidate with this key,
        // regardless of which run produced it.
        while let Some(next) = heap.peek() {
            if next.product.barcode != barcode {
                break;
            }
            let duplicate = heap.pop().expect("peeked heap item");
            if let Some(product) = next_product(&mut readers[duplicate.run])? {
                heap.push(HeapItem {
                    sort_key: product_sort_key(&product),
                    run: duplicate.run,
                    product,
                });
            }
        }
    }
    writer.commit()?;
    writer.wait_merging_threads()?;
    Ok((count, hex_encode(normalized_hasher.finalize())))
}

fn collect_index_files(index_root: &Path) -> Result<Vec<ManifestFile>, PipelineError> {
    fn visit(
        root: &Path,
        current: &Path,
        files: &mut Vec<ManifestFile>,
    ) -> Result<(), PipelineError> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else if path.is_file() {
                let relative = path.strip_prefix(root).map_err(|_| {
                    PipelineError::InvalidRelease("file escaped release root".into())
                })?;
                let path_string = relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                let metadata = fs::metadata(&path)?;
                files.push(ManifestFile {
                    path: path_string,
                    bytes: metadata.len(),
                    sha256: hash_file(&path)?,
                });
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(index_root, index_root, &mut files)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

fn files_checksum(files: &[ManifestFile]) -> String {
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.path.as_bytes());
        hasher.update([0]);
        hasher.update(file.bytes.to_string().as_bytes());
        hasher.update([0]);
        hasher.update(file.sha256.as_bytes());
        hasher.update(b"\n");
    }
    hex_encode(hasher.finalize())
}

fn safe_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_dataset_version(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphanumeric())
        && value.len() <= 64
        && chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
}

/// Build a complete release root and optionally a `.tar.zst` artifact.
pub fn build_release(options: &BuildOptions) -> Result<Manifest, PipelineError> {
    if options.chunk_size == 0 {
        return Err(PipelineError::InvalidRelease(
            "chunk size must be positive".into(),
        ));
    }
    if !valid_dataset_version(&options.dataset_version) {
        return Err(PipelineError::InvalidRelease(
            "dataset version must be 1-64 ASCII letters, digits, '.', '_' or '-' and start with a letter/digit".into(),
        ));
    }
    if !options.input.is_file() {
        return Err(PipelineError::InvalidRelease(format!(
            "input is not a file: {}",
            options.input.display()
        )));
    }
    if options.output.exists() {
        fs::remove_dir_all(&options.output)?;
    }
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir_all(options.output.join("index"))?;
    let temporary =
        tempfile::tempdir_in(options.output.parent().unwrap_or_else(|| Path::new(".")))?;
    let (runs, mut stats) = collect_runs(&options.input, temporary.path(), options.chunk_size)?;
    let (schema, fields) = build_schema();
    let index = Index::create_in_dir(options.output.join("index"), schema)?;
    let (count, normalized_sha256) = merge_runs(&runs, &index, fields)?;
    stats.record_count = count;
    // These locks belong to the build process, not the read-mostly release.
    // Leaving them in the archive can make a root-built release unwritable by
    // the service account on the first startup.
    for lock in [".tantivy-meta.lock", ".tantivy-writer.lock"] {
        let _ = fs::remove_file(options.output.join("index").join(lock));
    }
    let files = collect_index_files(&options.output.join("index"))?;
    let manifest = Manifest {
        schema_version: INDEX_SCHEMA_VERSION,
        pipeline_version: env!("CARGO_PKG_VERSION").into(),
        dataset_version: options.dataset_version.clone(),
        source: SOURCE.into(),
        record_count: count,
        input_sha256: hash_file(&options.input)?,
        normalized_sha256,
        index_sha256: files_checksum(&files),
        input_lines: stats.input_lines,
        invalid_lines: stats.invalid_lines,
        skipped_products: stats.skipped_products,
        files,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    fs::write(options.output.join(MANIFEST_FILE), manifest_bytes)?;
    verify_release(&options.output)?;
    if let Some(artifact) = &options.artifact {
        package_release(&options.output, artifact)?;
    }
    Ok(manifest)
}

/// Validate manifest checksums, schema, and Tantivy document count.
pub fn verify_release(root: &Path) -> Result<Manifest, PipelineError> {
    let manifest_path = root.join(MANIFEST_FILE);
    let manifest: Manifest = serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
    if manifest.schema_version != INDEX_SCHEMA_VERSION || manifest.source != SOURCE {
        return Err(PipelineError::InvalidRelease(
            "unsupported OFF manifest schema or source".into(),
        ));
    }
    if manifest.pipeline_version.trim().is_empty()
        || !valid_dataset_version(&manifest.dataset_version)
        || manifest.record_count == 0
    {
        return Err(PipelineError::InvalidRelease(
            "manifest has incomplete release metadata".into(),
        ));
    }
    let index_root = root.join("index");
    let index_metadata = fs::symlink_metadata(&index_root)
        .map_err(|_| PipelineError::InvalidRelease("release index directory is missing".into()))?;
    if !index_metadata.file_type().is_dir() {
        return Err(PipelineError::InvalidRelease(
            "release index path is not a directory".into(),
        ));
    }
    if !valid_sha256(&manifest.input_sha256)
        || !valid_sha256(&manifest.normalized_sha256)
        || !valid_sha256(&manifest.index_sha256)
        || manifest
            .files
            .iter()
            .any(|file| !valid_sha256(&file.sha256) || !safe_relative_path(&file.path))
    {
        return Err(PipelineError::InvalidRelease(
            "manifest contains an invalid checksum or unsafe file path".into(),
        ));
    }
    if files_checksum(&manifest.files) != manifest.index_sha256 {
        return Err(PipelineError::InvalidRelease(
            "index file checksum list mismatch".into(),
        ));
    }
    for file in &manifest.files {
        let path = root.join("index").join(&file.path);
        let metadata = fs::symlink_metadata(&path).map_err(|_| {
            PipelineError::InvalidRelease(format!("manifest file is missing: {}", file.path))
        })?;
        if !metadata.file_type().is_file() {
            return Err(PipelineError::InvalidRelease(format!(
                "manifest file is not regular: {}",
                file.path
            )));
        }
        if metadata.len() != file.bytes || hash_file(&path)? != file.sha256 {
            return Err(PipelineError::InvalidRelease(format!(
                "checksum mismatch: {}",
                file.path
            )));
        }
    }
    let index = Index::open_in_dir(root.join("index"))?;
    let schema = index.schema();
    for field in [
        "product_id",
        "barcode",
        "name",
        "name_exact",
        "brand",
        "brand_exact",
        "generic_name",
        "energy_kcal_100g",
        "protein_100g",
        "carbohydrates_100g",
        "fat_100g",
        "fiber_100g",
        "sugars_100g",
        "saturated_fat_100g",
        "salt_100g",
        "serving_size",
        "serving_quantity",
        "quantity",
        "countries",
        "languages",
        "image_url",
        "off_product_id",
        "off_last_modified",
        "source",
    ] {
        schema
            .get_field(field)
            .map_err(|_| PipelineError::InvalidRelease(format!("missing index field: {field}")))?;
    }
    let count = index
        .reader()?
        .searcher()
        .search(&tantivy::query::AllQuery, &Count)?;
    if count != manifest.record_count {
        return Err(PipelineError::InvalidRelease(format!(
            "Tantivy document count {} != manifest {}",
            count, manifest.record_count
        )));
    }
    Ok(manifest)
}

/// Write one POSIX ustar header. The release file names are intentionally
/// short, so the classic 100-byte name field is sufficient.
fn tar_header(name: &str, bytes: u64, mode: u32, typeflag: u8) -> Result<[u8; 512], PipelineError> {
    let name_bytes = name.as_bytes();
    if name_bytes.len() > 100 {
        return Err(PipelineError::InvalidRelease(format!(
            "archive path is too long: {name}"
        )));
    }
    let mut header = [0_u8; 512];
    header[..name_bytes.len()].copy_from_slice(name_bytes);
    fn octal(field: &mut [u8], value: u64) {
        field.fill(b'0');
        let text = format!("{value:o}");
        let start = field.len().saturating_sub(text.len() + 1);
        field[start..start + text.len()].copy_from_slice(text.as_bytes());
        field[field.len() - 1] = 0;
    }
    octal(&mut header[100..108], mode as u64);
    octal(&mut header[108..116], 0);
    octal(&mut header[116..124], 0);
    octal(&mut header[124..136], bytes);
    octal(&mut header[136..148], 0);
    header[148..156].fill(b' ');
    header[156] = typeflag;
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum: u32 = header.iter().map(|value| *value as u32).sum();
    let checksum_text = format!("{checksum:06o}\0 ");
    header[148..156].copy_from_slice(checksum_text.as_bytes());
    Ok(header)
}

fn append_archive_file<W: Write>(
    archive: &mut W,
    path: &Path,
    name: &str,
) -> Result<(), PipelineError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(PipelineError::InvalidRelease(format!(
            "archive source is not a regular file: {}",
            path.display()
        )));
    }
    let header = tar_header(name, metadata.len(), 0o640, b'0')?;
    archive.write_all(&header)?;
    let mut file = File::open(path)?;
    io::copy(&mut file, archive)?;
    let padding = (512 - (metadata.len() % 512)) % 512;
    if padding > 0 {
        archive.write_all(&vec![0_u8; padding as usize])?;
    }
    Ok(())
}

fn append_archive_tree<W: Write>(
    archive: &mut W,
    root: &Path,
    current: &Path,
) -> Result<(), PipelineError> {
    let mut entries = fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_dir() {
            append_archive_tree(archive, root, &path)?;
        } else if metadata.file_type().is_file() {
            if matches!(
                path.file_name().and_then(|value| value.to_str()),
                Some(".tantivy-meta.lock") | Some(".tantivy-writer.lock")
            ) {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| PipelineError::InvalidRelease("archive path escaped root".into()))?;
            let name = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            append_archive_file(archive, &path, &format!("index/{name}"))?;
        } else {
            return Err(PipelineError::InvalidRelease(format!(
                "archive source is not a regular file or directory: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Create a compressed tar archive containing `index/` and the manifest.
pub fn package_release(root: &Path, artifact: &Path) -> Result<(), PipelineError> {
    verify_release(root)?;
    if let Some(parent) = artifact.parent() {
        fs::create_dir_all(parent)?;
    }
    let output = File::create(artifact)?;
    let mut encoder = zstd::stream::write::Encoder::new(output, 6)?;
    append_archive_tree(&mut encoder, &root.join("index"), &root.join("index"))?;
    append_archive_file(&mut encoder, &root.join(MANIFEST_FILE), MANIFEST_FILE)?;
    encoder.write_all(&[0_u8; 1024])?;
    encoder.finish()?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn raw(code: &str, name: &str, kcal: f64) -> Value {
        serde_json::json!({
            "code": code,
            "product_name": name,
            "brands": "Example",
            "nutriments": {"energy-kcal_100g": kcal, "proteins_100g": 2.0},
        })
    }

    #[test]
    fn normalizes_aliases_and_sorts_lists() {
        let value = serde_json::json!({
            "_id": "0012345678905",
            "product_name": "Tea",
            "generic_name": "Tea drink",
            "brands": "Brand",
            "countries": "United States, France;france",
            "countries_tags": ["en:United States", "fr:France"],
            "languages_codes": ["en", "fr", "en"],
            "serving_quantity": "12.5",
            "last_modified_t": 1700000000,
            "nutriments": {"energy-kj_100g": 418.4, "carbohydrates_100g": 1}
        });
        let product = normalize_product(&value).expect("valid product");
        assert_eq!(product.barcode, "0012345678905");
        assert_eq!(product.countries, vec!["france", "united states"]);
        assert_eq!(product.languages, vec!["en", "fr"]);
        assert!((product.energy_kcal_100g.unwrap() - 100.0).abs() < 0.001);
    }

    #[test]
    fn rejects_invalid_or_empty_products() {
        assert!(normalize_product(&raw("123", "No", 10.)).is_none());
        assert!(
            normalize_product(&serde_json::json!({
                "code": "12345678",
                "product_name": "No nutrition",
                "nutriments": {}
            }))
            .is_none()
        );
        assert!(!valid_barcode("12345678x"));
    }

    #[test]
    fn duplicate_winner_is_independent_of_input_order() {
        let mut a = normalize_product(&raw("12345678", "A", 1.)).unwrap();
        let mut b = normalize_product(&raw("12345678", "B", 1.)).unwrap();
        a.brand = "".into();
        b.brand = "Better".into();
        assert_eq!(product_sort(&b, &a), Ordering::Less);
        assert_eq!(product_sort(&a, &b), Ordering::Greater);
    }

    #[test]
    fn safe_paths_reject_parent_components() {
        assert!(safe_relative_path("segments/abc"));
        assert!(!safe_relative_path("../abc"));
    }

    #[test]
    fn dataset_versions_match_activation_names() {
        assert!(valid_dataset_version("2026-01-17"));
        assert!(valid_dataset_version("fixture_1.0"));
        assert!(!valid_dataset_version("../unsafe"));
        assert!(!valid_dataset_version(""));
    }

    #[test]
    fn gzip_input_is_read_as_a_stream() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder
            .write_all(
                serde_json::to_string(&raw("12345678", "A", 1.))
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        let data = encoder.finish().unwrap();
        let mut decoder = MultiGzDecoder::new(Cursor::new(data));
        let mut output = String::new();
        decoder.read_to_string(&mut output).unwrap();
        assert!(output.contains("12345678"));
    }
}
