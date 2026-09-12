use off_data_pipeline::{BuildOptions, build_release, package_release, verify_release};
use std::env;
use std::path::PathBuf;

fn required_flag(args: &[String], name: &str) -> Result<PathBuf, String> {
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Err(format!("missing {name}"));
    };
    args.get(index + 1)
        .filter(|value| !value.starts_with('-'))
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing value for {name}"))
}

fn required_text(args: &[String], name: &str) -> Result<String, String> {
    required_flag(args, name).map(|value| value.to_string_lossy().into_owned())
}

fn optional_flag(args: &[String], name: &str) -> Result<Option<PathBuf>, String> {
    if args.iter().any(|arg| arg == name) {
        required_flag(args, name).map(Some)
    } else {
        Ok(None)
    }
}

fn optional_chunk_size(args: &[String]) -> Result<usize, String> {
    let Some(value) = optional_flag(args, "--chunk-size")? else {
        return Ok(off_data_pipeline::DEFAULT_CHUNK_SIZE);
    };
    let value = value
        .to_str()
        .ok_or_else(|| "chunk size is not valid UTF-8".to_owned())?;
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "chunk size must be a positive integer".to_owned())?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| "chunk size must be a positive integer".to_owned())
}

fn usage() -> &'static str {
    "usage:\n  off-data-pipeline build --input DUMP.jsonl.gz --output RELEASE --dataset-version VERSION [--artifact ARCHIVE] [--chunk-size N]\n  off-data-pipeline verify --release RELEASE\n  off-data-pipeline package --release RELEASE --artifact ARCHIVE"
}

fn run() -> Result<(), String> {
    let args = env::args().collect::<Vec<_>>();
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    match command {
        "build" => {
            let chunk_size = optional_chunk_size(&args)?;
            let manifest = build_release(&BuildOptions {
                input: required_flag(&args, "--input")?,
                output: required_flag(&args, "--output")?,
                artifact: optional_flag(&args, "--artifact")?,
                dataset_version: required_text(&args, "--dataset-version")?,
                chunk_size,
            })
            .map_err(|error| error.to_string())?;
            println!(
                "built {} records for {}",
                manifest.record_count, manifest.dataset_version
            );
            Ok(())
        }
        "verify" => {
            let manifest = verify_release(&required_flag(&args, "--release")?)
                .map_err(|error| error.to_string())?;
            println!(
                "verified {} records for {}",
                manifest.record_count, manifest.dataset_version
            );
            Ok(())
        }
        "package" => {
            let release = required_flag(&args, "--release")?;
            let artifact = required_flag(&args, "--artifact")?;
            package_release(&release, &artifact).map_err(|error| error.to_string())?;
            println!("packaged {}", artifact.display());
            Ok(())
        }
        "help" | "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(usage().into()),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("off-data-pipeline: {error}");
        std::process::exit(1);
    }
}
