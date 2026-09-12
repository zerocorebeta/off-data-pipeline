use off_data_pipeline::{
    BuildOptions, PipelineError, ProgressOptions, ProgressReporter, build_release_with_progress,
    package_release_with_progress, verify_release_with_progress,
};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

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

fn optional_number(args: &[String], name: &str) -> Result<Option<u64>, String> {
    let Some(value) = optional_flag(args, name)? else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .ok_or_else(|| format!("{name} value is not valid UTF-8"))?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| format!("{name} must be a non-negative integer"))
}

fn optional_env_number(name: &str) -> Result<Option<u64>, String> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| format!("{name} is not valid UTF-8"))?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| format!("{name} must be a non-negative integer"))
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

fn positive_usize(args: &[String], name: &str, default: usize) -> Result<usize, String> {
    let Some(value) = optional_number(args, name)? else {
        return Ok(default);
    };
    usize::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn progress_options(args: &[String]) -> Result<ProgressOptions, String> {
    let defaults = ProgressOptions::default();
    let record_interval = optional_number(args, "--progress-records")?
        .or(optional_env_number("OFF_PROGRESS_RECORDS")?)
        .unwrap_or(defaults.record_interval);
    let interval_ms = optional_number(args, "--progress-interval-ms")?
        .or(optional_env_number("OFF_PROGRESS_INTERVAL_MS")?);
    let interval_seconds = optional_number(args, "--progress-interval-seconds")?
        .or(optional_env_number("OFF_PROGRESS_INTERVAL_SECONDS")?);
    if interval_ms.is_some() && interval_seconds.is_some() {
        return Err("use only one of --progress-interval-ms or --progress-interval-seconds".into());
    }
    let wall_interval = if let Some(milliseconds) = interval_ms {
        Duration::from_millis(milliseconds)
    } else if let Some(seconds) = interval_seconds {
        Duration::from_secs(seconds)
    } else {
        defaults.wall_interval
    };
    ProgressOptions::new(record_interval, wall_interval)
}

fn finish_progress<T>(
    reporter: &mut ProgressReporter,
    result: Result<T, PipelineError>,
) -> Result<T, String> {
    match result {
        Ok(value) => reporter
            .complete()
            .map(|_| value)
            .map_err(|error| error.to_string()),
        Err(error) => {
            reporter.abort();
            Err(error.to_string())
        }
    }
}

fn usage() -> &'static str {
    "usage:\n  off-data-pipeline build --input DUMP.jsonl.gz --output RELEASE --dataset-version VERSION [--artifact ARCHIVE] [--chunk-size N] [--worker-threads N] [--index-memory-mb N] [--progress-records N] [--progress-interval-ms N|--progress-interval-seconds N]\n  off-data-pipeline verify --release RELEASE [progress flags]\n  off-data-pipeline package --release RELEASE --artifact ARCHIVE [progress flags]"
}

fn run() -> Result<(), String> {
    let args = env::args().collect::<Vec<_>>();
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    match command {
        "build" => {
            let chunk_size = optional_chunk_size(&args)?;
            let default_threads = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1);
            let worker_threads = positive_usize(&args, "--worker-threads", default_threads)?;
            let index_memory_mb = positive_usize(&args, "--index-memory-mb", 512)?;
            let progress = progress_options(&args)?;
            let mut reporter =
                ProgressReporter::new(progress).map_err(|error| error.to_string())?;
            let options = BuildOptions {
                input: required_flag(&args, "--input")?,
                output: required_flag(&args, "--output")?,
                artifact: optional_flag(&args, "--artifact")?,
                dataset_version: required_text(&args, "--dataset-version")?,
                chunk_size,
                worker_threads,
                index_memory_bytes: index_memory_mb
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| "--index-memory-mb is too large".to_owned())?,
            };
            let manifest = build_release_with_progress(&options, &mut reporter)
                .map_err(|error| error.to_string())?;
            println!(
                "built {} records for {}",
                manifest.record_count, manifest.dataset_version
            );
            Ok(())
        }
        "verify" => {
            let mut reporter = ProgressReporter::new(progress_options(&args)?)
                .map_err(|error| error.to_string())?;
            let release = required_flag(&args, "--release")?;
            let result = verify_release_with_progress(&release, &reporter);
            let manifest = finish_progress(&mut reporter, result)?;
            println!(
                "verified {} records for {}",
                manifest.record_count, manifest.dataset_version
            );
            Ok(())
        }
        "package" => {
            let release = required_flag(&args, "--release")?;
            let artifact = required_flag(&args, "--artifact")?;
            let mut reporter = ProgressReporter::new(progress_options(&args)?)
                .map_err(|error| error.to_string())?;
            let result = package_release_with_progress(&release, &artifact, &reporter);
            finish_progress(&mut reporter, result)?;
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
