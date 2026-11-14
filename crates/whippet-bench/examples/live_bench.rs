// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Live smoke runner for the harness itself.
//!
//! The supported entry point is `whippet bench`. This example exists so the
//! harness can be exercised against a running node while the CLI is being
//! changed independently:
//!
//! ```sh
//! cargo run --release -p whippet-bench --example live_bench -- \
//!   --gateway http://127.0.0.1:8080 --rate 1000 --duration 8 \
//!   --warmup 2 --repetitions 1 --label batch-10 --batch-size 10
//! ```
//!
//! It writes the same manifest as the CLI, under `--output-dir`.

use std::path::PathBuf;

use whippet_bench::{BenchConfig, BenchError};

fn take(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, BenchError> {
    args.next()
        .ok_or_else(|| BenchError::Config(format!("{flag} needs a value")))
}

fn parse<T: std::str::FromStr>(flag: &str, raw: &str) -> Result<T, BenchError> {
    raw.parse()
        .map_err(|_| BenchError::Config(format!("{flag}: invalid value {raw:?}")))
}

fn main() -> Result<(), BenchError> {
    let mut config = BenchConfig::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--gateway" => config.base_url = take(&mut args, &flag)?,
            "--rate" => config.rate_per_sec = parse(&flag, &take(&mut args, &flag)?)?,
            "--duration" => config.duration_secs = parse(&flag, &take(&mut args, &flag)?)?,
            "--warmup" => config.warmup_secs = parse(&flag, &take(&mut args, &flag)?)?,
            "--repetitions" => config.repetitions = parse(&flag, &take(&mut args, &flag)?)?,
            "--seed" => config.seed = parse(&flag, &take(&mut args, &flag)?)?,
            "--symbols" => {
                config.symbols = take(&mut args, &flag)?
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            "--label" => config.label = take(&mut args, &flag)?,
            "--mode" => config.mode = take(&mut args, &flag)?,
            "--output-dir" => config.output_dir = PathBuf::from(take(&mut args, &flag)?),
            "--max-in-flight" => config.max_in_flight = parse(&flag, &take(&mut args, &flag)?)?,
            "--timeout-ms" => config.timeout_ms = parse(&flag, &take(&mut args, &flag)?)?,
            "--batch-size" => config.batch_size = parse(&flag, &take(&mut args, &flag)?)?,
            "--ioc-ratio" => config.ioc_ratio = parse(&flag, &take(&mut args, &flag)?)?,
            "--cancel-ratio" => config.cancel_ratio = parse(&flag, &take(&mut args, &flag)?)?,
            "-h" | "--help" => {
                println!(
                    "usage: live_bench [--gateway URL] [--rate N] [--duration S] [--warmup S]\n\
                     \x20      [--repetitions N] [--seed N] [--symbols A,B] [--label NAME]\n\
                     \x20      [--mode MODE] [--output-dir DIR] [--max-in-flight N]\n\
                     \x20      [--timeout-ms N] [--batch-size N] [--ioc-ratio F]\n\
                     \x20      [--cancel-ratio F]"
                );
                return Ok(());
            }
            other => {
                return Err(BenchError::Config(format!("unknown flag {other}")));
            }
        }
    }

    let output_dir = config.output_dir.clone();
    let runtime = tokio::runtime::Runtime::new()?;
    let manifest = runtime.block_on(whippet_bench::run(config))?;
    println!(
        "{}  n={}  p50={:.0}µs  p99={:.0}µs  p99.9={:.0}µs  throughput={:.0}/s  errors={}  scope={}",
        manifest.workload.name,
        manifest.latency.count,
        manifest.latency.p50,
        manifest.latency.p99,
        manifest.latency.p999,
        manifest.throughput.achieved_per_sec,
        manifest.throughput.error_count,
        manifest.scope_label(),
    );
    println!(
        "manifest: {}/{}.json",
        output_dir.display(),
        manifest.run_id
    );
    Ok(())
}
