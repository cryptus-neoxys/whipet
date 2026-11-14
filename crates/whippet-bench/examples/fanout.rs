// Licensed under the Apache License, Version 2.0. See LICENSE.

//! 20 Hz WebSocket fanout benchmark runner.
//!
//! Connects `--subscribers` WebSocket clients to `/ws?trades=N`, optionally
//! sends a `Subscribe` symbol filter, runs for `--warmup + --duration` seconds,
//! and writes a fanout JSON manifest under `--output-dir`.
//!
//! ```sh
//! cargo run --release -p whippet-bench --example fanout -- \
//!   --gateway http://127.0.0.1:8080 --subscribers 10 --duration 30 \
//!   --warmup 5 --trades 0 --label dev-loop-fanout --output-dir artifacts/bench
//! ```

#![forbid(unsafe_code)]

use std::path::PathBuf;

use whippet_bench::BenchError;
use whippet_bench::fanout::{FanoutConfig, run_fanout};

fn take(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, BenchError> {
    args.next()
        .ok_or_else(|| BenchError::Config(format!("{flag} needs a value")))
}

fn parse<T: std::str::FromStr>(flag: &str, raw: &str) -> Result<T, BenchError> {
    raw.parse()
        .map_err(|_| BenchError::Config(format!("{flag}: invalid value {raw:?}")))
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<FanoutConfig>, BenchError> {
    let mut config = FanoutConfig::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--gateway" => config.gateway = take(&mut args, &flag)?,
            "--subscribers" => config.subscribers = parse(&flag, &take(&mut args, &flag)?)?,
            "--duration" => config.duration_secs = parse(&flag, &take(&mut args, &flag)?)?,
            "--warmup" => config.warmup_secs = parse(&flag, &take(&mut args, &flag)?)?,
            "--trades" => config.trades = parse(&flag, &take(&mut args, &flag)?)?,
            "--symbols" => {
                config.symbols = take(&mut args, &flag)?
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            "--output-dir" => config.output_dir = PathBuf::from(take(&mut args, &flag)?),
            "--label" => config.label = take(&mut args, &flag)?,
            "-h" | "--help" => {
                println!(
                    "usage: fanout [--gateway URL] [--subscribers N] [--duration S] [--warmup S]\n\
                     \x20      [--trades N] [--symbols A,B] [--output-dir DIR] [--label NAME]"
                );
                return Ok(None);
            }
            other => {
                return Err(BenchError::Config(format!("unknown flag {other}")));
            }
        }
    }
    Ok(Some(config))
}

fn main() -> Result<(), BenchError> {
    let Some(config) = parse_args(std::env::args().skip(1))? else {
        return Ok(());
    };
    let output_dir = config.output_dir.clone();
    let runtime = tokio::runtime::Runtime::new()?;
    let manifest = runtime.block_on(run_fanout(config))?;

    println!(
        "{}  subs={}  frames/s={:.1} ({:.1}/client)  bytes/s/client={:.0}  agg_bytes/s={:.0}  gap_p50={:.0}µs  gap_p99={:.0}µs  stalled={}  scope={}",
        manifest.label,
        manifest.subscribers,
        manifest.frames_per_sec,
        manifest.frames_per_sec_per_client,
        manifest.bytes_per_sec_per_client,
        manifest.aggregate_bytes_per_sec,
        manifest.gap_us.p50,
        manifest.gap_us.p99,
        manifest.stalled_clients,
        manifest.scope,
    );
    for client in &manifest.clients {
        println!(
            "  client[{}]  frames/s={:.1}  bytes/s={:.0}  gap_p50={:.0}µs  gap_p99={:.0}µs  gap_max={:.0}µs  stalled={}",
            client.client_id,
            client.frames_per_sec,
            client.bytes_per_sec,
            client.gap_p50_us,
            client.gap_p99_us,
            client.gap_max_us,
            client.stalled,
        );
    }
    if manifest.stalled_clients > 0 {
        eprintln!(
            "WARNING: {} subscriber(s) stalled (no frame for >1 s)",
            manifest.stalled_clients
        );
    }
    println!(
        "manifest: {}/{}.json",
        output_dir.display(),
        manifest.run_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_flags_parse_with_defaults_and_overrides() {
        let defaults = parse_args(std::iter::empty()).unwrap().unwrap();
        assert_eq!(defaults.gateway, "http://127.0.0.1:8080");
        assert_eq!(defaults.subscribers, 10);
        assert_eq!(defaults.duration_secs, 30);
        assert_eq!(defaults.warmup_secs, 5);
        assert_eq!(defaults.trades, 0);
        assert!(defaults.symbols.is_empty());
        assert_eq!(defaults.output_dir, PathBuf::from("artifacts/bench"));
        assert_eq!(defaults.label, "fanout");

        let args = [
            "--gateway",
            "http://127.0.0.1:18080",
            "--subscribers",
            "100",
            "--duration",
            "15",
            "--warmup",
            "3",
            "--trades",
            "25",
            "--symbols",
            "BTC-USD,ETH-USD",
            "--output-dir",
            "/tmp/out",
            "--label",
            "dev-loop-fanout",
        ]
        .into_iter()
        .map(String::from);
        let custom = parse_args(args).unwrap().unwrap();
        assert_eq!(custom.gateway, "http://127.0.0.1:18080");
        assert_eq!(custom.subscribers, 100);
        assert_eq!(custom.duration_secs, 15);
        assert_eq!(custom.warmup_secs, 3);
        assert_eq!(custom.trades, 25);
        assert_eq!(custom.symbols, vec!["BTC-USD", "ETH-USD"]);
        assert_eq!(custom.output_dir, PathBuf::from("/tmp/out"));
        assert_eq!(custom.label, "dev-loop-fanout");
    }
}
