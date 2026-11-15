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
//!
//! Comparison mode: run once with `--ingress-rate 0` (no-subscriber baseline)
//! and once with `--ingress-rate 10000`, passing the first manifest with
//! `--baseline-metrics` so the second manifest records whether subscribers
//! slowed the engine.

#![forbid(unsafe_code)]

use std::path::PathBuf;

use whippet_bench::BenchError;
use whippet_bench::fanout::{BaselineSnapshot, FanoutConfig, ParseMode, run_fanout};

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
            "--parse" => {
                let raw = take(&mut args, &flag)?;
                config.parse_mode = raw.parse::<ParseMode>().map_err(BenchError::Config)?;
            }
            "--ingress-rate" => config.ingress_rate = parse(&flag, &take(&mut args, &flag)?)?,
            "--ingress-batch" => {
                config.ingress_batch = parse(&flag, &take(&mut args, &flag)?)?;
            }
            "--baseline-metrics" => {
                config.baseline = Some(load_baseline(&take(&mut args, &flag)?)?);
            }
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
                     \x20      [--trades N] [--parse envelope|full] [--ingress-rate R] [--ingress-batch N]\n\
                     \x20      [--baseline-metrics PATH] [--symbols A,B] [--output-dir DIR] [--label NAME]"
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

/// Load the `/metrics` deltas from a no-subscriber baseline manifest so this
/// run's manifest can record whether subscribers slowed the engine.
fn load_baseline(path: &str) -> Result<BaselineSnapshot, BenchError> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        BenchError::Config(format!("--baseline-metrics: cannot read {path:?}: {error}"))
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|error| {
        BenchError::Config(format!(
            "--baseline-metrics: invalid JSON in {path:?}: {error}"
        ))
    })?;
    let at = |pointer: &str| value.pointer(pointer).and_then(serde_json::Value::as_u64);
    Ok(BaselineSnapshot {
        source: path.to_string(),
        orders_ingressed_delta: at("/metrics/orders_ingressed_delta"),
        md_sequence_delta: at("/metrics/md_sequence_delta"),
    })
}

fn main() -> Result<(), BenchError> {
    let Some(config) = parse_args(std::env::args().skip(1))? else {
        return Ok(());
    };
    let output_dir = config.output_dir.clone();
    let runtime = tokio::runtime::Runtime::new()?;
    let manifest = runtime.block_on(run_fanout(config))?;

    println!(
        "{}  subs={}  parse={}  frames/s={:.1} ({:.1}/client)  bytes/s/client={:.0}  agg_bytes/s={:.0}  gap_p50={:.0}µs  gap_p99={:.0}µs  stalled={}  scope={}",
        manifest.label,
        manifest.subscribers,
        manifest.parse_mode,
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
    if manifest.ingress.sent > 0 {
        println!(
            "ingress: target={}/s  batch={}  sent={}  ok={}  errors={}  achieved={:.0}/s",
            manifest.ingress.target_rate_per_sec,
            manifest.ingress.batch_size,
            manifest.ingress.sent,
            manifest.ingress.ok,
            manifest.ingress.errors,
            manifest.ingress.achieved_per_sec,
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
        assert_eq!(defaults.parse_mode, ParseMode::Envelope);
        assert_eq!(defaults.ingress_rate, 0);
        assert_eq!(defaults.ingress_batch, 1);
        assert!(defaults.baseline.is_none());
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
            "--parse",
            "full",
            "--ingress-rate",
            "10000",
            "--ingress-batch",
            "10",
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
        assert_eq!(custom.parse_mode, ParseMode::Full);
        assert_eq!(custom.ingress_rate, 10_000);
        assert_eq!(custom.ingress_batch, 10);
        assert_eq!(custom.symbols, vec!["BTC-USD", "ETH-USD"]);
        assert_eq!(custom.output_dir, PathBuf::from("/tmp/out"));
        assert_eq!(custom.label, "dev-loop-fanout");
    }

    #[test]
    fn baseline_metrics_flag_loads_deltas_from_a_manifest() {
        let dir = std::env::temp_dir().join(format!("whippet-baseline-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("baseline.json");
        std::fs::write(
            &path,
            r#"{"metrics": {"orders_ingressed_delta": 300000, "md_sequence_delta": 600}}"#,
        )
        .unwrap();
        let args = ["--baseline-metrics", path.to_str().unwrap()]
            .into_iter()
            .map(String::from);
        let config = parse_args(args).unwrap().unwrap();
        let baseline = config.baseline.unwrap();
        assert_eq!(baseline.source, path.to_str().unwrap());
        assert_eq!(baseline.orders_ingressed_delta, Some(300_000));
        assert_eq!(baseline.md_sequence_delta, Some(600));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn thousand_subscribers_parse_without_a_cap() {
        let args = ["--subscribers", "1000"].into_iter().map(String::from);
        let config = parse_args(args).unwrap().unwrap();
        assert_eq!(config.subscribers, 1000);
    }

    #[test]
    fn parse_and_ingress_batch_flags_reject_bad_values() {
        let bad_mode = ["--parse", "json"].into_iter().map(String::from);
        assert!(parse_args(bad_mode).is_err());
        let bad_batch = ["--ingress-batch", "nope"].into_iter().map(String::from);
        assert!(parse_args(bad_batch).is_err());
    }
}
