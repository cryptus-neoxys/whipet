// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::Client;
use whippet_bench::BenchConfig;
use whippet_core::config::ExchangeConfig;
use whippet_engine::ShardEngine;

use crate::snapshot::SnapshotStore;

#[derive(Args, Debug, Clone)]
pub struct BenchArgs {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub gateway: String,
    #[arg(long, default_value_t = 1_000)]
    pub rate: u64,
    #[arg(long, default_value_t = 30)]
    pub duration: u64,
    #[arg(long, default_value_t = 5)]
    pub warmup: u64,
    #[arg(long, default_value_t = 5)]
    pub repetitions: u32,
    #[arg(long, default_value_t = 0x5eed_0001)]
    pub seed: u64,
    #[arg(long, default_value = "BTC-USD,ETH-USD,SOL-USD")]
    pub symbols: String,
    #[arg(long, default_value = "durable")]
    pub mode: String,
    #[arg(long, default_value = "artifacts/bench")]
    pub output_dir: PathBuf,
    #[arg(long, default_value = "L1")]
    pub label: String,
    #[arg(long, default_value_t = 4_096)]
    pub max_in_flight: usize,
    #[arg(long, default_value_t = 5_000)]
    pub timeout_ms: u64,
}

pub async fn run_bench(args: BenchArgs) -> Result<()> {
    let output_dir = args.output_dir.clone();
    let config = BenchConfig {
        base_url: args.gateway,
        rate_per_sec: args.rate,
        duration_secs: args.duration,
        warmup_secs: args.warmup,
        repetitions: args.repetitions,
        seed: args.seed,
        symbols: args
            .symbols
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
        mode: args.mode,
        output_dir: args.output_dir,
        label: args.label,
        max_in_flight: args.max_in_flight,
        timeout_ms: args.timeout_ms,
    };
    let manifest = whippet_bench::run(config).await?;
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

#[derive(Args, Debug, Clone)]
pub struct HealthArgs {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub gateway: String,
    #[arg(long, default_value_t = 3_000)]
    pub timeout_ms: u64,
}

pub async fn run_health(args: HealthArgs) -> Result<()> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_millis(args.timeout_ms))
        .build()?;
    let url = format!("{}/api/health", args.gateway.trim_end_matches('/'));
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let body: serde_json::Value = response.json().await?;
    println!("{}", serde_json::to_string_pretty(&body)?);
    if !status.is_success() || body.get("status").and_then(|value| value.as_str()) != Some("ok") {
        bail!("Whippet is not healthy (HTTP {status})");
    }
    Ok(())
}

#[derive(Args, Debug, Clone)]
pub struct VerifyArgs {
    #[arg(long, default_value = "data/snapshots")]
    pub snapshot_dir: PathBuf,
    #[arg(long, default_value_t = 16)]
    pub partitions: u32,
}

pub fn run_verify(args: VerifyArgs) -> Result<()> {
    let exchange = ExchangeConfig::default();
    let store = SnapshotStore::new(args.snapshot_dir.clone());
    let mut found = 0;
    for shard in 0..args.partitions {
        let Some(snapshot) = store
            .load(shard)
            .with_context(|| format!("loading snapshot for shard {shard}"))?
        else {
            continue;
        };
        let engine = ShardEngine::from_snapshot(&exchange, snapshot.clone())
            .with_context(|| format!("verifying shard {shard}"))?;
        let symbols: Vec<String> = snapshot
            .symbols
            .iter()
            .map(|symbol| format!("{}@{}", symbol.symbol, symbol.last_offset))
            .collect();
        println!(
            "shard {:>2}  last_offset={:<10}  hash={}  [{}]",
            shard,
            snapshot
                .symbols
                .iter()
                .map(|symbol| symbol.last_offset)
                .max()
                .unwrap_or(0),
            hex(&engine.state_hash()),
            symbols.join(", ")
        );
        found += 1;
    }
    if found == 0 {
        println!(
            "no snapshots in {} (the node writes one every N commands)",
            args.snapshot_dir.display()
        );
    } else {
        println!("verified {found} shard snapshot(s); all state hashes match");
    }
    Ok(())
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
