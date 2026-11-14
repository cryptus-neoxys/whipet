// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use reqwest::Client;
use whippet_bench::BenchConfig;
use whippet_core::config::ExchangeConfig;
use whippet_core::protocol::Command;
use whippet_engine::ShardEngine;
use whippet_journal::{IggyJournal, Journal, Topic};

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

#[derive(Args, Debug, Clone)]
pub struct ReplayArgs {
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Partition to replay. BTC-USD hashes to 1 with 16 partitions.
    #[arg(long, default_value_t = 1)]
    pub partition: u32,
    /// Start offset. Omit to replay from a snapshot tail when one exists, else 0.
    #[arg(long)]
    pub from: Option<u64>,
    /// Inclusive stop offset. Omit to replay to the current end.
    #[arg(long)]
    pub to: Option<u64>,
    #[arg(long, default_value = "data/snapshots")]
    pub snapshot_dir: PathBuf,
    #[arg(long, default_value_t = 16)]
    pub exchange_partitions: u32,
    /// Fail unless the final state hash equals this value.
    #[arg(long)]
    pub expect_hash: Option<String>,
}

pub async fn run_replay(args: ReplayArgs) -> Result<()> {
    let exchange = ExchangeConfig {
        partitions: args.exchange_partitions,
        ..ExchangeConfig::default()
    };
    let store = SnapshotStore::new(args.snapshot_dir.clone());
    let snapshot = store.load(args.partition)?;
    // An explicit --from means a clean replay from that offset. Without it,
    // seed from the snapshot and replay only the tail. Never do both: that
    // would apply the tail twice.
    let use_snapshot = args.from.is_none() && snapshot.is_some();
    let (mut engine, snapshot_offset, seeded) = match (use_snapshot, &snapshot) {
        (true, Some(snapshot)) => {
            let mut engine = ShardEngine::from_snapshot(&exchange, snapshot.clone())
                .with_context(|| format!("restoring shard {}", args.partition))?;
            engine.set_snapshot_interval(u64::MAX);
            let offset = snapshot
                .symbols
                .iter()
                .map(|symbol| symbol.last_offset)
                .max()
                .unwrap_or(0);
            (engine, Some(offset), true)
        }
        _ => (
            ShardEngine::new_with_snapshot_interval(args.partition, &exchange, u64::MAX),
            None,
            false,
        ),
    };

    let start = args
        .from
        .unwrap_or_else(|| snapshot_offset.map_or(0, |offset| offset.saturating_add(1)));
    let journal = Journal::Iggy(
        IggyJournal::connect(&args.iggy_addr, &args.iggy_user, &args.iggy_password)
            .await
            .with_context(|| format!("connecting to Iggy at {}", args.iggy_addr))?,
    );
    let consumer = format!("whippet-replay-p{}", args.partition);
    let mut offset = start;
    let mut decoded = 0u64;
    let mut skipped = 0u64;
    let mut last_offset = start.saturating_sub(1);
    let mut out = whippet_engine::EngineOutput::with_capacity(16, 16);
    let mut empty_polls = 0u32;

    loop {
        if let Some(to) = args.to
            && offset > to
        {
            break;
        }
        let records = journal
            .read(Topic::Orders, args.partition, &consumer, offset, 1_000, 100)
            .await
            .with_context(|| format!("reading orders from offset {offset}"))?;
        if records.is_empty() {
            // An empty poll can be transient while the server materialises a
            // partition or a producer's last write becomes visible. Do not
            // declare end-of-log on the first empty batch.
            empty_polls += 1;
            if empty_polls >= 5 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        empty_polls = 0;
        if let Some(first) = records.first()
            && first.offset > offset
        {
            bail!(
                "order log gap: requested offset {offset}, first available {}; a snapshot or reset is required",
                first.offset
            );
        }
        for record in records {
            if let Some(to) = args.to
                && record.offset > to
            {
                break;
            }
            match Command::decode(&record.payload) {
                Ok(command) => {
                    engine
                        .process(&command, record.offset, &mut out)
                        .with_context(|| format!("applying offset {}", record.offset))?;
                    decoded += 1;
                }
                Err(error) => {
                    skipped += 1;
                    eprintln!("skip offset {}: {error}", record.offset);
                }
            }
            last_offset = record.offset;
            offset = record.offset.saturating_add(1);
        }
    }

    let hash = hex(&engine.state_hash());
    let summary = serde_json::json!({
        "partition": args.partition,
        "seeded_from_snapshot": seeded,
        "snapshot_offset": snapshot_offset,
        "from": start,
        "to": args.to,
        "last_offset": last_offset,
        "decoded": decoded,
        "skipped": skipped,
        "state_hash": hash,
    });
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if let Some(expected) = args.expect_hash {
        let expected = expected.trim().to_ascii_lowercase();
        if expected != hash {
            bail!("state hash mismatch: expected {expected}, got {hash}");
        }
        println!("state hash matches expectation");
    }
    Ok(())
}
