// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;
use iggy::prelude::{
    AutoLogin, Client as IggyClientTrait, Credentials, Identifier, IggyClient, TopicClient,
};
use reqwest::Client;
use whippet_bench::BenchConfig;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::protocol::Command;
use whippet_engine::ShardEngine;
use whippet_journal::{IggyJournal, Journal, Topic};

use crate::config::{exchange_config, read_password};
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
    /// Orders per HTTP request. 1 is the single-order path; up to 256 matches
    /// the Iggy default message batch.
    #[arg(long, default_value_t = 1)]
    pub batch_size: usize,
    /// Ingress transport: `http` (default) or `tcp`.
    #[arg(long, default_value = "http")]
    pub transport: String,
    /// Binary TCP ingress address, used when `--transport tcp`.
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub tcp_addr: String,
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
        batch_size: args.batch_size,
        transport: args.transport,
        tcp_addr: args.tcp_addr,
        ..BenchConfig::default()
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
            .read_payloads(Topic::Orders, args.partition, &consumer, offset, 1_000, 100)
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

/// Read-only preflight. It never creates topics or snapshots; it reports what
/// the node would find if it started now.
#[derive(Args, Clone)]
pub struct DoctorArgs {
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Read the Iggy password from a file, as the node does.
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD_FILE")]
    pub iggy_password_file: Option<PathBuf>,
    #[arg(long, env = "WHIPPET_PARTITIONS", default_value_t = 16)]
    pub partitions: u32,
    #[arg(
        long,
        env = "WHIPPET_SYMBOLS",
        default_value = "BTC-USD,ETH-USD,SOL-USD"
    )]
    pub symbols: String,
    #[arg(long, env = "WHIPPET_SNAPSHOT_DIR", default_value = "data/snapshots")]
    pub snapshot_dir: PathBuf,
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub gateway: String,
    #[arg(long, default_value_t = 3_000)]
    pub timeout_ms: u64,
}

pub async fn run_doctor(args: DoctorArgs) -> Result<()> {
    if args.partitions == 0 {
        bail!("--partitions must be at least 1");
    }
    let exchange = exchange_config("whippet", args.partitions, Mode::Durable, &args.symbols)?;
    let password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
    let mut failures: Vec<String> = Vec::new();

    println!("whippet doctor");
    println!("  iggy      {}", args.iggy_addr);
    println!("  gateway   {}", args.gateway);
    println!("  snapshots {}", args.snapshot_dir.display());
    println!("  partitions {}", exchange.partitions);
    println!("  symbols   {}", args.symbols);

    // Doctor talks to Iggy directly for a read-only topic inventory. The
    // node's data path still goes through `whippet-journal`; this client only
    // inspects topics and never creates, appends, or commits.
    let client = IggyClient::builder()
        .with_tcp()
        .with_server_address(args.iggy_addr.clone())
        .with_no_delay()
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            args.iggy_user.clone(),
            password.into(),
        )))
        .build()?;
    let timeout = Duration::from_millis(args.timeout_ms);
    match tokio::time::timeout(timeout, client.connect()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => bail!("could not connect to Iggy at {}: {error}", args.iggy_addr),
        Err(_) => bail!(
            "timed out connecting to Iggy at {} after {} ms",
            args.iggy_addr,
            args.timeout_ms
        ),
    }
    println!("iggy: connected");

    for topic in Topic::ALL {
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let expected = topic.partition_count(&exchange);
        let details =
            match tokio::time::timeout(timeout, client.get_topic(&stream_id, &topic_id)).await {
                Ok(Ok(details)) => details,
                Ok(Err(error)) => {
                    println!("  FAIL {topic:<26} {error}");
                    failures.push(format!("topic {topic} could not be inspected: {error}"));
                    continue;
                }
                Err(_) => {
                    println!("  FAIL {topic:<26} timed out");
                    failures.push(format!("topic {topic} inspection timed out"));
                    continue;
                }
            };
        match details {
            None => {
                println!("  FAIL {topic:<26} missing (expected {expected} partitions)");
                failures.push(format!("topic {topic} is missing"));
            }
            Some(details) => {
                let actual = details.partitions_count;
                if actual != expected {
                    println!("  FAIL {topic:<26} partitions {actual}, expected {expected}");
                    failures.push(format!(
                        "topic {topic} has {actual} partitions, expected {expected}"
                    ));
                } else {
                    println!("  ok   {topic:<26} partitions {actual}");
                }
                let missing: Vec<u32> = (0..expected)
                    .filter(|id| !details.partitions.iter().any(|part| part.id == *id))
                    .collect();
                if !missing.is_empty() {
                    println!("       missing partition ids: {missing:?}");
                    failures.push(format!("topic {topic} is missing partitions {missing:?}"));
                }
                let heads: Vec<String> = details
                    .partitions
                    .iter()
                    .map(|part| format!("{}:{}", part.id, part.current_offset))
                    .collect();
                println!("       head {}", heads.join(" "));
            }
        }
    }

    let store = SnapshotStore::new(args.snapshot_dir.clone());
    let mut snapshots = 0u32;
    for shard in 0..exchange.partitions {
        match store.load(shard) {
            Ok(None) => {}
            Ok(Some(snapshot)) => {
                snapshots += 1;
                match ShardEngine::from_snapshot(&exchange, snapshot) {
                    Ok(engine) => {
                        let last_offset = engine
                            .snapshot_state()
                            .symbols
                            .iter()
                            .map(|symbol| symbol.last_offset)
                            .max()
                            .unwrap_or(0);
                        println!(
                            "  ok   snapshot shard {shard:<3} hash={} last_offset={last_offset}",
                            hex(&engine.state_hash())
                        );
                    }
                    Err(error) => {
                        println!("  FAIL snapshot shard {shard}: {error}");
                        failures.push(format!(
                            "snapshot shard {shard} failed verification: {error}"
                        ));
                    }
                }
            }
            Err(error) => {
                println!("  FAIL snapshot shard {shard}: {error}");
                failures.push(format!("snapshot shard {shard} could not be read: {error}"));
            }
        }
    }
    if snapshots == 0 {
        println!(
            "snapshots: none in {} (the node writes one every N commands)",
            args.snapshot_dir.display()
        );
    } else {
        println!("snapshots: verified {snapshots} shard file(s)");
    }

    let http = Client::builder()
        .timeout(Duration::from_millis(args.timeout_ms))
        .build()?;
    let url = format!("{}/api/health", args.gateway.trim_end_matches('/'));
    match http.get(&url).send().await {
        Ok(response) => {
            let status = response.status();
            let body: serde_json::Value = response.json().await.unwrap_or(serde_json::Value::Null);
            let health = body
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let auth_required = body
                .get("auth_required")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            println!("gateway: HTTP {status} status={health} auth_required={auth_required}");
            if !status.is_success() || health != "ok" {
                failures.push(format!(
                    "gateway {url} reports HTTP {status} status={health}"
                ));
            }
        }
        Err(error) => {
            println!("gateway: unreachable at {url} ({error}); skipped");
        }
    }

    if failures.is_empty() {
        println!("doctor: all checks passed");
        Ok(())
    } else {
        for failure in &failures {
            eprintln!("doctor: {failure}");
        }
        bail!("{} hard failure(s)", failures.len())
    }
}
