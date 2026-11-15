// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::Args;
use iggy::prelude::{
    AutoLogin, Client as IggyClientTrait, Credentials, Identifier, IggyClient, TopicClient,
};
use reqwest::Client;
use whippet_bench::BenchConfig;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, Symbol};
use whippet_core::protocol::{Command, ExecutionReport};
use whippet_engine::ShardEngine;
use whippet_journal::{IggyJournal, Journal, PayloadRecord, Topic};

use crate::config::{exchange_config, read_password};
use crate::engine_runner::resume_offset;
use crate::inspect::{
    AUDIT_TOPIC_PARTITIONS, AuditFilter, AuditRecord, ExecRole, OrderTrace, build_order_trace,
    filter_audit, parse_order_id,
};
use crate::retention::{RetentionDecision, RetentionInput, evaluate, explain};
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
    /// Partition count the snapshots were written with. `from_snapshot` checks
    /// each symbol's shard against this count, so a 4-partition snapshot must
    /// be verified with `--partitions 4`, not the 16-partition default.
    #[arg(long, default_value_t = 16)]
    pub partitions: u32,
    /// Comma-separated symbols the node was started with. Defaults to the
    /// built-in BTC-USD,ETH-USD,SOL-USD set. Pass the node's
    /// `WHIPPET_SYMBOLS` when it ran a different universe, or the per-shard
    /// checks reject a valid snapshot for a symbol the node never wrote.
    #[arg(long, env = "WHIPPET_SYMBOLS")]
    pub symbols: Option<String>,
    /// Exit non-zero when the directory holds no snapshots instead of reporting
    /// that nothing was verified. Use for CI and release proof; leave unset for
    /// the lenient dev path.
    #[arg(long)]
    pub require_snapshot: bool,
}

/// Build the exchange layout a snapshot was written with.
///
/// `verify` and `replay` restore snapshots by symbol, so their universe must
/// match the node's. The node derives it from `--symbols`/`WHIPPET_SYMBOLS`;
/// when no list is supplied these commands keep the historical default
/// (BTC-USD, ETH-USD, SOL-USD).
fn snapshot_exchange(partitions: u32, symbols: Option<&str>) -> Result<ExchangeConfig> {
    let Some(symbols) = symbols else {
        return Ok(ExchangeConfig {
            partitions,
            ..ExchangeConfig::default()
        });
    };
    let venue = ExchangeConfig::default().venue;
    exchange_config(&venue, partitions, Mode::Durable, symbols)
        .with_context(|| format!("invalid --symbols {symbols:?}"))
}

pub fn run_verify(args: VerifyArgs) -> Result<()> {
    if args.partitions == 0 {
        bail!("--partitions must be at least 1");
    }
    let exchange = snapshot_exchange(args.partitions, args.symbols.as_deref())?;
    let store = SnapshotStore::new(args.snapshot_dir.clone());
    // Active shards own at least one configured symbol. A `--require-snapshot`
    // gate must cover every one of them, not just "some file exists".
    let active_shards: Vec<u32> = {
        let mut shards: Vec<u32> = exchange
            .instruments
            .iter()
            .map(|instrument| instrument.symbol.partition(exchange.partitions))
            .collect();
        shards.sort_unstable();
        shards.dedup();
        shards
    };
    let mut found = 0;
    let mut found_shards: Vec<u32> = Vec::new();
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
        found_shards.push(shard);
    }
    if found == 0 {
        // Lenient by default so a fresh checkout can run `verify` without a
        // snapshot; `--require-snapshot` turns "nothing was checked" into a
        // failure, because a vacuous pass is not a proof.
        if args.require_snapshot {
            bail!(
                "no snapshots in {}; nothing verified (--require-snapshot)",
                args.snapshot_dir.display()
            );
        }
        println!(
            "no snapshots in {}; nothing verified (the node writes one every N commands)",
            args.snapshot_dir.display()
        );
        return Ok(());
    }
    if args.require_snapshot {
        let missing: Vec<u32> = active_shards
            .iter()
            .copied()
            .filter(|shard| !found_shards.contains(shard))
            .collect();
        if !missing.is_empty() {
            bail!(
                "missing snapshots for active shard(s) {missing:?} in {}; a partial set is not a proof (--require-snapshot)",
                args.snapshot_dir.display()
            );
        }
    }
    println!("verified {found} shard snapshot(s); all state hashes match");
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
    /// Comma-separated symbols the node was started with. Defaults to the
    /// built-in BTC-USD,ETH-USD,SOL-USD set. Pass the node's
    /// `WHIPPET_SYMBOLS` when it ran a different universe, or a valid snapshot
    /// is rejected for a symbol the node never wrote.
    #[arg(long, env = "WHIPPET_SYMBOLS")]
    pub symbols: Option<String>,
    /// Fail unless the replay is seeded from a snapshot that was found, is a
    /// real resume point (applied commands > 0), sits before the log head, and
    /// is not ahead of the engine's committed offset. Without it a missing
    /// snapshot silently degrades to a full replay from zero.
    #[arg(long)]
    pub require_snapshot: bool,
    /// Fail unless the final state hash equals this value.
    #[arg(long)]
    pub expect_hash: Option<String>,
}

pub async fn run_replay(args: ReplayArgs) -> Result<()> {
    let journal = Journal::Iggy(
        IggyJournal::connect(&args.iggy_addr, &args.iggy_user, &args.iggy_password)
            .await
            .with_context(|| format!("connecting to Iggy at {}", args.iggy_addr))?,
    );
    replay_with_journal(args, journal).await.map(|_| ())
}

/// The replay core, with the journal injected so the fail-closed rules can be
/// tested on the in-memory journal. `run_replay` supplies the Iggy journal.
///
/// Returns the summary that was printed, so tests can assert on the reported
/// offsets without scraping stdout.
#[doc(hidden)]
pub async fn replay_with_journal(args: ReplayArgs, journal: Journal) -> Result<serde_json::Value> {
    if args.exchange_partitions == 0 {
        bail!("--exchange-partitions must be at least 1");
    }
    let exchange = snapshot_exchange(args.exchange_partitions, args.symbols.as_deref())?;
    let store = SnapshotStore::new(args.snapshot_dir.clone());
    let snapshot = store
        .load(args.partition)
        .with_context(|| format!("loading snapshot for partition {}", args.partition))?;
    let snapshot_found = snapshot.is_some();
    // The node's resume rule, reused verbatim: a snapshot from a shard that
    // never applied a command is not a resume point. Treating its
    // `last_offset: 0` as one would skip the first order.
    let snapshot_resume = snapshot.as_ref().and_then(resume_offset);

    // The engine consumer is the authority for how far the live node has
    // applied the log. Replay reads under its own consumer and never commits,
    // so this is a read-only observation.
    let engine_consumer = format!("whippet-engine-p{}", args.partition);
    let committed_offset = journal
        .committed(Topic::Orders, args.partition, &engine_consumer)
        .await
        .with_context(|| format!("reading committed offset for {engine_consumer}"))?;
    // Head is the last offset in the partition, inclusive. `lag` reports the
    // records after the committed offset, so committed + lag is the head on
    // both journal backends. Read it even when nothing has been committed yet.
    let head = {
        let committed = committed_offset.unwrap_or(0);
        let lag = journal
            .lag(Topic::Orders, args.partition, committed)
            .await
            .with_context(|| format!("reading lag for partition {}", args.partition))?;
        Some(committed.saturating_add(lag))
    };

    // An explicit --from means a clean replay from that offset. Without it,
    // seed from the snapshot and replay only the tail. Never do both: that
    // would apply the tail twice.
    let use_snapshot = args.from.is_none() && snapshot_resume.is_some();
    let (mut engine, snapshot_offset, seeded) = match (use_snapshot, snapshot) {
        (true, Some(snapshot)) => {
            let mut engine = ShardEngine::from_snapshot(&exchange, snapshot)
                .with_context(|| format!("restoring shard {}", args.partition))?;
            engine.set_snapshot_interval(u64::MAX);
            (engine, snapshot_resume, true)
        }
        (_, snapshot) => {
            // Lenient mode may reach here with a snapshot file that has no
            // applied commands; say so rather than looking seeded to an
            // operator reading the summary.
            if snapshot.is_some() && snapshot_resume.is_none() {
                eprintln!(
                    "replay: partition {} has a snapshot with no applied commands; \
                     it is not a resume point, replaying from offset 0",
                    args.partition
                );
            }
            (
                ShardEngine::new_with_snapshot_interval(args.partition, &exchange, u64::MAX),
                None,
                false,
            )
        }
    };

    if args.require_snapshot {
        if let Some(from) = args.from {
            bail!(
                "--require-snapshot: --from {from} starts a clean replay; the run cannot \
                 be seeded from a snapshot"
            );
        }
        let Some(offset) = snapshot_offset else {
            bail!(
                "--require-snapshot: no usable snapshot for partition {} in {} ({}); \
                 refusing to fall back to a full replay",
                args.partition,
                args.snapshot_dir.display(),
                if snapshot_found {
                    "the file has no applied commands"
                } else {
                    "no snapshot file"
                }
            );
        };
        if !seeded {
            bail!(
                "--require-snapshot: the replay was not seeded from the snapshot at \
                 offset {offset}"
            );
        }
        let Some(head) = head else {
            bail!(
                "--require-snapshot: could not read the log head for partition {}; \
                 cannot prove snapshot offset {offset} is before it",
                args.partition
            );
        };
        if offset > head {
            bail!(
                "--require-snapshot: snapshot offset {offset} is ahead of the log head \
                 {head} on partition {}; the snapshot cannot be a resume point",
                args.partition
            );
        }
        if offset >= head {
            bail!(
                "--require-snapshot: snapshot offset {offset} is not before the log head \
                 {head} on partition {}; there is no tail to replay",
                args.partition
            );
        }
    }

    let start = args
        .from
        .unwrap_or_else(|| snapshot_offset.map_or(0, |offset| offset.saturating_add(1)));
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

    // A skipped record is a hole in the proof: the tail was not fully decoded,
    // so matching hashes could still come from a shorter log. Strict mode
    // refuses it; the lenient path reports the count in the summary instead.
    if args.require_snapshot && skipped > 0 {
        bail!(
            "--require-snapshot: skipped {skipped} undecodable record(s) on partition {}; \
             the replay does not prove the whole tail",
            args.partition
        );
    }

    let hash = hex(&engine.state_hash());
    // tail is the number of orders after the snapshot. It is only defined for
    // a seeded run; a full replay has no snapshot to measure from.
    let tail = match (head, snapshot_offset) {
        (Some(head), Some(snapshot_offset)) => Some(head.saturating_sub(snapshot_offset)),
        _ => None,
    };
    let summary = serde_json::json!({
        "partition": args.partition,
        "seeded_from_snapshot": seeded,
        "snapshot_offset": snapshot_offset,
        "engine_consumer": engine_consumer,
        "committed_offset": committed_offset,
        "head": head,
        "tail": tail,
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
    Ok(summary)
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

/// Printed with every retention plan, in the table header and in each JSON row.
pub const RETENTION_PLAN_NOTICE: &str = "read-only plan; this command never deletes data.";

#[derive(Args, Debug, Clone)]
pub struct RetentionPlanArgs {
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Read the Iggy password from a file, as the node does.
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD_FILE")]
    pub iggy_password_file: Option<PathBuf>,
    #[arg(long, default_value = "data/snapshots")]
    pub snapshot_dir: PathBuf,
    /// Partition count the log and snapshots were written with.
    #[arg(long, default_value_t = 16)]
    pub partitions: u32,
    /// Comma-separated symbols the node was started with. Defaults to the
    /// built-in BTC-USD,ETH-USD,SOL-USD set. Pass the node's
    /// `WHIPPET_SYMBOLS` when it ran a different universe.
    #[arg(long, env = "WHIPPET_SYMBOLS")]
    pub symbols: Option<String>,
    /// Records retained behind the oldest consumer/snapshot boundary as a
    /// recovery cushion. `0` plans deletion right up to the boundary.
    #[arg(
        long,
        env = "WHIPPET_RETENTION_SAFETY_MARGIN_RECORDS",
        default_value_t = 10_000
    )]
    pub safety_margin_records: u64,
    /// Print one JSON object per shard instead of a table.
    #[arg(long)]
    pub json: bool,
}

/// One shard's retention plan, as observed from the live journal.
///
/// Read-only by construction: the row carries the verdict and the evidence
/// behind it, and `action` is always `none`. This command has no delete, trim,
/// commit, or append path.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RetentionPlanRow {
    pub partition: u32,
    pub head: u64,
    pub engine_consumer: String,
    pub engine_committed: Option<u64>,
    pub md_consumer: String,
    /// The MD consumer's committed offset on the `md` topic. Diagnostic only:
    /// MD offsets live in a different space and never bound the orders log.
    pub md_topic_committed: Option<u64>,
    /// Maximum `last_offset` of the newest snapshot, after verification.
    pub snapshot_offset: Option<u64>,
    /// `eligible` or `blocked`.
    pub decision: &'static str,
    /// Inclusive offset a future trimmer may delete through, when eligible.
    pub safe_through: Option<u64>,
    pub reason: String,
    /// Always `none`: the plan never deletes.
    pub action: &'static str,
    pub notice: &'static str,
}

/// Connect to Iggy and print the plan. The Iggy read path never commits.
pub async fn run_retention_plan(args: RetentionPlanArgs) -> Result<()> {
    let password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
    let journal = Journal::Iggy(
        IggyJournal::connect(&args.iggy_addr, &args.iggy_user, &password)
            .await
            .with_context(|| format!("connecting to Iggy at {}", args.iggy_addr))?,
    );
    retention_plan_with_journal(args, &journal)
        .await
        .map(|_| ())
}

/// The plan core, with the journal injected so the read-only behaviour can be
/// tested on the in-memory journal. `run_retention_plan` supplies the Iggy
/// journal.
///
/// Returns the rows that were printed, so tests can assert on the verdicts and
/// offsets without scraping stdout. `Blocked` is a valid result and not an
/// error: only I/O, connect, and snapshot-load/verify failures are.
#[doc(hidden)]
pub async fn retention_plan_with_journal(
    args: RetentionPlanArgs,
    journal: &Journal,
) -> Result<Vec<RetentionPlanRow>> {
    if args.partitions == 0 {
        bail!("--partitions must be at least 1");
    }
    let exchange = snapshot_exchange(args.partitions, args.symbols.as_deref())?;
    let store = SnapshotStore::new(args.snapshot_dir.clone());
    // Only shards that own a configured symbol have an active engine consumer.
    let mut shards: Vec<u32> = exchange
        .instruments
        .iter()
        .map(|instrument| instrument.symbol.partition(exchange.partitions))
        .collect();
    shards.sort_unstable();
    shards.dedup();

    // Say it on every run and repeat it in each JSON row: this is a proof, not
    // an action.
    eprintln!("{RETENTION_PLAN_NOTICE}");
    if !args.json {
        println!(
            "{:>9}  {:>10}  {:>10}  {:>10}  {:>10}  {:<8}  {:>12}  reason",
            "partition", "head", "engine", "md-topic", "snapshot", "decision", "safe_through"
        );
    }
    let mut rows = Vec::with_capacity(shards.len());
    for shard in shards {
        let engine_consumer = format!("whippet-engine-p{shard}");
        let md_consumer = format!("whippet-md-p{shard}");
        let engine_committed = journal
            .committed(Topic::Orders, shard, &engine_consumer)
            .await
            .with_context(|| format!("reading committed offset for {engine_consumer}"))?;
        // The head is the last offset in the partition, inclusive. `lag` counts
        // the records after the committed offset, so committed + lag is the
        // head on both journal backends, exactly as `replay` computes it.
        let head = {
            let committed = engine_committed.unwrap_or(0);
            let lag = journal
                .lag(Topic::Orders, shard, committed)
                .await
                .with_context(|| format!("reading lag for partition {shard}"))?;
            committed.saturating_add(lag)
        };
        // The MD consumer commits on the MD topic, whose offsets are a
        // different space from the orders log. It is a diagnostic column only:
        // it must not constrain the orders deletion boundary.
        let md_topic_committed = journal
            .committed(Topic::Md, shard, &md_consumer)
            .await
            .with_context(|| format!("reading committed offset for {md_consumer}"))?;
        // Verify before trusting: a corrupt snapshot is an I/O-class failure,
        // not a boundary the planner may use.
        let snapshot_offset = match store
            .load(shard)
            .with_context(|| format!("loading snapshot for shard {shard}"))?
        {
            Some(snapshot) => {
                ShardEngine::from_snapshot(&exchange, snapshot.clone())
                    .with_context(|| format!("verifying snapshot for shard {shard}"))?;
                snapshot
                    .symbols
                    .iter()
                    .map(|symbol| symbol.last_offset)
                    .max()
            }
            None => None,
        };

        // The orders-log boundary is the engine consumer plus the verified
        // orders snapshot. A missing engine commit is unprovable, so it blocks;
        // a missing MD commit says nothing about the orders log.
        let (decision, reason) = match engine_committed {
            Some(engine) => {
                let input = RetentionInput {
                    head,
                    consumers: vec![(engine_consumer.clone(), engine)],
                    snapshot_offset,
                    safety_margin_records: args.safety_margin_records,
                };
                let decision = evaluate(&input);
                let reason = match &decision {
                    RetentionDecision::Blocked { reason, .. } => reason.clone(),
                    RetentionDecision::Eligible { .. } => explain(&input),
                };
                (decision, reason)
            }
            None => {
                let reason = format!(
                    "no committed consumer: {engine_consumer} has no committed offset, so the \
                     plan cannot prove what it has consumed"
                );
                let decision = RetentionDecision::Blocked {
                    reason: reason.clone(),
                    safe_through: None,
                };
                (decision, reason)
            }
        };

        let row = RetentionPlanRow {
            partition: shard,
            head,
            engine_consumer,
            engine_committed,
            md_consumer,
            md_topic_committed,
            snapshot_offset,
            decision: if decision.is_eligible() {
                "eligible"
            } else {
                "blocked"
            },
            safe_through: decision.safe_through(),
            reason,
            action: "none",
            notice: RETENTION_PLAN_NOTICE,
        };
        if args.json {
            println!("{}", serde_json::to_string(&row)?);
        } else {
            println!(
                "{:>9}  {:>10}  {:>10}  {:>10}  {:>10}  {:<8}  {:>12}  {}",
                row.partition,
                row.head,
                offset_cell(row.engine_committed),
                offset_cell(row.md_topic_committed),
                offset_cell(row.snapshot_offset),
                row.decision,
                offset_cell(row.safe_through),
                row.reason
            );
        }
        rows.push(row);
    }

    if !args.json {
        let eligible = rows.iter().filter(|row| row.decision == "eligible").count();
        println!(
            "{} shard(s): {eligible} eligible, {} blocked; nothing was deleted",
            rows.len(),
            rows.len() - eligible
        );
    }
    Ok(rows)
}

/// Render an optional offset for the table; a missing value is `-`, never 0.
fn offset_cell(offset: Option<u64>) -> String {
    offset.map_or_else(|| "-".to_string(), |offset| offset.to_string())
}

fn optional_cell<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_string(), |value| value.to_string())
}

fn format_timestamp_ns(timestamp_ns: u64) -> String {
    let seconds = (timestamp_ns / 1_000_000_000) as i64;
    let nanos = (timestamp_ns % 1_000_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(seconds, nanos) {
        Some(timestamp) => timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => timestamp_ns.to_string(),
    }
}

fn offset_list(offsets: &[u64]) -> String {
    offsets
        .iter()
        .map(|offset| offset.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Args, Debug, Clone)]
pub struct AuditArgs {
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Read the Iggy password from a file, as the node does.
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD_FILE")]
    pub iggy_password_file: Option<PathBuf>,
    /// Number of audit-topic partitions to scan from offset 0. The topic is
    /// pinned at 4 partitions (ADR 0015); a lower value reads a prefix.
    #[arg(long, default_value_t = AUDIT_TOPIC_PARTITIONS)]
    pub partitions: u32,
    /// Only records written by this operator.
    #[arg(long)]
    pub operator: Option<String>,
    /// Only halt or resume records.
    #[arg(long, value_parser = ["halt", "resume"])]
    pub action: Option<String>,
    /// Only records for this symbol (case-insensitive).
    #[arg(long)]
    pub symbol: Option<String>,
    /// Only records at or after this audit-topic offset (inclusive). This is the
    /// audit topic's offset, not the order_offset the record carries.
    #[arg(long)]
    pub min_offset: Option<u64>,
    /// Stop after this many matching records.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Exit non-zero when any record on the scanned partitions is not valid
    /// audit JSON. Without it, malformed records are counted and skipped.
    #[arg(long)]
    pub strict: bool,
    /// Print one JSON object per matching record instead of a table.
    #[arg(long)]
    pub json: bool,
}

impl AuditArgs {
    fn validate(&self) -> Result<()> {
        if self.partitions == 0 || self.partitions > AUDIT_TOPIC_PARTITIONS {
            bail!(
                "--partitions must be between 1 and {AUDIT_TOPIC_PARTITIONS}; the audit topic is \
                 pinned at {AUDIT_TOPIC_PARTITIONS} partitions (ADR 0015)"
            );
        }
        Ok(())
    }
}

/// What `whippet audit` read and printed.
///
/// `records` is the filtered set, in read order (partition-major, then offset).
/// `scanned` counts every record read, `decoded` the valid audit JSON, and
/// `malformed` the rest.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditReport {
    pub partitions: u32,
    pub scanned: u64,
    pub decoded: u64,
    pub malformed: u64,
    pub records: Vec<AuditRecord>,
}

/// List control-plane audit records. Read-only: it reads the audit topic and
/// never appends, commits, or creates anything.
pub async fn run_audit(args: AuditArgs) -> Result<()> {
    // Validate before connecting so a bad partition count is a usage error, not
    // a connection attempt followed by an error.
    args.validate()?;
    let password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
    let journal = Journal::Iggy(
        IggyJournal::connect(&args.iggy_addr, &args.iggy_user, &password)
            .await
            .with_context(|| format!("connecting to Iggy at {}", args.iggy_addr))?,
    );
    audit_with_journal(args, &journal).await.map(|_| ())
}

/// The audit core, with the journal injected so the read path and the
/// malformed-record policy can be tested on the memory journal.
///
/// Reads every requested audit partition from offset 0, decodes the gateway
/// JSON, then applies [`AuditFilter`]. It never commits: an audit listing must
/// not move any consumer's watermark.
///
/// Returns the report that was printed. With `--strict` and at least one
/// malformed record the records are still printed, then the call fails, so the
/// operator sees the damage and the shell still gets a non-zero exit.
#[doc(hidden)]
pub async fn audit_with_journal(args: AuditArgs, journal: &Journal) -> Result<AuditReport> {
    args.validate()?;
    let filter = AuditFilter {
        operator: args.operator.clone(),
        action: args.action.clone(),
        symbol: args.symbol.clone(),
        min_offset: args.min_offset,
        limit: args.limit,
    };

    let mut records = Vec::new();
    let mut scanned = 0u64;
    let mut malformed = 0u64;
    for partition in 0..args.partitions {
        let consumer = format!("whippet-audit-p{partition}");
        let payloads = read_topic_from_start(journal, Topic::Audit, partition, &consumer).await?;
        for payload in payloads {
            scanned += 1;
            match AuditRecord::decode(partition, payload.offset, &payload.payload) {
                Ok(decoded) => records.push(decoded),
                Err(error) => {
                    malformed += 1;
                    eprintln!(
                        "skip audit partition {partition} offset {}: {error}",
                        payload.offset
                    );
                }
            }
        }
    }
    let decoded = records.len() as u64;
    let matched = filter_audit(&records, &filter);
    let description = filter.describe();

    if args.json {
        for record in &matched {
            println!("{}", serde_json::to_string(record)?);
        }
        if matched.is_empty() {
            eprintln!("no audit records matched ({description})");
        }
        eprintln!(
            "audit: scanned {scanned} record(s) on {} partition(s), {decoded} decoded, \
             {malformed} malformed, {} matched ({description}); read-only, nothing committed",
            args.partitions,
            matched.len()
        );
    } else {
        if matched.is_empty() {
            println!("no audit records matched ({description})");
        } else {
            print_audit_table(&matched);
        }
        println!(
            "audit: scanned {scanned} record(s) on {} partition(s), {decoded} decoded, \
             {malformed} malformed, {} matched ({description}); read-only, nothing committed",
            args.partitions,
            matched.len()
        );
    }

    if args.strict && malformed > 0 {
        bail!("--strict: {malformed} audit record(s) were not valid JSON");
    }
    Ok(AuditReport {
        partitions: args.partitions,
        scanned,
        decoded,
        malformed,
        records: matched,
    })
}

fn print_audit_table(records: &[AuditRecord]) {
    println!(
        "{:>9}  {:>10}  {:<23}  {:<12}  {:<6}  {:<8}  {:<6}  {:>12}  reason",
        "partition",
        "offset",
        "timestamp",
        "operator",
        "action",
        "symbol",
        "halted",
        "order_offset"
    );
    for record in records {
        println!(
            "{:>9}  {:>10}  {:<23}  {:<12}  {:<6}  {:<8}  {:<6}  {:>12}  {}",
            record.partition,
            record.offset,
            format_timestamp_ns(record.payload.timestamp_ns),
            record.payload.operator,
            record.payload.action,
            record.payload.symbol,
            record.payload.halted,
            record.payload.order_offset,
            record.payload.reason.as_deref().unwrap_or("-"),
        );
    }
}

#[derive(Args, Debug, Clone)]
pub struct OrderTraceArgs {
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Read the Iggy password from a file, as the node does.
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD_FILE")]
    pub iggy_password_file: Option<PathBuf>,
    /// Partition that owns the symbol. BTC-USD is partition 1 with 16 partitions.
    #[arg(long, default_value_t = 1)]
    pub partition: u32,
    /// Order id as a u128 decimal or 0x-prefixed hex. A bare 32-digit hex value
    /// (as `OrderId` prints it) is accepted when it contains a hex letter.
    #[arg(long, value_parser = parse_order_id)]
    pub order_id: OrderId,
    /// Restrict the scan to these symbols. Omit to scan every symbol on the
    /// partition.
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<Symbol>,
    /// Print the trace as one JSON document instead of a timeline table.
    #[arg(long)]
    pub json: bool,
    /// Exit non-zero when an execution's cause offset does not resolve to a
    /// decoded order or cancel record for the same symbol.
    #[arg(long)]
    pub verify: bool,
}

/// Reconstruct one order's timeline across the orders and executions topics.
/// Read-only: it reads both topics from offset 0 and never appends or commits.
pub async fn run_order_trace(args: OrderTraceArgs) -> Result<()> {
    let password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
    let journal = Journal::Iggy(
        IggyJournal::connect(&args.iggy_addr, &args.iggy_user, &password)
            .await
            .with_context(|| format!("connecting to Iggy at {}", args.iggy_addr))?,
    );
    order_trace_with_journal(args, &journal).await.map(|_| ())
}

/// The order-trace core, with the journal injected so the reconstruction can be
/// tested on the memory journal.
///
/// Both topics are read from offset 0 on one partition. The orders log is the
/// authority for which commands exist; the executions log is matched by
/// `order_id` (own events) and `maker_order_id` (maker-side legs). The table is
/// printed, then `--verify` turns an unresolved cause offset into a non-zero
/// exit. "No matching records" is a valid empty trace and exits zero.
#[doc(hidden)]
pub async fn order_trace_with_journal(
    args: OrderTraceArgs,
    journal: &Journal,
) -> Result<OrderTrace> {
    let orders_consumer = format!("whippet-order-trace-orders-p{}", args.partition);
    let order_payloads =
        read_topic_from_start(journal, Topic::Orders, args.partition, &orders_consumer).await?;
    let mut orders = Vec::with_capacity(order_payloads.len());
    let mut skipped_orders = 0u64;
    for payload in order_payloads {
        match Command::decode(&payload.payload) {
            Ok(command) => orders.push((payload.offset, command)),
            Err(error) => {
                skipped_orders += 1;
                eprintln!("skip orders offset {}: {error}", payload.offset);
            }
        }
    }

    let executions_consumer = format!("whippet-order-trace-executions-p{}", args.partition);
    let execution_payloads = read_topic_from_start(
        journal,
        Topic::Executions,
        args.partition,
        &executions_consumer,
    )
    .await?;
    let mut executions = Vec::with_capacity(execution_payloads.len());
    let mut skipped_executions = 0u64;
    for payload in execution_payloads {
        match ExecutionReport::decode(&payload.payload) {
            Ok(report) => executions.push(report),
            Err(error) => {
                skipped_executions += 1;
                eprintln!("skip executions offset {}: {error}", payload.offset);
            }
        }
    }

    let symbols = (!args.symbols.is_empty()).then_some(args.symbols.as_slice());
    let trace = build_order_trace(args.order_id, args.partition, symbols, &orders, &executions);

    if args.json {
        if trace.orders.is_empty() && trace.executions.is_empty() {
            eprintln!(
                "no orders or executions reference order id {} on partition {}",
                trace.order_id.to_hex(),
                trace.partition
            );
        }
        if !trace.unresolved_cause_offsets.is_empty() {
            eprintln!(
                "unresolved cause offset(s): {}",
                offset_list(&trace.unresolved_cause_offsets)
            );
        }
        println!("{}", serde_json::to_string_pretty(&trace)?);
    } else {
        print_order_trace(&trace);
        println!(
            "skipped: {skipped_orders} order record(s), {skipped_executions} execution record(s) \
             did not decode; read-only, nothing committed"
        );
    }

    if args.verify && !trace.unresolved_cause_offsets.is_empty() {
        bail!(
            "--verify: cause offset(s) {} do not resolve to a decoded order or cancel record on \
             partition {}",
            offset_list(&trace.unresolved_cause_offsets),
            trace.partition
        );
    }
    Ok(trace)
}

fn print_order_trace(trace: &OrderTrace) {
    println!(
        "order-trace  order_id={}  partition={}",
        trace.order_id.to_hex(),
        trace.partition
    );

    // One merged timeline: an order event sorts at its own offset and an
    // execution sorts at the offset that caused it, so a fill appears
    // immediately after the command that produced it.
    let mut rows: Vec<(u64, u8, u64, String)> = Vec::new();
    for event in &trace.orders {
        rows.push((
            event.offset,
            0,
            0,
            format!(
                "{:>10}  {:>8}  {:>6}  {:<26}  {:<6}  {:>10}  {:>10}  {:>10}  {:<23}  participant={}",
                "orders",
                event.offset,
                "-",
                "-",
                "-",
                optional_cell(event.price_cents),
                optional_cell(event.qty),
                "-",
                format_timestamp_ns(event.timestamp_ns),
                event.participant.0,
            ),
        ));
    }
    for event in &trace.executions {
        let (symbol, cause_offset, seq, kind) = event.identity();
        let role = match event.role {
            ExecRole::Order => "order",
            ExecRole::Maker => "maker",
        };
        let maker = if event.maker_order_id == OrderId(0) {
            "-".to_string()
        } else {
            event.maker_order_id.to_hex()
        };
        rows.push((
            event.cause_offset,
            1,
            event.seq,
            format!(
                "{:>10}  {:>8}  {:>6}  {:<26}  {:<6}  {:>10}  {:>10}  {:>10}  {:<23}  seq={seq} maker={maker} reason={}",
                "executions",
                "-",
                cause_offset,
                format!("{symbol}/{cause_offset}/{seq}/{}", kind.as_str()),
                role,
                event.price,
                event.qty,
                event.remaining,
                format_timestamp_ns(event.timestamp_ns),
                event.reason.as_str(),
            ),
        ));
    }
    rows.sort_unstable_by_key(|(cause_offset, log, seq, _)| (*cause_offset, *log, *seq));

    if rows.is_empty() {
        println!(
            "no orders or executions reference order id {} on partition {}",
            trace.order_id.to_hex(),
            trace.partition
        );
    } else {
        println!(
            "{:>10}  {:>8}  {:>6}  {:<26}  {:<6}  {:>10}  {:>10}  {:>10}  {:<23}  detail",
            "log", "offset", "cause", "identity", "role", "price", "qty", "remaining", "timestamp"
        );
        for (_, _, _, line) in rows {
            println!("{line}");
        }
    }

    let own = trace
        .executions
        .iter()
        .filter(|event| event.role == ExecRole::Order)
        .count();
    let maker = trace.executions.len() - own;
    println!(
        "summary: orders={} executions={} (own={own}, maker={maker}) complete={}",
        trace.orders.len(),
        trace.executions.len(),
        if trace.complete { "yes" } else { "no" }
    );
    if !trace.complete {
        println!(
            "note: no terminal execution report (filled/rejected/cancelled) names this order id; \
             a maker-side fill does not carry the maker's remaining quantity"
        );
    }
    if trace.unresolved_cause_offsets.is_empty() {
        println!(
            "causes: every execution cause offset resolves to a decoded order or cancel record"
        );
    } else {
        println!(
            "causes: unresolved cause offset(s): {}",
            offset_list(&trace.unresolved_cause_offsets)
        );
    }
}

/// Read every record in one topic partition from offset zero.
///
/// Read-only: this polls with `read_payloads_into` and never commits, appends,
/// creates topics, or trims. Iggy can return a transient empty poll while a
/// partition is being materialised, so an empty batch is retried a bounded
/// number of times; the memory journal is synchronous, so an empty batch there
/// is the true end of the log.
async fn read_topic_from_start(
    journal: &Journal,
    topic: Topic,
    partition: u32,
    consumer: &str,
) -> Result<Vec<PayloadRecord>> {
    // Offline scan: the broker's batch cap is 256, and a zero wait returns
    // whatever is already available instead of holding each poll open. A
    // non-zero wait here made a 100k-record topic take minutes.
    const BATCH: u32 = 256;
    const MAX_WAIT_MS: u64 = 0;
    const EMPTY_POLL_LIMIT: u32 = 5;

    let mut records = Vec::new();
    let mut batch = Vec::new();
    let mut offset = 0u64;
    let mut empty_polls = 0u32;
    loop {
        let count = journal
            .read_payloads_into(
                topic,
                partition,
                consumer,
                offset,
                BATCH,
                MAX_WAIT_MS,
                &mut batch,
            )
            .await
            .with_context(|| {
                format!("reading {topic} partition {partition} from offset {offset}")
            })?;
        if count == 0 {
            if !journal.is_iggy() || empty_polls + 1 >= EMPTY_POLL_LIMIT {
                break;
            }
            empty_polls += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        empty_polls = 0;
        for record in &batch {
            // Never move backwards if the broker echoes an older record.
            offset = offset.max(record.offset.saturating_add(1));
            records.push(record.clone());
        }
    }
    Ok(records)
}
