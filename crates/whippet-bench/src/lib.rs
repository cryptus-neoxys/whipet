// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Open-loop benchmark harness.
//!
//! The pacer schedules every request at an absolute deadline on a dedicated OS
//! thread, so its final spin never competes with request tasks on the Tokio
//! workers. Latency is measured from that deadline, not from the moment the
//! request happened to leave, which is the coordinated-omission correction. If
//! the generator cannot keep up, `scheduler_slip` grows and the run says so;
//! slip p99 above 5% of measured p99 labels the run generator-bound. A
//! saturated in-flight cap drops slots as overload and labels the run invalid
//! rather than inventing a latency sample.
//!
//! The workload is a mixed L1 order flow: resting limit orders, IOC orders that
//! never rest, and cancels that reference real tracked order ids. Resting
//! orders are bounded by a per-symbol working set, so a long run cannot fill
//! the engine's order arena and turn into an unbounded-book measurement.
//!
//! With `batch_size > 1` the harness posts `batch_size` same-symbol orders in
//! one request to `/api/orders/batch` and records the batch completion time
//! once per order. That is the coordinated-omission-corrected batch latency:
//! every order inherits the lateness of the request that carried it.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use hdrhistogram::Histogram;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use whippet_core::api::{
    BatchOrderAck, BatchOrderRequest, CancelRequest, MAX_BATCH_ORDERS, OrderRequest,
};
use whippet_core::config::{ExchangeConfig, InstrumentConfig};
use whippet_core::ids::{OrderId, Symbol};
use whippet_core::manifest::{
    BenchManifest, HostInfo, LatencySummary, MANIFEST_SCHEMA_VERSION, RepetitionResult,
    ThroughputSummary, WorkloadSpec,
};
use whippet_core::protocol::{OrderType, Side};

/// A one-second window shorter than this many windows cannot be split into a
/// first and last ten, so the steady-state check is skipped and reported as
/// skipped rather than silently passed.
const STEADY_STATE_MIN_WINDOWS: usize = 20;
/// Number of one-second windows compared at each end of the run.
const STEADY_STATE_WINDOWS: usize = 10;
/// Drift beyond this percentage is called out as not-steady-state.
const STEADY_STATE_DRIFT_PCT: f64 = 5.0;
/// Default share of ingress events that are IOC orders.
pub const DEFAULT_IOC_RATIO: f64 = 0.10;
/// Default share of ingress events that are cancels of tracked open orders.
pub const DEFAULT_CANCEL_RATIO: f64 = 0.10;
/// Lower bound on the tracked open-order working set, per symbol.
const MIN_OPEN_ORDERS_PER_SYMBOL: usize = 512;
/// Upper bound on the tracked working set, per symbol. Even at 100k orders/s
/// this keeps the resting book far below the engine's 250k-order arena.
const MAX_OPEN_ORDERS_PER_SYMBOL: usize = 50_000;
/// Histogram ceiling and precision shared by latency, slip, and windows.
const HISTOGRAM_MAX_US: u64 = 60_000_000;
const HISTOGRAM_SIG_DIGITS: u8 = 3;
/// Ticks buffered between the pacer thread and the request runtime. A full
/// channel blocks the pacer, which shows up as scheduler slip instead of a
/// silent schedule change.
const TICK_CHANNEL_CAPACITY: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("histogram error: {0}")]
    Histogram(String),
    #[error("invalid config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchConfig {
    pub base_url: String,
    pub rate_per_sec: u64,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub repetitions: u32,
    pub seed: u64,
    pub symbols: Vec<String>,
    pub mode: String,
    pub output_dir: PathBuf,
    pub label: String,
    pub max_in_flight: usize,
    pub timeout_ms: u64,
    /// Orders per HTTP request. `1` is the single-order path; more than one
    /// posts to `/api/orders/batch`. Capped at the gateway's 256-order batch
    /// limit because one batch is one Iggy append.
    pub batch_size: usize,
    /// Share of ingress events that are IOC orders. IOC orders never rest.
    pub ioc_ratio: f64,
    /// Share of ingress events that cancel a tracked open order.
    pub cancel_ratio: f64,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:8080".into(),
            rate_per_sec: 1_000,
            duration_secs: 30,
            warmup_secs: 5,
            repetitions: 5,
            seed: 0x5eed_0001,
            symbols: vec!["BTC-USD".into(), "ETH-USD".into(), "SOL-USD".into()],
            mode: "durable".into(),
            output_dir: PathBuf::from("artifacts/bench"),
            label: "L1".into(),
            max_in_flight: 4_096,
            timeout_ms: 5_000,
            batch_size: 1,
            ioc_ratio: DEFAULT_IOC_RATIO,
            cancel_ratio: DEFAULT_CANCEL_RATIO,
        }
    }
}

/// One latency observation. `weight` is the number of orders the request
/// carried: a batch of ten orders contributes ten samples, each with the
/// batch's completion-minus-deadline latency. Only requests that were actually
/// issued produce a sample; an overload drop must not fabricate a latency.
struct Sample {
    latency_us: u64,
    slip_us: u64,
    ok: bool,
    weight: u32,
    /// One-second bucket, indexed from the start of the measured window.
    window: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Limit,
    Ioc,
    Cancel,
}

/// The effective event after applying the working-set cap and the empty-id
/// fallback. Kept pure so the mix rules can be tested without a network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventPlan {
    Limit,
    Ioc,
    Cancel,
    /// The tracked set is full, so a resting limit becomes a cancel.
    ForcedCancel,
    /// The tracked set is full and has no ids, so the limit becomes an IOC.
    ForcedIoc,
    /// A cancel was selected but no open id is tracked; send a new order.
    FallbackLimit,
}

fn choose_action(roll: f64, ioc_ratio: f64, cancel_ratio: f64) -> Action {
    let cancel_edge = cancel_ratio.clamp(0.0, 1.0);
    let ioc_edge = (cancel_ratio + ioc_ratio).clamp(0.0, 1.0);
    if roll < cancel_edge {
        Action::Cancel
    } else if roll < ioc_edge {
        Action::Ioc
    } else {
        Action::Limit
    }
}

fn plan_event(
    roll: f64,
    ioc_ratio: f64,
    cancel_ratio: f64,
    open_len: usize,
    open_cap: usize,
) -> EventPlan {
    match choose_action(roll, ioc_ratio, cancel_ratio) {
        Action::Limit if open_len >= open_cap => {
            if open_len > 0 {
                EventPlan::ForcedCancel
            } else {
                EventPlan::ForcedIoc
            }
        }
        Action::Cancel if open_len == 0 => EventPlan::FallbackLimit,
        Action::Cancel => EventPlan::Cancel,
        Action::Ioc => EventPlan::Ioc,
        Action::Limit => EventPlan::Limit,
    }
}

/// Per-repetition event counts, including warmup. The manifest reports the
/// achieved mix because the working-set cap and the empty-id fallback can move
/// it away from the configured ratios.
#[derive(Debug, Clone, Copy, Default)]
struct MixCounts {
    limit: u64,
    ioc: u64,
    cancel: u64,
    forced_cancel: u64,
    forced_ioc: u64,
    fallback_limit: u64,
    evicted: u64,
    /// Slots dropped because the in-flight cap was saturated. These are
    /// overload, not errors, and they never enter the latency histograms.
    overloaded_slots: u64,
    overloaded_orders: u64,
    requests: u64,
    batch_requests: u64,
}

impl MixCounts {
    fn merge(&mut self, other: &Self) {
        self.limit += other.limit;
        self.ioc += other.ioc;
        self.cancel += other.cancel;
        self.forced_cancel += other.forced_cancel;
        self.forced_ioc += other.forced_ioc;
        self.fallback_limit += other.fallback_limit;
        self.evicted += other.evicted;
        self.overloaded_slots += other.overloaded_slots;
        self.overloaded_orders += other.overloaded_orders;
        self.requests += other.requests;
        self.batch_requests += other.batch_requests;
    }
}

/// Result of the one-second-window steady-state check.
#[derive(Debug, Clone, Default)]
struct SteadyState {
    windows: usize,
    first_p99_us: f64,
    last_p99_us: f64,
    drift_pct: Option<f64>,
    flagged: bool,
}

impl SteadyState {
    fn note(&self, repetition: u32) -> String {
        match self.drift_pct {
            Some(drift) => {
                let verdict = if self.flagged {
                    "WARNING: not steady state"
                } else {
                    "within tolerance"
                };
                format!(
                    "Repetition {repetition} steady state: first-{STEADY_STATE_WINDOWS}s p99 {:.0}µs, \
                     last-{STEADY_STATE_WINDOWS}s p99 {:.0}µs, drift {drift:+.1}% ({verdict}; \
                     {} one-second windows).",
                    self.first_p99_us, self.last_p99_us, self.windows
                )
            }
            None => format!(
                "Repetition {repetition} steady-state check skipped: {} one-second windows with \
                 samples (need at least {STEADY_STATE_MIN_WINDOWS}).",
                self.windows
            ),
        }
    }
}

struct RepStats {
    throughput: ThroughputSummary,
    latency: LatencySummary,
    measured_secs: f64,
    steady: SteadyState,
    mix: MixCounts,
}

/// Absolute-deadline schedule derived from rate, duration, warmup, and batch
/// size. Pure so the pacing arithmetic is unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pacing {
    total_orders: u64,
    warmup_orders: u64,
    total_slots: u64,
    warmup_slots: u64,
    orders_per_slot: u64,
    period_ns: u64,
}

impl Pacing {
    fn orders_in_slot(&self, slot: u64) -> u64 {
        let start = slot.saturating_mul(self.orders_per_slot);
        self.total_orders
            .saturating_sub(start)
            .min(self.orders_per_slot)
    }
}

fn plan_pacing(
    rate_per_sec: u64,
    duration_secs: u64,
    warmup_secs: u64,
    batch_size: usize,
) -> Result<Pacing> {
    if rate_per_sec == 0 {
        return Err(BenchError::Config("rate_per_sec must be positive".into()));
    }
    if batch_size == 0 {
        return Err(BenchError::Config("batch_size must be at least 1".into()));
    }
    if batch_size > MAX_BATCH_ORDERS {
        return Err(BenchError::Config(format!(
            "batch_size must be at most {MAX_BATCH_ORDERS} (one Iggy message batch)"
        )));
    }
    let orders_per_slot = batch_size as u64;
    let total_orders = rate_per_sec.saturating_mul(duration_secs.saturating_add(warmup_secs));
    let total_slots = total_orders.div_ceil(orders_per_slot);
    // Whole warmup batches only, so the measured count is exact.
    let warmup_slots = rate_per_sec.saturating_mul(warmup_secs) / orders_per_slot;
    let warmup_orders = warmup_slots
        .saturating_mul(orders_per_slot)
        .min(total_orders);
    // Batch slots are paced at rate / batch_size; the final slot may be short.
    let period_ns = (1_000_000_000u64.saturating_mul(orders_per_slot) / rate_per_sec).max(1);
    Ok(Pacing {
        total_orders,
        warmup_orders,
        total_slots,
        warmup_slots,
        orders_per_slot,
        period_ns,
    })
}

/// The tracked working set is sized to saturate during a short warmup and stay
/// bounded for a long run: roughly a quarter-second of passive limits per
/// symbol at the configured rate, never below 512 and never above 50k.
fn open_order_cap(rate_per_sec: u64, symbol_count: usize) -> usize {
    let per_symbol = rate_per_sec / (4 * symbol_count.max(1) as u64);
    usize::try_from(per_symbol)
        .unwrap_or(usize::MAX)
        .clamp(MIN_OPEN_ORDERS_PER_SYMBOL, MAX_OPEN_ORDERS_PER_SYMBOL)
}

struct OpenOrder {
    order_id: OrderId,
    participant: String,
}

/// Bounded per-symbol stack of tracked resting order ids.
///
/// Cancels take the newest id. Price-time priority fills the oldest order at a
/// price first, so the newest tracked order is the one most likely to still be
/// resting when the cancel reaches the engine.
struct OpenOrders {
    per_symbol: Vec<VecDeque<OpenOrder>>,
    cap: usize,
}

impl OpenOrders {
    fn new(symbols: usize, cap: usize) -> Self {
        Self {
            per_symbol: (0..symbols).map(|_| VecDeque::new()).collect(),
            cap: cap.max(1),
        }
    }

    fn len(&self, symbol_index: usize) -> usize {
        self.per_symbol[symbol_index].len()
    }

    /// Returns `true` when an older id had to be evicted to stay under the cap.
    fn record(&mut self, symbol_index: usize, order_id: OrderId, participant: String) -> bool {
        let queue = &mut self.per_symbol[symbol_index];
        let mut evicted = false;
        while queue.len() >= self.cap {
            queue.pop_front();
            evicted = true;
        }
        queue.push_back(OpenOrder {
            order_id,
            participant,
        });
        evicted
    }

    fn take(&mut self, symbol_index: usize) -> Option<OpenOrder> {
        self.per_symbol[symbol_index].pop_back()
    }
}

struct GeneratedOrder {
    request: OrderRequest,
    order_id: OrderId,
    participant: String,
    /// A passive limit may rest; an aggressive limit or an IOC may trade
    /// instead, so it is not tracked as an open order.
    passive: bool,
}

/// A request whose JSON body was serialized once, before it was handed to the
/// request task. The task only writes bytes to the socket, so the measurement
/// does not include serde work inside the request.
struct PreparedRequest {
    url: String,
    body: Vec<u8>,
    weight: u32,
    /// Batch responses are checked against the order count they acknowledged.
    check_batch_ack: bool,
}

pub async fn run(config: BenchConfig) -> Result<BenchManifest> {
    let mut config = config;
    let mut config_notes = Vec::new();
    apply_env_overrides(&mut config, &mut config_notes)?;
    validate_config(&config)?;

    let exchange = ExchangeConfig::default();
    let client = Client::builder()
        .pool_max_idle_per_host(config.max_in_flight.max(1_024))
        .tcp_nodelay(true)
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let mut repetitions = Vec::new();
    let mut mix = MixCounts::default();
    let mut steady_notes = Vec::new();

    for index in 0..config.repetitions.max(1) {
        tracing::info!(
            repetition = index,
            rate = config.rate_per_sec,
            duration = config.duration_secs,
            batch_size = config.batch_size,
            ioc_ratio = config.ioc_ratio,
            cancel_ratio = config.cancel_ratio,
            "starting benchmark repetition"
        );
        let started_at = Utc::now().to_rfc3339();
        let stats = run_repetition(&client, &config, &exchange, &run_id, index).await?;
        if stats.steady.flagged {
            tracing::warn!(
                repetition = index,
                drift_pct = stats.steady.drift_pct.unwrap_or_default(),
                "steady-state drift exceeds 5%; the run is not steady state"
            );
        }
        steady_notes.push(stats.steady.note(index));
        mix.merge(&stats.mix);
        repetitions.push(RepetitionResult {
            index,
            started_at,
            duration_secs: stats.measured_secs,
            throughput: stats.throughput.clone(),
            latency: stats.latency.clone(),
            stages: Vec::new(),
        });
    }

    // Headline = the repetition with the median p99, never the best run.
    let mut ordered: Vec<&RepetitionResult> = repetitions.iter().collect();
    ordered.sort_by(|a, b| a.latency.p99.partial_cmp(&b.latency.p99).unwrap());
    let median = ordered[ordered.len() / 2];

    let workload = WorkloadSpec {
        name: config.label.clone(),
        mode: config.mode.clone(),
        transport: "http".into(),
        payload_bytes: 96,
        symbols: config.symbols.clone(),
        partitions: exchange.partitions,
        rate_per_sec: config.rate_per_sec,
        duration_secs: config.duration_secs,
        warmup_secs: config.warmup_secs,
        repetitions: config.repetitions.max(1),
        subscribers: 0,
        seed: config.seed,
        durability: "persisted".into(),
        consumer_offset_durability: "persisted".into(),
        batch_size: config.batch_size as u32,
        encryption: false,
    };

    let host = host_info();
    let mut labels = vec![scope_label(&host)];
    let mut notes = vec![
        "Open-loop absolute-deadline pacing; latency measured from the scheduled deadline.".into(),
        "M4/Docker Desktop is a dev-loop measurement, not a headline claim.".into(),
        "HTTP ingress is a convenience path; the Linux headline uses TCP on a native host.".into(),
        "payload_bytes is a nominal label, not a measured request size; a batch body is larger."
            .into(),
    ];
    notes.extend(config_notes);
    if let Some(value) = env_value("WHIPPET_SNAPSHOT_EVERY") {
        notes.push(format!(
            "Node snapshot interval: {value} commands (WHIPPET_SNAPSHOT_EVERY). A snapshot write \
             of the order book stalls the shard; a short run with snapshots enabled includes \
             those stalls, and 0 means snapshots were disabled for this run."
        ));
    }
    notes.push(mix_note(&config, &mix));
    notes.push(working_set_note(&config));
    if config.batch_size > 1 {
        notes.push(batch_note(&config));
    }
    notes.extend(steady_notes);

    if mix.overloaded_slots > 0 {
        labels.push("invalid-overload".into());
        notes.push(format!(
            "INVALID as a latency measurement: {} slot(s) ({} orders) were dropped because the \
             in-flight cap of {} was saturated. Drops are overload, not errors, and are excluded \
             from the latency histograms; scheduled_samples exceeds completed_samples by the \
             dropped count. Raise max_in_flight or lower the rate before trusting this run.",
            mix.overloaded_slots, mix.overloaded_orders, config.max_in_flight
        ));
        tracing::warn!(
            overloaded_slots = mix.overloaded_slots,
            overloaded_orders = mix.overloaded_orders,
            "in-flight cap saturated; the run is not valid as a latency measurement"
        );
    }

    let mut generator_bound_run = false;
    for repetition in &repetitions {
        let slip_p99 = repetition.throughput.scheduler_slip.p99;
        let latency_p99 = repetition.latency.p99;
        if generator_bound(slip_p99, latency_p99) {
            generator_bound_run = true;
            notes.push(format!(
                "Repetition {} is generator-bound: scheduler slip p99 {:.0}µs is {:.1}% of \
                 measured p99 {:.0}µs (>5%). The latency tail includes generator lateness; fix \
                 the generator before quoting the tail.",
                repetition.index,
                slip_p99,
                slip_p99 / latency_p99 * 100.0,
                latency_p99
            ));
            tracing::warn!(
                repetition = repetition.index,
                slip_p99_us = slip_p99,
                latency_p99_us = latency_p99,
                "generator-bound run: scheduler slip p99 exceeds 5% of measured latency"
            );
        }
    }
    if generator_bound_run {
        labels.push("generator-bound".into());
    }

    let manifest = BenchManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        git_sha: git_sha(),
        git_dirty: git_dirty(),
        image_digest: std::env::var("WHIPPET_IMAGE_DIGEST")
            .ok()
            .filter(|value| !value.is_empty() && value != "unknown"),
        host,
        workload,
        throughput: median.throughput.clone(),
        latency: median.latency.clone(),
        stages: Vec::new(),
        repetitions,
        notes,
        labels,
    };

    std::fs::create_dir_all(&config.output_dir)?;
    let path = config.output_dir.join(format!("{run_id}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&manifest)?)?;
    tracing::info!(
        path = %path.display(),
        p50_us = manifest.latency.p50,
        p99_us = manifest.latency.p99,
        throughput = manifest.throughput.achieved_per_sec,
        "benchmark manifest written"
    );
    Ok(manifest)
}

fn validate_config(config: &BenchConfig) -> Result<()> {
    plan_pacing(
        config.rate_per_sec,
        config.duration_secs,
        config.warmup_secs,
        config.batch_size,
    )?;
    for (name, value) in [
        ("ioc_ratio", config.ioc_ratio),
        ("cancel_ratio", config.cancel_ratio),
    ] {
        if !(0.0..=1.0).contains(&value) {
            return Err(BenchError::Config(format!(
                "{name} must be between 0 and 1, got {value}"
            )));
        }
    }
    if config.ioc_ratio + config.cancel_ratio > 1.0 {
        return Err(BenchError::Config(format!(
            "ioc_ratio + cancel_ratio must be at most 1, got {}",
            config.ioc_ratio + config.cancel_ratio
        )));
    }
    if config.max_in_flight < config.batch_size {
        return Err(BenchError::Config(format!(
            "max_in_flight ({}) must be at least batch_size ({}) so one slot can be in flight",
            config.max_in_flight, config.batch_size
        )));
    }
    Ok(())
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_ratio(name: &str, raw: &str) -> Result<f64> {
    let value = raw.parse::<f64>().map_err(|_| {
        BenchError::Config(format!(
            "{name} must be a number between 0 and 1, got {raw:?}"
        ))
    })?;
    if !(0.0..=1.0).contains(&value) {
        return Err(BenchError::Config(format!(
            "{name} must be between 0 and 1, got {value}"
        )));
    }
    Ok(value)
}

/// The CLI does not expose the mix ratios yet, so the scripts set them here.
/// Every override is recorded in the manifest notes; the effective values are
/// also in the workload spec, so a run stays reproducible.
fn apply_env_overrides(config: &mut BenchConfig, notes: &mut Vec<String>) -> Result<()> {
    if let Some(raw) = env_value("WHIPPET_BENCH_BATCH_SIZE") {
        let value = raw.parse::<usize>().map_err(|_| {
            BenchError::Config(format!(
                "WHIPPET_BENCH_BATCH_SIZE must be an integer, got {raw:?}"
            ))
        })?;
        config.batch_size = value;
        notes.push(format!(
            "batch_size={value} set by WHIPPET_BENCH_BATCH_SIZE (overrides the CLI value)."
        ));
    }
    if let Some(raw) = env_value("WHIPPET_BENCH_IOC_RATIO") {
        config.ioc_ratio = parse_ratio("WHIPPET_BENCH_IOC_RATIO", &raw)?;
        notes.push(format!(
            "ioc_ratio={} set by WHIPPET_BENCH_IOC_RATIO.",
            config.ioc_ratio
        ));
    }
    if let Some(raw) = env_value("WHIPPET_BENCH_CANCEL_RATIO") {
        config.cancel_ratio = parse_ratio("WHIPPET_BENCH_CANCEL_RATIO", &raw)?;
        notes.push(format!(
            "cancel_ratio={} set by WHIPPET_BENCH_CANCEL_RATIO.",
            config.cancel_ratio
        ));
    }
    Ok(())
}

fn mix_note(config: &BenchConfig, mix: &MixCounts) -> String {
    let total = mix.limit + mix.ioc + mix.cancel;
    let pct = |count: u64| {
        if total == 0 {
            0.0
        } else {
            count as f64 * 100.0 / total as f64
        }
    };
    format!(
        "Achieved mix over {total} order events (warmup included, configured {:.0}% IOC / {:.0}% \
         cancel): limit {:.1}% ({}), IOC {:.1}% ({}), cancel {:.1}% ({}); forced_cancel={}, \
         forced_ioc={}, cancel_fallback={}, tracked_id_evictions={}, overloaded_slots={} ({} orders), \
         ingress_requests={} ({} batch).",
        config.ioc_ratio * 100.0,
        config.cancel_ratio * 100.0,
        pct(mix.limit),
        mix.limit,
        pct(mix.ioc),
        mix.ioc,
        pct(mix.cancel),
        mix.cancel,
        mix.forced_cancel,
        mix.forced_ioc,
        mix.fallback_limit,
        mix.evicted,
        mix.overloaded_slots,
        mix.overloaded_orders,
        mix.requests,
        mix.batch_requests,
    )
}

fn working_set_note(config: &BenchConfig) -> String {
    let cap = open_order_cap(config.rate_per_sec, config.symbols.len().max(1));
    format!(
        "Open-order working set: at most {cap} tracked passive limit ids per symbol. When the cap \
         is reached a resting limit is replaced by a cancel (single-order mode) or an IOC order \
         (batch mode), so a long run cannot grow the resting book without bound. Limit prices span \
         the reference price minus ten ticks to three ticks through it; orders that cross trade \
         instead of resting."
    )
}

fn batch_note(config: &BenchConfig) -> String {
    format!(
        "Batch mode: up to {} orders per POST /api/orders/batch, paced at {:.2} batch slots/s \
         ({} orders/s). Latency is batch completion minus the slot deadline, recorded once per \
         order, so a batch of {} orders contributes {} samples. Cancels stay individual \
         POST /api/cancel requests because the batch endpoint accepts orders only.",
        config.batch_size,
        config.rate_per_sec as f64 / config.batch_size as f64,
        config.rate_per_sec,
        config.batch_size,
        config.batch_size,
    )
}

async fn run_repetition(
    client: &Client,
    config: &BenchConfig,
    exchange: &ExchangeConfig,
    run_id: &str,
    repetition: u32,
) -> Result<RepStats> {
    let pacing = plan_pacing(
        config.rate_per_sec,
        config.duration_secs,
        config.warmup_secs,
        config.batch_size,
    )?;
    let symbols: Vec<Symbol> = config
        .symbols
        .iter()
        .map(|symbol| Symbol::new(symbol))
        .collect::<std::result::Result<_, _>>()
        .map_err(|error| BenchError::Config(error.to_string()))?;
    if symbols.is_empty() {
        return Err(BenchError::Config("no symbols configured".into()));
    }
    for symbol in &symbols {
        if exchange.instrument(*symbol).is_none() {
            return Err(BenchError::Config(format!(
                "unknown symbol {symbol}; the exchange does not configure it"
            )));
        }
    }

    let semaphore = Arc::new(Semaphore::new(config.max_in_flight.max(1)));
    let (tx, rx) = mpsc::channel::<Sample>(1_000_000);
    let window_count = config.duration_secs.max(1) as usize;
    let collector = tokio::spawn(collect(rx, window_count));
    tracing::debug!(
        total_orders = pacing.total_orders,
        warmup_orders = pacing.warmup_orders,
        total_slots = pacing.total_slots,
        period_ns = pacing.period_ns,
        "pacing plan"
    );
    let base = config.base_url.trim_end_matches('/').to_string();
    let order_url = format!("{base}/api/orders");
    let batch_url = format!("{base}/api/orders/batch");
    let cancel_url = format!("{base}/api/cancel");
    let mut rng = ChaCha20Rng::seed_from_u64(config.seed ^ u64::from(repetition));
    let open_cap = open_order_cap(config.rate_per_sec, symbols.len());
    let mut open = OpenOrders::new(symbols.len(), open_cap);
    let mut mix = MixCounts::default();
    let batching = config.batch_size > 1;
    let start = Instant::now();
    let measure_start = start + Duration::from_secs(config.warmup_secs);
    let mut scheduled = 0u64;
    let mut order_seq = 0u64;

    // The pacer owns the absolute-deadline clock on its own OS thread. The
    // final spin before a deadline therefore never competes with request tasks
    // on the Tokio workers; the thread only hands scheduled ticks over.
    let (tick_tx, mut tick_rx) = mpsc::channel::<Tick>(TICK_CHANNEL_CAPACITY);
    let total_slots = pacing.total_slots;
    let period_ns = pacing.period_ns;
    let pacer = std::thread::spawn(move || {
        for slot in 0..total_slots {
            let deadline = start + Duration::from_nanos(period_ns.saturating_mul(slot));
            wait_until(deadline);
            if tick_tx.blocking_send(Tick { slot, deadline }).is_err() {
                break;
            }
        }
    });

    while let Some(tick) = tick_rx.recv().await {
        let slot = tick.slot;
        let deadline = tick.deadline;
        // Slip is measured when the generator hands the slot to the request
        // side, so it includes pacer lateness and async wakeup.
        let slip_us = Instant::now()
            .saturating_duration_since(deadline)
            .as_micros() as u64;
        let measured = slot >= pacing.warmup_slots;
        let slot_orders = pacing.orders_in_slot(slot).max(1) as usize;
        if measured {
            scheduled += slot_orders as u64;
        }
        let window = deadline.saturating_duration_since(measure_start).as_secs();

        let symbol_index = (slot as usize) % symbols.len();
        let symbol = symbols[symbol_index];
        let instrument = exchange
            .instrument(symbol)
            .expect("symbols were validated above");

        let mut orders: Vec<OrderRequest> = Vec::with_capacity(slot_orders);
        let mut cancels: Vec<CancelRequest> = Vec::new();
        for _ in 0..slot_orders {
            let roll: f64 = rng.gen_range(0.0..1.0);
            let plan = plan_event(
                roll,
                config.ioc_ratio,
                config.cancel_ratio,
                open.len(symbol_index),
                open_cap,
            );
            match plan {
                EventPlan::Cancel | EventPlan::ForcedCancel => {
                    let Some(entry) = open.take(symbol_index) else {
                        // `plan_event` only chooses a cancel when an id exists.
                        continue;
                    };
                    mix.cancel += 1;
                    if plan == EventPlan::ForcedCancel {
                        mix.forced_cancel += 1;
                    }
                    cancels.push(CancelRequest {
                        symbol,
                        order_id: entry.order_id,
                        participant: Some(entry.participant),
                    });
                }
                EventPlan::FallbackLimit => {
                    mix.fallback_limit += 1;
                    mix.limit += 1;
                    let generated = generate_order(
                        symbol,
                        instrument,
                        &mut rng,
                        run_id,
                        repetition,
                        &mut order_seq,
                        OrderType::Limit,
                    );
                    if generated.passive
                        && open.record(symbol_index, generated.order_id, generated.participant)
                    {
                        mix.evicted += 1;
                    }
                    orders.push(generated.request);
                }
                EventPlan::ForcedIoc => {
                    mix.forced_ioc += 1;
                    mix.ioc += 1;
                    let generated = generate_order(
                        symbol,
                        instrument,
                        &mut rng,
                        run_id,
                        repetition,
                        &mut order_seq,
                        OrderType::Ioc,
                    );
                    orders.push(generated.request);
                }
                EventPlan::Ioc => {
                    mix.ioc += 1;
                    let generated = generate_order(
                        symbol,
                        instrument,
                        &mut rng,
                        run_id,
                        repetition,
                        &mut order_seq,
                        OrderType::Ioc,
                    );
                    orders.push(generated.request);
                }
                EventPlan::Limit => {
                    mix.limit += 1;
                    let generated = generate_order(
                        symbol,
                        instrument,
                        &mut rng,
                        run_id,
                        repetition,
                        &mut order_seq,
                        OrderType::Limit,
                    );
                    if generated.passive
                        && open.record(symbol_index, generated.order_id, generated.participant)
                    {
                        mix.evicted += 1;
                    }
                    orders.push(generated.request);
                }
            }
        }

        // Serialize each body once, before it reaches a request task, so the
        // measurement does not include serde work on the request path.
        let mut requests: Vec<PreparedRequest> =
            Vec::with_capacity(orders.len().min(1) + cancels.len());
        if !orders.is_empty() {
            mix.requests += 1;
            if batching {
                mix.batch_requests += 1;
                let weight = orders.len() as u32;
                requests.push(PreparedRequest {
                    url: batch_url.clone(),
                    body: serde_json::to_vec(&BatchOrderRequest { orders })?,
                    weight,
                    check_batch_ack: true,
                });
            } else {
                for order in orders {
                    requests.push(PreparedRequest {
                        url: order_url.clone(),
                        body: serde_json::to_vec(&order)?,
                        weight: 1,
                        check_batch_ack: false,
                    });
                }
            }
        }
        for cancel in cancels {
            mix.requests += 1;
            requests.push(PreparedRequest {
                url: cancel_url.clone(),
                body: serde_json::to_vec(&cancel)?,
                weight: 1,
                check_batch_ack: false,
            });
        }

        // One permit per request. If the in-flight cap is saturated the whole
        // slot is dropped as overload: no fake latency sample, no error, just
        // an explicit gap between scheduled and completed samples.
        let mut permits: Vec<OwnedSemaphorePermit> = Vec::with_capacity(requests.len());
        let mut denied = false;
        for _ in 0..requests.len() {
            match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(_) => {
                    denied = true;
                    break;
                }
            }
        }
        if denied {
            mix.overloaded_slots += 1;
            mix.overloaded_orders += slot_orders as u64;
            continue;
        }

        for (request, permit) in requests.into_iter().zip(permits) {
            spawn_ingress(
                client.clone(),
                request,
                permit,
                tx.clone(),
                deadline,
                slip_us,
                window,
                measured,
            );
        }
    }

    drop(tick_rx);
    let _ = pacer.join();

    // Drain in-flight requests before closing the collector.
    let _ = semaphore.acquire_many(config.max_in_flight as u32).await;
    drop(tx);
    let collected = collector
        .await
        .map_err(|error| BenchError::Config(error.to_string()))?;
    // The measured window is exactly the measured order count divided by the
    // offered rate. Warmup is trimmed to whole batches, so this can differ
    // slightly from `duration_secs` in batch mode; using the real span keeps
    // achieved throughput comparable across batch sizes.
    let measured_secs = if config.duration_secs == 0 || config.rate_per_sec == 0 {
        start.elapsed().as_secs_f64()
    } else {
        pacing.total_orders.saturating_sub(pacing.warmup_orders) as f64 / config.rate_per_sec as f64
    };
    let throughput = ThroughputSummary {
        target_per_sec: config.rate_per_sec,
        achieved_per_sec: collected.completed as f64 / measured_secs.max(f64::EPSILON),
        goodput_per_sec: collected.completed.saturating_sub(collected.errors) as f64
            / measured_secs.max(f64::EPSILON),
        error_count: collected.errors,
        scheduled_samples: scheduled,
        completed_samples: collected.completed,
        scheduler_slip: collected.slip,
    };
    let steady = check_steady_state(&collected.windows);
    Ok(RepStats {
        throughput,
        latency: collected.latency,
        measured_secs,
        steady,
        mix,
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_ingress(
    client: Client,
    request: PreparedRequest,
    permit: OwnedSemaphorePermit,
    tx: mpsc::Sender<Sample>,
    deadline: Instant,
    slip_us: u64,
    window: u64,
    measured: bool,
) {
    tokio::spawn(async move {
        let PreparedRequest {
            url,
            body,
            weight,
            check_batch_ack,
        } = request;
        let response = client
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;
        let completed = Instant::now();
        // A 2xx is not enough for a batch: a gateway that silently dropped
        // orders would otherwise be counted as a full success.
        let ok = match response {
            Ok(response) if response.status().is_success() => {
                if check_batch_ack {
                    let expected = weight as usize;
                    response
                        .json::<BatchOrderAck>()
                        .await
                        .map(|ack| ack.count as usize == expected && ack.orders.len() == expected)
                        .unwrap_or(false)
                } else {
                    true
                }
            }
            _ => false,
        };
        if measured {
            let latency_us = completed.saturating_duration_since(deadline).as_micros() as u64;
            let _ = tx
                .send(Sample {
                    latency_us,
                    slip_us,
                    ok,
                    weight,
                    window,
                })
                .await;
        }
        drop(permit);
    });
}

struct Collected {
    latency: LatencySummary,
    slip: LatencySummary,
    completed: u64,
    errors: u64,
    windows: Vec<Histogram<u64>>,
}

async fn collect(mut rx: mpsc::Receiver<Sample>, window_count: usize) -> Collected {
    let mut latency = new_histogram();
    let mut slip = new_histogram();
    let mut windows: Vec<Histogram<u64>> =
        (0..window_count.max(1)).map(|_| new_histogram()).collect();
    let mut completed = 0u64;
    let mut errors = 0u64;
    while let Some(sample) = rx.recv().await {
        let weight = u64::from(sample.weight.max(1));
        completed += weight;
        if !sample.ok {
            errors += weight;
        }
        let latency_us = sample.latency_us.min(HISTOGRAM_MAX_US - 1);
        let _ = latency.record_n(latency_us, weight);
        let _ = slip.record_n(sample.slip_us.min(HISTOGRAM_MAX_US - 1), weight);
        if let Some(window) = windows.get_mut(sample.window as usize) {
            let _ = window.record_n(latency_us, weight);
        }
    }
    Collected {
        latency: summarize(&latency),
        slip: summarize(&slip),
        completed,
        errors,
        windows,
    }
}

fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_max(HISTOGRAM_MAX_US, HISTOGRAM_SIG_DIGITS)
        .expect("valid histogram bounds")
}

/// Compare the pooled p99 of the first and last ten one-second windows.
fn check_steady_state(windows: &[Histogram<u64>]) -> SteadyState {
    let non_empty: Vec<&Histogram<u64>> =
        windows.iter().filter(|window| !window.is_empty()).collect();
    let count = non_empty.len();
    if count < STEADY_STATE_MIN_WINDOWS {
        return SteadyState {
            windows: count,
            ..SteadyState::default()
        };
    }
    let first = merge(&non_empty[..STEADY_STATE_WINDOWS]);
    let last = merge(&non_empty[count - STEADY_STATE_WINDOWS..]);
    let first_p99_us = first.value_at_quantile(0.99) as f64;
    let last_p99_us = last.value_at_quantile(0.99) as f64;
    let drift_pct = if first_p99_us > 0.0 {
        (last_p99_us - first_p99_us) / first_p99_us * 100.0
    } else {
        0.0
    };
    SteadyState {
        windows: count,
        first_p99_us,
        last_p99_us,
        drift_pct: Some(drift_pct),
        flagged: drift_pct.abs() > STEADY_STATE_DRIFT_PCT,
    }
}

fn merge(histograms: &[&Histogram<u64>]) -> Histogram<u64> {
    let mut merged = new_histogram();
    for histogram in histograms {
        let _ = merged.add(*histogram);
    }
    merged
}

fn summarize(histogram: &Histogram<u64>) -> LatencySummary {
    if histogram.is_empty() {
        return LatencySummary {
            count: 0,
            min: 0.0,
            mean: 0.0,
            p50: 0.0,
            p90: 0.0,
            p99: 0.0,
            p999: 0.0,
            p9999: 0.0,
            max: 0.0,
            p50_ci95: None,
            p99_ci95: None,
        };
    }
    LatencySummary {
        count: histogram.len(),
        min: histogram.min() as f64,
        mean: histogram.mean(),
        p50: histogram.value_at_quantile(0.50) as f64,
        p90: histogram.value_at_quantile(0.90) as f64,
        p99: histogram.value_at_quantile(0.99) as f64,
        p999: histogram.value_at_quantile(0.999) as f64,
        p9999: histogram.value_at_quantile(0.9999) as f64,
        max: histogram.max() as f64,
        p50_ci95: None,
        p99_ci95: None,
    }
}

/// One scheduled slot handed from the pacer thread to the request runtime.
struct Tick {
    slot: u64,
    deadline: Instant,
}

/// Sleep until a little before the deadline, then spin the last few tens of
/// microseconds. This runs on a dedicated OS thread, not on a Tokio worker.
fn wait_until(deadline: Instant) {
    const FINAL_SPIN: Duration = Duration::from_micros(40);
    let now = Instant::now();
    if now >= deadline {
        return;
    }
    let remaining = deadline - now;
    if remaining > FINAL_SPIN {
        std::thread::sleep(remaining - FINAL_SPIN);
    }
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

/// A run is generator-bound when its own wakeup lateness is a material share
/// of the measured tail; the latency then says more about the pacer than the
/// system under test.
fn generator_bound(slip_p99_us: f64, latency_p99_us: f64) -> bool {
    latency_p99_us > 0.0 && slip_p99_us > latency_p99_us * 0.05
}

#[allow(clippy::too_many_arguments)]
fn generate_order(
    symbol: Symbol,
    instrument: &InstrumentConfig,
    rng: &mut ChaCha20Rng,
    run_id: &str,
    repetition: u32,
    order_seq: &mut u64,
    order_type: OrderType,
) -> GeneratedOrder {
    let seq = *order_seq;
    *order_seq += 1;
    let client_order_id = format!("bench-{run_id}-{repetition}-{seq}");
    // Unique per order, so the engine's self-trade prevention never fires
    // between benchmark orders.
    let participant = format!("bench-{repetition}-{seq}");
    let order_id = OrderId::derive(symbol, &client_order_id);
    let mid = instrument.reference_price().0;
    // -3..=10 ticks around the reference price. Positive offsets rest on the
    // book; zero and negative offsets cross and trade. Keeping a share of the
    // flow aggressive stops the resting book from growing monotonically.
    let offset = rng.gen_range(-3..=10i64);
    let buy = seq.is_multiple_of(2);
    let price_ticks = if order_type == OrderType::Ioc {
        // IOC is a taker: price through the touch so it trades instead of
        // cancelling unused. `offset.abs()` keeps the ladder distance in 0..=10.
        let distance = offset.abs();
        if buy { mid + distance } else { mid - distance }
    } else if buy {
        mid - offset
    } else {
        mid + offset
    };
    let price_cents = price_ticks * instrument.tick_size_cents;
    let request = OrderRequest {
        symbol,
        side: if buy { Side::Buy } else { Side::Sell },
        order_type,
        price_cents: Some(price_cents),
        qty: rng.gen_range(1..=50),
        client_order_id: Some(client_order_id),
        participant: Some(participant.clone()),
    };
    GeneratedOrder {
        request,
        order_id,
        participant,
        passive: offset > 0 && order_type == OrderType::Limit,
    }
}

fn scope_label(host: &HostInfo) -> String {
    if host.virtualization == "native" && host.os.eq_ignore_ascii_case("linux") {
        "headline".into()
    } else {
        "dev-loop".into()
    }
}

/// A container is never a headline host, whatever `WHIPPET_VIRTUALIZATION`
/// says. This is the harness-side backstop for the Linux runner's checks.
fn virtualization() -> String {
    let container = Path::new("/.dockerenv").exists()
        || std::fs::read_to_string("/proc/1/cgroup")
            .map(|contents| {
                contents.contains("docker")
                    || contents.contains("containerd")
                    || contents.contains("kubepods")
            })
            .unwrap_or(false);
    let declared = env_value("WHIPPET_VIRTUALIZATION");
    if container {
        return match declared.as_deref() {
            Some(value) if value != "native" => value.to_string(),
            _ => "container".into(),
        };
    }
    declared.unwrap_or_else(|| {
        if cfg!(target_os = "linux") {
            "native".into()
        } else {
            "dev-loop".into()
        }
    })
}

fn host_info() -> HostInfo {
    let os = command("uname", &["-s"]).unwrap_or_else(|| std::env::consts::OS.into());
    let kernel = command("uname", &["-r"]).unwrap_or_default();
    let (cpu_model, logical_cores, memory_bytes) = platform_cpu();
    HostInfo {
        hostname: command("hostname", &[]).unwrap_or_else(|| "unknown".into()),
        os,
        kernel,
        cpu_model,
        physical_cores: logical_cores,
        logical_cores,
        memory_bytes,
        virtualization: virtualization(),
        docker_version: command("docker", &["--version"]),
        governor: std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .ok()
            .map(|value| value.trim().to_string()),
        smt_enabled: std::fs::read_to_string("/sys/devices/system/cpu/smt/active")
            .ok()
            .and_then(|value| value.trim().parse::<u8>().ok())
            .map(|value| value == 1),
        isolated_cores: std::env::var("WHIPPET_ISOLATED_CORES").ok(),
    }
}

fn platform_cpu() -> (String, usize, u64) {
    #[cfg(target_os = "macos")]
    {
        let cpu = command("sysctl", &["-n", "machdep.cpu.brand_string"])
            .unwrap_or_else(|| "Apple Silicon".into());
        let cores = command("sysctl", &["-n", "hw.ncpu"])
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
        let memory = command("sysctl", &["-n", "hw.memsize"])
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        (cpu, cores, memory)
    }
    #[cfg(target_os = "linux")]
    {
        let cpu = std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|contents| {
                contents
                    .lines()
                    .find(|line| line.starts_with("model name"))
                    .and_then(|line| line.split(':').nth(1))
                    .map(|value| value.trim().to_string())
            })
            .unwrap_or_else(|| "unknown".into());
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let memory = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|contents| {
                contents
                    .lines()
                    .find(|line| line.starts_with("MemTotal:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|kilobytes| kilobytes * 1024)
            })
            .unwrap_or(0);
        (cpu, cores, memory)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        ("unknown".into(), 1, 0)
    }
}

fn command(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_sha() -> String {
    std::env::var("WHIPPET_GIT_SHA")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            command("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into())
        })
}

fn git_dirty() -> bool {
    if let Ok(value) = std::env::var("WHIPPET_GIT_DIRTY") {
        return matches!(value.as_str(), "1" | "true" | "TRUE" | "yes");
    }
    command("git", &["status", "--porcelain"]).is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_summarizes_to_zero() {
        let histogram = Histogram::<u64>::new_with_max(1_000, 3).unwrap();
        let summary = summarize(&histogram);
        assert_eq!(summary.count, 0);
        assert_eq!(summary.p99, 0.0);
    }

    #[test]
    fn summary_has_ordered_percentiles() {
        let mut histogram = Histogram::<u64>::new_with_max(1_000_000, 3).unwrap();
        for value in 1..=10_000u64 {
            histogram.record(value).unwrap();
        }
        let summary = summarize(&histogram);
        assert!(summary.p50 <= summary.p99);
        assert!(summary.p99 <= summary.p999);
        assert!(summary.max >= 10_000.0);
    }

    #[test]
    fn batch_pacing_matches_the_order_rate() {
        let single = plan_pacing(1_000, 30, 5, 1).unwrap();
        assert_eq!(single.total_orders, 35_000);
        assert_eq!(single.warmup_orders, 5_000);
        assert_eq!(single.total_slots, 35_000);
        assert_eq!(single.period_ns, 1_000_000);

        let batch = plan_pacing(1_000, 30, 5, 10).unwrap();
        assert_eq!(batch.total_orders, 35_000);
        assert_eq!(batch.warmup_orders, 5_000);
        assert_eq!(batch.total_slots, 3_500);
        // Batches are paced at rate / batch_size, so the batch period is ten
        // single-order periods.
        assert_eq!(batch.period_ns, 10_000_000);
        let offered: u64 = (0..batch.total_slots)
            .map(|slot| batch.orders_in_slot(slot))
            .sum();
        assert_eq!(offered, batch.total_orders);
        let measured: u64 = (batch.warmup_slots..batch.total_slots)
            .map(|slot| batch.orders_in_slot(slot))
            .sum();
        assert_eq!(measured, 30_000);
        // Each order in a batch is one latency sample, not one per request.
        assert_eq!(batch.total_slots * 10, 35_000);
    }

    #[test]
    fn batch_pacing_handles_a_short_final_slot() {
        let pacing = plan_pacing(1_000, 30, 5, 7).unwrap();
        assert_eq!(pacing.total_slots, 5_000);
        assert_eq!(pacing.period_ns, 7_000_000);
        let last = pacing.orders_in_slot(pacing.total_slots - 1);
        assert!((1..=7).contains(&last));
        let offered: u64 = (0..pacing.total_slots)
            .map(|slot| pacing.orders_in_slot(slot))
            .sum();
        assert_eq!(offered, pacing.total_orders);
        let measured: u64 = (pacing.warmup_slots..pacing.total_slots)
            .map(|slot| pacing.orders_in_slot(slot))
            .sum();
        assert!((30_000..30_007).contains(&measured));
    }

    #[test]
    fn batch_size_is_validated_against_the_gateway_limit() {
        assert!(plan_pacing(1_000, 10, 1, MAX_BATCH_ORDERS).is_ok());
        assert!(plan_pacing(1_000, 10, 1, MAX_BATCH_ORDERS + 1).is_err());
        assert!(plan_pacing(1_000, 10, 1, 0).is_err());
        assert!(plan_pacing(0, 10, 1, 1).is_err());

        let too_large = BenchConfig {
            batch_size: MAX_BATCH_ORDERS + 1,
            ..BenchConfig::default()
        };
        assert!(validate_config(&too_large).is_err());
        assert!(validate_config(&BenchConfig::default()).is_ok());
    }

    #[test]
    fn mix_ratios_must_fit_in_one_unit() {
        let overlapping = BenchConfig {
            ioc_ratio: 0.6,
            cancel_ratio: 0.6,
            ..BenchConfig::default()
        };
        assert!(validate_config(&overlapping).is_err());
        let negative = BenchConfig {
            ioc_ratio: -0.1,
            cancel_ratio: 0.1,
            ..BenchConfig::default()
        };
        assert!(validate_config(&negative).is_err());
        let nan = BenchConfig {
            ioc_ratio: f64::NAN,
            ..BenchConfig::default()
        };
        assert!(validate_config(&nan).is_err());
        let balanced = BenchConfig {
            ioc_ratio: 0.25,
            cancel_ratio: 0.25,
            ..BenchConfig::default()
        };
        assert!(validate_config(&balanced).is_ok());
    }

    #[test]
    fn ratios_parse_and_reject_garbage() {
        assert_eq!(parse_ratio("X", "0.25").unwrap(), 0.25);
        assert!(parse_ratio("X", "not-a-number").is_err());
        assert!(parse_ratio("X", "1.5").is_err());
    }

    #[test]
    fn cancel_tracking_is_bounded_and_last_in_first_out() {
        let mut open = OpenOrders::new(1, 2);
        assert!(open.take(0).is_none());
        let a = OrderId(1);
        let b = OrderId(2);
        let c = OrderId(3);
        assert!(!open.record(0, a, "p-a".into()));
        assert!(!open.record(0, b, "p-b".into()));
        assert_eq!(open.len(0), 2);
        // The cap evicts the oldest id and says so.
        assert!(open.record(0, c, "p-c".into()));
        assert_eq!(open.len(0), 2);
        // LIFO: the newest tracked order is the most likely to still rest.
        assert_eq!(open.take(0).unwrap().order_id, c);
        assert_eq!(open.take(0).unwrap().order_id, b);
        assert!(open.take(0).is_none());
    }

    #[test]
    fn cancel_without_an_open_id_falls_back_to_a_limit_order() {
        assert_eq!(
            plan_event(0.0, 0.10, 0.10, 0, 512),
            EventPlan::FallbackLimit
        );
        assert_eq!(plan_event(0.0, 0.10, 0.10, 1, 512), EventPlan::Cancel);
        assert_eq!(plan_event(0.05, 0.10, 0.10, 1, 512), EventPlan::Cancel);
        assert_eq!(plan_event(0.15, 0.10, 0.10, 1, 512), EventPlan::Ioc);
        assert_eq!(plan_event(0.50, 0.10, 0.10, 1, 512), EventPlan::Limit);
    }

    #[test]
    fn the_working_set_cap_forces_cancels_instead_of_growth() {
        assert_eq!(
            plan_event(0.50, 0.10, 0.10, 512, 512),
            EventPlan::ForcedCancel
        );
        assert_eq!(plan_event(0.50, 0.10, 0.10, 0, 0), EventPlan::ForcedIoc);
    }

    #[test]
    fn generator_bound_uses_five_percent_of_latency() {
        assert!(generator_bound(60.0, 1_000.0));
        assert!(!generator_bound(40.0, 1_000.0));
        assert!(!generator_bound(10.0, 0.0));
    }

    #[test]
    fn open_order_cap_scales_with_rate_and_stays_bounded() {
        assert_eq!(open_order_cap(1_000, 3), MIN_OPEN_ORDERS_PER_SYMBOL);
        assert_eq!(open_order_cap(100_000, 1), 25_000);
        assert_eq!(open_order_cap(10_000_000, 1), MAX_OPEN_ORDERS_PER_SYMBOL);
    }

    #[test]
    fn steady_state_flags_drift() {
        let windows: Vec<Histogram<u64>> = (0..30)
            .map(|index| {
                let mut histogram = new_histogram();
                let value = if index < 10 { 1_000 } else { 1_200 };
                for _ in 0..100 {
                    histogram.record(value).unwrap();
                }
                histogram
            })
            .collect();
        let steady = check_steady_state(&windows);
        assert!(steady.drift_pct.unwrap() > 19.0);
        assert!(steady.flagged);
        assert_eq!(steady.windows, 30);
    }

    #[test]
    fn steady_state_accepts_a_flat_run() {
        let windows: Vec<Histogram<u64>> = (0..30)
            .map(|_| {
                let mut histogram = new_histogram();
                for _ in 0..100 {
                    histogram.record(1_000).unwrap();
                }
                histogram
            })
            .collect();
        let steady = check_steady_state(&windows);
        assert_eq!(steady.drift_pct, Some(0.0));
        assert!(!steady.flagged);
    }

    #[test]
    fn steady_state_needs_twenty_windows() {
        let windows: Vec<Histogram<u64>> = (0..10)
            .map(|_| {
                let mut histogram = new_histogram();
                histogram.record(1_000).unwrap();
                histogram
            })
            .collect();
        let steady = check_steady_state(&windows);
        assert_eq!(steady.drift_pct, None);
        assert!(!steady.flagged);
        assert_eq!(steady.windows, 10);
    }

    #[test]
    fn generated_orders_are_ladder_valid_and_unique() {
        let exchange = ExchangeConfig::default();
        let symbol = Symbol::new("BTC-USD").unwrap();
        let instrument = exchange.instrument(symbol).unwrap();
        let mut rng = ChaCha20Rng::seed_from_u64(7);
        let mut seq = 0u64;
        let mut participants = std::collections::HashSet::new();
        let mut order_ids = std::collections::HashSet::new();
        let mut passive = 0usize;
        for _ in 0..1_000 {
            let generated = generate_order(
                symbol,
                instrument,
                &mut rng,
                "run",
                0,
                &mut seq,
                OrderType::Limit,
            );
            let cents = generated.request.price_cents.unwrap();
            assert!(instrument.ticks_from_cents(cents).is_ok());
            assert!(generated.request.qty > 0);
            assert!(participants.insert(generated.participant.clone()));
            assert!(order_ids.insert(generated.order_id));
            passive += usize::from(generated.passive);
        }
        assert_eq!(participants.len(), 1_000);
        // Some flow is aggressive, so the resting book does not grow
        // monotonically; most of it still rests.
        assert!(passive > 600 && passive < 900);
    }
}
