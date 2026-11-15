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
//!
//! With `transport = "tcp"` the same workload goes over the binary
//! length-prefixed ingress instead: one persistent connection per worker,
//! `batch_size` commands per [`TcpRequest::Submit`], pipelined up to
//! `max_in_flight` frames. Each frame carries a stage sample (T0 scheduled
//! deadline, T1 frame written, T2 ack received, T3 ack parsed) so the manifest
//! can say where the latency sits rather than only how large it is.

#![forbid(unsafe_code)]

pub mod fanout;
pub mod soak;

use std::collections::VecDeque;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use whippet_core::api::{
    BatchOrderAck, BatchOrderRequest, CancelRequest, MAX_BATCH_ORDERS, OrderRequest,
};
use whippet_core::config::{ExchangeConfig, InstrumentConfig};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::manifest::{
    BenchManifest, HostInfo, LatencySummary, MANIFEST_SCHEMA_VERSION, RepetitionResult,
    StageLatency, ThroughputSummary, WorkloadSpec,
};
use whippet_core::protocol::{COMMAND_ENCODED_LEN, Command, OrderType, Side};
use whippet_core::tcp::{TcpRequest, TcpResponse, body_len};

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
/// Signed clock offsets do not fit an unsigned HDR histogram, so negative and
/// positive offsets go into two histograms. Values below the sub-bucket count
/// are recorded exactly; larger magnitudes carry HDR's relative precision,
/// which is finer than the network delay inside the offset. One minute of
/// headroom on each side is far more than any sane pair of NTP-synced hosts.
const OFFSET_HIST_MAX_NS: u64 = 60_000_000_000;
/// Upper bound on TCP connections per repetition. More connections add
/// generator work and concurrency; four is enough to keep a serial server busy
/// without turning the generator into the bottleneck.
const MAX_TCP_CONNECTIONS: usize = 64;
/// Default TCP connection pool. The server processes one frame at a time per
/// connection, so a small pool avoids head-of-line blocking on a single socket.
const DEFAULT_TCP_CONNECTIONS: usize = 4;
/// Bytes a connection may have outstanding before the writer stops sending.
/// The server reads one frame, appends, writes the ack, and only then reads the
/// next frame, so writing far ahead only fills socket buffers. 64 KiB per
/// connection times four connections stays under 256 KiB.
const TCP_INFLIGHT_BYTES: usize = 64 * 1024;

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
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("invalid config: {0}")]
    Config(String),
}

impl From<whippet_core::error::CoreError> for BenchError {
    fn from(error: whippet_core::error::CoreError) -> Self {
        BenchError::Protocol(error.to_string())
    }
}

/// Ingress transport. The HTTP/JSON path stays the default so existing scripts
/// and manifests keep their meaning; TCP is opt-in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Http,
    Tcp,
}

impl Transport {
    fn as_str(self) -> &'static str {
        match self {
            Transport::Http => "http",
            Transport::Tcp => "tcp",
        }
    }
}

fn parse_transport(value: &str) -> Result<Transport> {
    match value.trim().to_ascii_lowercase().as_str() {
        "http" => Ok(Transport::Http),
        "tcp" => Ok(Transport::Tcp),
        other => Err(BenchError::Config(format!(
            "transport must be http or tcp, got {other:?}"
        ))),
    }
}

fn default_transport() -> String {
    "http".into()
}

fn default_tcp_addr() -> String {
    "127.0.0.1:8081".into()
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
    /// Ingress transport: `http` (default) or `tcp`.
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Binary TCP ingress address, used only when `transport = "tcp"`.
    #[serde(default = "default_tcp_addr")]
    pub tcp_addr: String,
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
            transport: default_transport(),
            tcp_addr: default_tcp_addr(),
        }
    }
}

/// Stage timestamps for one TCP Submit frame. T0 is the scheduled deadline,
/// T1 is when the frame was written, T2 is when the ack bytes were read, and
/// T3 is when the ack was decoded.
///
/// `server_offset_ns` is the server's ack timestamp minus the client's wall
/// clock at T2. It includes the clock offset between the two hosts, so it is a
/// best-effort offset estimate, not a one-way delay.
#[derive(Debug, Clone, Copy)]
struct StageSample {
    send_us: u64,
    wire_us: u64,
    parse_us: u64,
    server_offset_ns: Option<i64>,
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
    /// Present only on transports that can stamp their stages (TCP today).
    stages: Option<StageSample>,
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

/// Machine-readable steady-state outcome. "Too few windows" is `Skipped`, never
/// `Passed`: a short run cannot support a steady-state claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SteadyVerdict {
    Passed,
    Flagged,
    Skipped,
}

impl SteadyState {
    fn verdict(&self) -> SteadyVerdict {
        match self.drift_pct {
            None => SteadyVerdict::Skipped,
            Some(_) if self.flagged => SteadyVerdict::Flagged,
            Some(_) => SteadyVerdict::Passed,
        }
    }

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
    stages: Vec<StageLatency>,
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
    /// Price in instrument ticks. The HTTP path re-derives this at the
    /// gateway; the TCP path needs the exact tick value for the fixed-layout
    /// command, so it is kept here.
    price: Price,
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

    let transport = parse_transport(&config.transport)?;
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
    let mut steady_flagged = false;
    let mut steady_skipped = false;

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
        match stats.steady.verdict() {
            SteadyVerdict::Flagged => {
                steady_flagged = true;
                tracing::warn!(
                    repetition = index,
                    drift_pct = stats.steady.drift_pct.unwrap_or_default(),
                    "steady-state drift exceeds 5%; the run is not steady state"
                );
            }
            SteadyVerdict::Skipped => steady_skipped = true,
            SteadyVerdict::Passed => {}
        }
        steady_notes.push(stats.steady.note(index));
        mix.merge(&stats.mix);
        repetitions.push(RepetitionResult {
            index,
            started_at,
            duration_secs: stats.measured_secs,
            throughput: stats.throughput.clone(),
            latency: stats.latency.clone(),
            stages: stats.stages.clone(),
        });
    }

    // Headline = the repetition with the median p99, never the best run.
    let mut ordered: Vec<&RepetitionResult> = repetitions.iter().collect();
    ordered.sort_by(|a, b| a.latency.p99.total_cmp(&b.latency.p99));
    let median = ordered[ordered.len() / 2];

    let workload = workload_spec(&config, &exchange, transport);

    let host = host_info();
    let git_sha_value = git_sha();
    let git_dirty_value = git_dirty();
    let image_digest_value = image_digest();
    let mut notes = vec![
        "Open-loop absolute-deadline pacing; latency measured from the scheduled deadline.".into(),
        "M4/Docker Desktop is a dev-loop measurement, not a headline claim.".into(),
        "payload_bytes is a nominal label, not a measured request size; a batch body is larger."
            .into(),
    ];
    notes.push(match transport {
        Transport::Http => {
            "HTTP ingress is a convenience path; the Linux headline uses TCP on a native host."
                .into()
        }
        Transport::Tcp => tcp_note(&config),
    });
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

    // Provenance gaps are recorded, not left blank. A valid dev run is not a
    // failure because the tree is dirty or the image was built locally, but the
    // manifest must say so before anyone quotes it.
    if git_dirty_value {
        notes.push(
            "git_dirty=true: the working tree had uncommitted changes when this run started, so \
             git_sha does not uniquely identify the measured code."
                .into(),
        );
    }
    if git_sha_value == "unknown" {
        notes.push(
            "git_sha is \"unknown\": the harness could not read a commit (git missing or \
             WHIPPET_GIT_SHA unset); this manifest is not tied to a commit."
                .into(),
        );
    }
    if image_digest_value.is_none() {
        notes.push(
            "image_digest is null: WHIPPET_IMAGE_DIGEST was unset or \"unknown\", so this run \
             cannot be tied to a built image."
                .into(),
        );
    }

    if mix.overloaded_slots > 0 {
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

    let steady_verdict = if steady_flagged {
        SteadyVerdict::Flagged
    } else if steady_skipped {
        SteadyVerdict::Skipped
    } else {
        SteadyVerdict::Passed
    };
    let labels = run_labels(
        host.scope_label(),
        mix.overloaded_slots,
        generator_bound_run,
        steady_verdict,
    );

    let manifest = BenchManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        git_sha: git_sha_value,
        git_dirty: git_dirty_value,
        image_digest: image_digest_value,
        host,
        workload,
        throughput: median.throughput.clone(),
        latency: median.latency.clone(),
        stages: median.stages.clone(),
        repetitions,
        notes,
        labels,
    };

    // The honesty rules are part of the run, not an after-the-fact audit: a
    // manifest that fails them is not written.
    manifest
        .check_invariants()
        .map_err(|error| BenchError::Config(format!("manifest invariants: {error}")))?;
    let path = write_manifest(&config.output_dir, &run_id, &manifest)?;
    tracing::info!(
        path = %path.display(),
        p50_us = manifest.latency.p50,
        p99_us = manifest.latency.p99,
        throughput = manifest.throughput.achieved_per_sec,
        "benchmark manifest written"
    );
    Ok(manifest)
}

/// Validity labels in one place so the rules are executable and testable:
/// host scope first, then an overload verdict, a generator verdict, and one
/// steady-state verdict. An invalid run is labelled; it is never made to look
/// like a clean one.
fn run_labels(
    scope: &str,
    overloaded_slots: u64,
    generator_bound_run: bool,
    steady: SteadyVerdict,
) -> Vec<String> {
    let mut labels = vec![scope.to_string()];
    if overloaded_slots > 0 {
        labels.push("invalid-overload".into());
    }
    if generator_bound_run {
        labels.push("generator-bound".into());
    }
    labels.push(
        match steady {
            SteadyVerdict::Passed => "steady-state",
            SteadyVerdict::Flagged => "not-steady-state",
            SteadyVerdict::Skipped => "steady-state-skipped",
        }
        .into(),
    );
    labels
}

/// Write the manifest fully or not at all. Serialization happens before the
/// file is created, the file is created exclusively (a `run_id` may not
/// overwrite an earlier artifact), and a write error removes the partial file
/// instead of leaving truncated JSON that looks like a result.
fn write_manifest(output_dir: &Path, run_id: &str, manifest: &BenchManifest) -> Result<PathBuf> {
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join(format!("{run_id}.json"));
    let json = serde_json::to_vec_pretty(manifest)?;
    let mut file = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(BenchError::Config(format!(
                "manifest {} already exists; a run_id must be unique per run",
                path.display()
            )));
        }
        Err(error) => return Err(error.into()),
    };
    if let Err(error) = file.write_all(&json).and_then(|_| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(&path);
        return Err(error.into());
    }
    Ok(path)
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
    let transport = parse_transport(&config.transport)?;
    if transport == Transport::Tcp && config.tcp_addr.trim().is_empty() {
        return Err(BenchError::Config(
            "tcp_addr must not be empty when transport = tcp".into(),
        ));
    }
    if config.label.trim().is_empty() {
        return Err(BenchError::Config(
            "label must not be empty; it is the workload name in the manifest".into(),
        ));
    }
    if config.mode.trim().is_empty() {
        return Err(BenchError::Config(
            "mode must not be empty; it is the workload mode in the manifest".into(),
        ));
    }
    if config.symbols.is_empty() {
        return Err(BenchError::Config(
            "symbols must contain at least one symbol".into(),
        ));
    }
    for (index, symbol) in config.symbols.iter().enumerate() {
        if symbol.trim().is_empty() {
            return Err(BenchError::Config(format!("symbols[{index}] is empty")));
        }
        if config.symbols[..index].contains(symbol) {
            return Err(BenchError::Config(format!(
                "symbols contains {symbol} more than once"
            )));
        }
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
    match parse_transport(&config.transport) {
        Ok(Transport::Tcp) => format!(
            "TCP batch mode: up to {} commands per Submit frame, paced at {:.2} frames/s ({} \
             orders/s). One frame carries one symbol and includes that slot's orders and cancels. \
             Latency is ack receipt minus the slot deadline, recorded once per order, so a frame \
             of {} orders contributes {} samples.",
            config.batch_size,
            config.rate_per_sec as f64 / config.batch_size as f64,
            config.rate_per_sec,
            config.batch_size,
            config.batch_size,
        ),
        _ => format!(
            "Batch mode: up to {} orders per POST /api/orders/batch, paced at {:.2} batch slots/s \
             ({} orders/s). Latency is batch completion minus the slot deadline, recorded once per \
             order, so a batch of {} orders contributes {} samples. Cancels stay individual \
             POST /api/cancel requests because the batch endpoint accepts orders only.",
            config.batch_size,
            config.rate_per_sec as f64 / config.batch_size as f64,
            config.rate_per_sec,
            config.batch_size,
            config.batch_size,
        ),
    }
}

/// Transport note for a TCP run: connection shape, pipelining, and exactly what
/// each stage field means. The clock-offset caveat is part of the field
/// definition, not a footnote.
fn tcp_note(config: &BenchConfig) -> String {
    let frame_bytes = tcp_frame_bytes(config.batch_size);
    let window = tcp_inflight_window(frame_bytes, config.max_in_flight);
    format!(
        "TCP ingress at {}: {} persistent connection(s), TCP_NODELAY, round-robin batches. The \
         server processes one frame at a time per connection (read, append, ack, then read the \
         next), so each connection keeps at most {window} Submit frame(s) of {frame_bytes} bytes \
         outstanding ({} KiB budget); a reader task drains acks continuously and matches them \
         FIFO. A Submit frame carries one symbol and up to {} commands (orders and cancels). \
         Stage timestamps: T0 scheduled deadline, T1 frame written, T2 ack bytes read, T3 ack \
         parsed. `send_us` = T1-T0, `wire_us` = T2-T1, `parse_us` = T3-T2, one sample per frame; \
         the manifest `stages` array holds their p50/p99. `server_offset_ns` = ack `server_ns` - \
         client wall clock at T2; it includes the clock offset between the hosts and is not a \
         one-way delay.",
        config.tcp_addr,
        tcp_connection_count(),
        TCP_INFLIGHT_BYTES / 1024,
        config.batch_size,
    )
}

fn tcp_connection_count() -> usize {
    env_value("WHIPPET_BENCH_TCP_CONNECTIONS")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_TCP_CONNECTIONS)
        .clamp(1, MAX_TCP_CONNECTIONS)
}

/// Wire size of one Submit frame with `commands` commands: the four-byte
/// length prefix, the kind byte, the u16 command count, and the fixed-layout
/// commands.
fn tcp_frame_bytes(commands: usize) -> usize {
    4 + 1 + 2 + commands * COMMAND_ENCODED_LEN
}

/// Frames a connection may have outstanding. The byte budget keeps a
/// write-ahead client from filling the socket buffers while the server is
/// still appending an earlier frame.
fn tcp_inflight_window(frame_bytes: usize, max_in_flight: usize) -> usize {
    (TCP_INFLIGHT_BYTES / frame_bytes.max(1)).clamp(1, max_in_flight.max(1))
}

fn tcp_token() -> Option<String> {
    env_value("WHIPPET_API_TOKEN")
}

fn workload_spec(
    config: &BenchConfig,
    exchange: &ExchangeConfig,
    transport: Transport,
) -> WorkloadSpec {
    WorkloadSpec {
        name: config.label.clone(),
        mode: config.mode.clone(),
        transport: transport.as_str().into(),
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
    }
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
    let mut scheduled = 0u64;
    let mut order_seq = 0u64;

    // TCP connections are established before the pacer starts, so a bad
    // address or a rejected hello fails the repetition instead of silently
    // queueing jobs into a dead worker.
    let transport = parse_transport(&config.transport)?;
    let tcp_window = tcp_inflight_window(tcp_frame_bytes(config.batch_size), config.max_in_flight);
    let mut tcp_senders: Vec<mpsc::UnboundedSender<TcpJob>> = Vec::new();
    let mut tcp_workers = Vec::new();
    if transport == Transport::Tcp {
        let token = tcp_token();
        for _ in 0..tcp_connection_count() {
            let stream = connect_tcp(&config.tcp_addr, token.as_deref()).await?;
            let (job_tx, job_rx) = mpsc::unbounded_channel::<TcpJob>();
            tcp_senders.push(job_tx);
            tcp_workers.push(tokio::spawn(tcp_worker(
                stream,
                tcp_window,
                Duration::from_millis(config.timeout_ms),
                job_rx,
                tx.clone(),
            )));
        }
    }
    let mut dispatch_error: Option<BenchError> = None;

    // Start the deadline clock only after the connections are ready, so the
    // warmup is not eaten by connection setup.
    let start = Instant::now();
    let measure_start = start + Duration::from_secs(config.warmup_secs);

    // The pacer owns the absolute-deadline clock on its own OS thread. The
    // final spin before a deadline therefore never competes with request tasks
    // on the Tokio workers; the thread only hands scheduled ticks over.
    let (tick_tx, mut tick_rx) = mpsc::channel::<Tick>(TICK_CHANNEL_CAPACITY);
    let total_slots = pacing.total_slots;
    let period_ns = pacing.period_ns;
    let pacer = std::thread::spawn(move || {
        for slot in 0..total_slots {
            let deadline = slot_deadline(start, period_ns, slot);
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
        let slip_us = since_deadline_us(deadline, Instant::now());
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

        let mut orders: Vec<GeneratedOrder> = Vec::with_capacity(slot_orders);
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
                        && open.record(
                            symbol_index,
                            generated.order_id,
                            generated.participant.clone(),
                        )
                    {
                        mix.evicted += 1;
                    }
                    orders.push(generated);
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
                    orders.push(generated);
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
                    orders.push(generated);
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
                        && open.record(
                            symbol_index,
                            generated.order_id,
                            generated.participant.clone(),
                        )
                    {
                        mix.evicted += 1;
                    }
                    orders.push(generated);
                }
            }
        }

        match transport {
            Transport::Tcp => {
                if orders.is_empty() && cancels.is_empty() {
                    continue;
                }
                // All commands in one frame share one symbol and one append.
                // Orders and cancels both fit the fixed-layout command, so the
                // tracked-open-id logic is identical to the HTTP path.
                let timestamp_ns = now_ns();
                let mut commands = Vec::with_capacity(orders.len() + cancels.len());
                for order in &orders {
                    commands.push(command_from_generated(order, timestamp_ns));
                }
                for cancel in &cancels {
                    commands.push(command_from_cancel(cancel, timestamp_ns));
                }
                mix.requests += 1;
                if commands.len() > 1 {
                    mix.batch_requests += 1;
                }
                let weight = commands.len() as u32;
                // Encode before the frame reaches the writer, so `send_us`
                // measures the write path rather than encoder work.
                let frame = TcpRequest::Submit { commands }.encode();
                let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        mix.overloaded_slots += 1;
                        mix.overloaded_orders += slot_orders as u64;
                        continue;
                    }
                };
                let worker = (slot as usize) % tcp_senders.len();
                let job = TcpJob {
                    frame,
                    weight,
                    deadline,
                    slip_us,
                    window,
                    measured,
                    permit,
                };
                if tcp_senders[worker].send(job).is_err() {
                    dispatch_error = Some(BenchError::Config(
                        "a TCP worker stopped accepting frames".into(),
                    ));
                    break;
                }
            }
            Transport::Http => {
                // Serialize each body once, before it reaches a request task,
                // so the measurement does not include serde work on the
                // request path.
                let mut requests: Vec<PreparedRequest> =
                    Vec::with_capacity(orders.len().min(1) + cancels.len());
                if !orders.is_empty() {
                    mix.requests += 1;
                    if batching {
                        mix.batch_requests += 1;
                        let weight = orders.len() as u32;
                        let orders: Vec<OrderRequest> =
                            orders.into_iter().map(|order| order.request).collect();
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
                                body: serde_json::to_vec(&order.request)?,
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

                // One permit per request. If the in-flight cap is saturated
                // the whole slot is dropped as overload: no fake latency
                // sample, no error, just an explicit gap between scheduled and
                // completed samples.
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
        }
    }

    drop(tick_rx);
    let _ = pacer.join();
    // Closing the job senders lets each worker drain its pending acks and
    // exit; only then can every in-flight permit be released.
    drop(tcp_senders);
    let mut worker_error = None;
    for worker in tcp_workers {
        match worker.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => worker_error = Some(error),
            Err(error) => {
                worker_error = Some(BenchError::Config(format!(
                    "TCP worker task failed: {error}"
                )));
            }
        }
    }
    if let Some(error) = dispatch_error.or(worker_error) {
        drop(tx);
        return Err(error);
    }

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
        stages: collected.stages,
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
            let latency_us = since_deadline_us(deadline, completed);
            let _ = tx
                .send(Sample {
                    latency_us,
                    slip_us,
                    ok,
                    weight,
                    window,
                    stages: None,
                })
                .await;
        }
        drop(permit);
    });
}

/// One Submit frame handed to a TCP worker. The frame is already encoded, so
/// the writer only copies bytes.
struct TcpJob {
    frame: Vec<u8>,
    weight: u32,
    deadline: Instant,
    slip_us: u64,
    window: u64,
    measured: bool,
    permit: OwnedSemaphorePermit,
}

struct PendingFrame {
    job: TcpJob,
    written: Instant,
}

/// An ack as the reader saw it. T2 is stamped after the body bytes are read;
/// T3 after the body is decoded. `received_ns` is the wall clock at T2, used
/// only for the best-effort server clock offset.
struct AckEvent {
    received: Instant,
    received_ns: u64,
    parsed: Instant,
    response: TcpResponse,
}

async fn connect_tcp(addr: &str, token: Option<&str>) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    if let Some(token) = token {
        let frame = TcpRequest::Hello {
            token: token.to_string(),
        }
        .encode();
        stream.write_all(&frame).await?;
        match read_tcp_response(&mut stream).await? {
            TcpResponse::HelloAck { ok: true, .. } => {}
            TcpResponse::HelloAck { message, .. } => {
                return Err(BenchError::Config(format!("TCP hello rejected: {message}")));
            }
            other => {
                return Err(BenchError::Config(format!(
                    "unexpected TCP hello response: {other:?}"
                )));
            }
        }
    }
    Ok(stream)
}

async fn read_tcp_response<R: AsyncRead + Unpin>(reader: &mut R) -> Result<TcpResponse> {
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).await?;
    let len = body_len(prefix)?;
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    Ok(TcpResponse::decode_body(&body)?)
}

/// Reads acks continuously on one connection. Keeping the read side in its own
/// task is what lets the writer send the next frame without ever blocking on
/// the ack for the previous one.
async fn tcp_reader<R: AsyncRead + Unpin>(reader: &mut R, tx: mpsc::Sender<AckEvent>) {
    loop {
        let mut prefix = [0u8; 4];
        if reader.read_exact(&mut prefix).await.is_err() {
            break;
        }
        let Ok(len) = body_len(prefix) else {
            break;
        };
        let mut body = vec![0u8; len];
        if reader.read_exact(&mut body).await.is_err() {
            break;
        }
        let received = Instant::now();
        let received_ns = now_ns();
        let Ok(response) = TcpResponse::decode_body(&body) else {
            break;
        };
        let parsed = Instant::now();
        if tx
            .send(AckEvent {
                received,
                received_ns,
                parsed,
                response,
            })
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Wait for the next ack, bounded by the configured request timeout. Both
/// `timeout` and `Receiver::recv` are cancel-safe, so the select in the writer
/// can cancel this future when a new job arrives without losing an ack.
async fn next_ack(
    ack_rx: &mut mpsc::Receiver<AckEvent>,
    ack_timeout: Duration,
) -> Option<AckEvent> {
    tokio::time::timeout(ack_timeout, ack_rx.recv())
        .await
        .ok()
        .flatten()
}

/// One connection's writer. The server handles one frame at a time per
/// connection, so the writer only keeps `window` frames outstanding and the
/// reader task drains acks. Acks are matched FIFO: TCP preserves order and the
/// server processes frames in order.
async fn tcp_worker(
    stream: TcpStream,
    window: usize,
    ack_timeout: Duration,
    mut rx: mpsc::UnboundedReceiver<TcpJob>,
    tx: mpsc::Sender<Sample>,
) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (ack_tx, mut ack_rx) = mpsc::channel::<AckEvent>(window.max(1) * 2);
    let reader_task = tokio::spawn(async move {
        tcp_reader(&mut reader, ack_tx).await;
    });

    let mut pending: VecDeque<PendingFrame> = VecDeque::with_capacity(window.max(1));
    let mut input_open = true;
    loop {
        if !input_open && pending.is_empty() {
            break;
        }
        let can_send = input_open && pending.len() < window;
        if can_send {
            tokio::select! {
                job = rx.recv() => {
                    match job {
                        Some(job) => {
                            writer.write_all(&job.frame).await?;
                            let written = Instant::now();
                            pending.push_back(PendingFrame { job, written });
                        }
                        None => input_open = false,
                    }
                }
                ack = next_ack(&mut ack_rx, ack_timeout) => {
                    let Some(ack) = ack else {
                        fail_pending(&mut pending, &tx).await;
                        return Err(BenchError::Config(
                            "TCP ack did not arrive before the timeout".into(),
                        ));
                    };
                    complete_frame(pending.pop_front(), ack, &tx).await;
                }
            }
        } else if !pending.is_empty() {
            let Some(ack) = next_ack(&mut ack_rx, ack_timeout).await else {
                fail_pending(&mut pending, &tx).await;
                return Err(BenchError::Config(
                    "TCP ack did not arrive before the timeout".into(),
                ));
            };
            complete_frame(pending.pop_front(), ack, &tx).await;
        } else {
            break;
        }
    }

    // Every outstanding frame has an ack, so there is nothing left to read.
    // Half-close the write side and abort the reader; waiting for the server
    // to close would hang, because the server is waiting for the next frame.
    let _ = writer.shutdown().await;
    reader_task.abort();
    let _ = reader_task.await;
    Ok(())
}

async fn complete_frame(pending: Option<PendingFrame>, ack: AckEvent, tx: &mpsc::Sender<Sample>) {
    let Some(PendingFrame { job, written }) = pending else {
        return;
    };
    let TcpJob {
        frame: _,
        weight,
        deadline,
        slip_us,
        window,
        measured,
        permit,
    } = job;
    let (ok, server_offset_ns) = match &ack.response {
        TcpResponse::SubmitAck {
            count, server_ns, ..
        } => (
            *count == weight,
            Some(*server_ns as i64 - ack.received_ns as i64),
        ),
        _ => (false, None),
    };
    if measured {
        let latency_us = since_deadline_us(deadline, ack.received);
        let stages = StageSample {
            send_us: written.saturating_duration_since(deadline).as_micros() as u64,
            wire_us: ack.received.saturating_duration_since(written).as_micros() as u64,
            parse_us: ack
                .parsed
                .saturating_duration_since(ack.received)
                .as_micros() as u64,
            server_offset_ns,
        };
        let _ = tx
            .send(Sample {
                latency_us,
                slip_us,
                ok,
                weight,
                window,
                stages: Some(stages),
            })
            .await;
    }
    drop(permit);
}

/// A connection died with frames still outstanding. Count them as errors
/// rather than dropping them silently: the run should say the acks never came.
async fn fail_pending(pending: &mut VecDeque<PendingFrame>, tx: &mpsc::Sender<Sample>) {
    let now = Instant::now();
    while let Some(PendingFrame { job, .. }) = pending.pop_front() {
        let TcpJob {
            frame: _,
            weight,
            deadline,
            slip_us,
            window,
            measured,
            permit,
        } = job;
        if measured {
            let latency_us = since_deadline_us(deadline, now);
            let _ = tx
                .send(Sample {
                    latency_us,
                    slip_us,
                    ok: false,
                    weight,
                    window,
                    stages: None,
                })
                .await;
        }
        drop(permit);
    }
}

fn command_from_generated(order: &GeneratedOrder, timestamp_ns: u64) -> Command {
    Command::new_order(
        order.request.symbol,
        order.request.side,
        order.request.order_type,
        order.price,
        Qty(order.request.qty),
        order.order_id,
        ParticipantId::derive(&order.participant),
        timestamp_ns,
    )
}

fn command_from_cancel(cancel: &CancelRequest, timestamp_ns: u64) -> Command {
    let participant = cancel.participant.as_deref().unwrap_or_default();
    Command::cancel(
        cancel.symbol,
        cancel.order_id,
        ParticipantId::derive(participant),
        timestamp_ns,
    )
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

struct Collected {
    latency: LatencySummary,
    slip: LatencySummary,
    stages: Vec<StageLatency>,
    completed: u64,
    errors: u64,
    windows: Vec<Histogram<u64>>,
}

async fn collect(mut rx: mpsc::Receiver<Sample>, window_count: usize) -> Collected {
    let mut latency = new_histogram();
    let mut slip = new_histogram();
    let mut send = new_histogram();
    let mut wire = new_histogram();
    let mut parse = new_histogram();
    let mut offset_negative = new_offset_histogram();
    let mut offset_positive = new_offset_histogram();
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
        // Stage histograms are per frame, not per order: every order in a
        // frame shares the frame's send/wire/parse times.
        if let Some(stage) = sample.stages {
            let _ = send.record(stage.send_us.min(HISTOGRAM_MAX_US - 1));
            let _ = wire.record(stage.wire_us.min(HISTOGRAM_MAX_US - 1));
            let _ = parse.record(stage.parse_us.min(HISTOGRAM_MAX_US - 1));
            if let Some(offset_ns) = stage.server_offset_ns {
                record_offset(&mut offset_negative, &mut offset_positive, offset_ns);
            }
        }
    }
    Collected {
        latency: summarize(&latency),
        slip: summarize(&slip),
        stages: stage_summaries(&send, &wire, &parse, &offset_negative, &offset_positive),
        completed,
        errors,
        windows,
    }
}

pub(crate) fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_max(HISTOGRAM_MAX_US, HISTOGRAM_SIG_DIGITS)
        .expect("valid histogram bounds")
}

fn new_offset_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_max(OFFSET_HIST_MAX_NS, HISTOGRAM_SIG_DIGITS)
        .expect("valid offset histogram bounds")
}

fn record_offset(negative: &mut Histogram<u64>, positive: &mut Histogram<u64>, offset_ns: i64) {
    if offset_ns < 0 {
        let magnitude = offset_ns.unsigned_abs().min(OFFSET_HIST_MAX_NS - 1);
        let _ = negative.record(magnitude);
    } else {
        let _ = positive.record((offset_ns as u64).min(OFFSET_HIST_MAX_NS - 1));
    }
}

fn stage_summaries(
    send: &Histogram<u64>,
    wire: &Histogram<u64>,
    parse: &Histogram<u64>,
    offset_negative: &Histogram<u64>,
    offset_positive: &Histogram<u64>,
) -> Vec<StageLatency> {
    let mut stages = Vec::new();
    for (stage, histogram) in [("send_us", send), ("wire_us", wire), ("parse_us", parse)] {
        if !histogram.is_empty() {
            stages.push(StageLatency {
                stage: stage.into(),
                latency: summarize(histogram),
            });
        }
    }
    if !offset_negative.is_empty() || !offset_positive.is_empty() {
        stages.push(StageLatency {
            stage: "server_offset_ns".into(),
            latency: summarize_offset(offset_negative, offset_positive),
        });
    }
    stages
}

/// Merge the two one-sided histograms into ascending signed `(value, count)`
/// pairs. Negative magnitudes are iterated smallest-first, so the signed order
/// is reversed before the two sides are joined.
fn offset_values(negative: &Histogram<u64>, positive: &Histogram<u64>) -> Vec<(i64, u64)> {
    let mut values: Vec<(i64, u64)> = negative
        .iter_recorded()
        .map(|value| (-(value.value_iterated_to() as i64), value.count_at_value()))
        .collect();
    values.extend(
        positive
            .iter_recorded()
            .map(|value| (value.value_iterated_to() as i64, value.count_at_value())),
    );
    values.sort_by_key(|(value, _)| *value);
    values
}

fn signed_quantile(values: &[(i64, u64)], total: u64, quantile: f64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let rank = ((total as f64) * quantile).ceil().max(1.0) as u64;
    let mut seen = 0u64;
    for (value, count) in values {
        seen += *count;
        if seen >= rank {
            return *value as f64;
        }
    }
    values.last().map(|(value, _)| *value as f64).unwrap_or(0.0)
}

/// Summarize the two one-sided offset histograms as signed nanoseconds. Unlike
/// the other stage summaries, this one is in nanoseconds because it describes a
/// clock difference, not a duration.
fn summarize_offset(negative: &Histogram<u64>, positive: &Histogram<u64>) -> LatencySummary {
    let total = negative.len() + positive.len();
    if total == 0 {
        return empty_latency();
    }
    let values = offset_values(negative, positive);
    let sum: i128 = values
        .iter()
        .map(|(value, count)| i128::from(*value) * i128::from(*count))
        .sum();
    LatencySummary {
        count: total,
        min: values
            .first()
            .map(|(value, _)| *value as f64)
            .unwrap_or(0.0),
        mean: sum as f64 / total as f64,
        p50: signed_quantile(&values, total, 0.50),
        p90: signed_quantile(&values, total, 0.90),
        p99: signed_quantile(&values, total, 0.99),
        p999: signed_quantile(&values, total, 0.999),
        p9999: signed_quantile(&values, total, 0.9999),
        max: values.last().map(|(value, _)| *value as f64).unwrap_or(0.0),
        p50_ci95: None,
        p99_ci95: None,
    }
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

fn empty_latency() -> LatencySummary {
    LatencySummary {
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
    }
}

pub(crate) fn summarize(histogram: &Histogram<u64>) -> LatencySummary {
    if histogram.is_empty() {
        return empty_latency();
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

/// A slot's absolute deadline on the open-loop schedule. Every deadline is
/// derived from the run start, so a late wakeup never shifts the schedule.
fn slot_deadline(start: Instant, period_ns: u64, slot: u64) -> Instant {
    start + Duration::from_nanos(period_ns.saturating_mul(slot))
}

/// Coordinated-omission correction: latency is completion minus the absolute
/// deadline, never completion minus the send time. A request that queued
/// before it left still carries its lateness.
fn since_deadline_us(deadline: Instant, at: Instant) -> u64 {
    at.saturating_duration_since(deadline).as_micros() as u64
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
        price: Price(price_ticks),
        passive: offset > 0 && order_type == OrderType::Limit,
    }
}

pub fn scope_label(host: &HostInfo) -> String {
    // One rule for the harness and the manifest: `headline` only for native
    // Linux. Docker Desktop, OrbStack, Linux VMs, macOS, and containers are
    // dev-loop runs regardless of what the host claims.
    host.scope_label().to_string()
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

pub fn host_info() -> HostInfo {
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
    let output = ProcessCommand::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn git_sha() -> String {
    std::env::var("WHIPPET_GIT_SHA")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            command("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into())
        })
}

pub fn git_dirty() -> bool {
    if let Ok(value) = std::env::var("WHIPPET_GIT_DIRTY") {
        return matches!(value.as_str(), "1" | "true" | "TRUE" | "yes");
    }
    command("git", &["status", "--porcelain"]).is_some_and(|value| !value.is_empty())
}

pub fn image_digest() -> Option<String> {
    std::env::var("WHIPPET_IMAGE_DIGEST")
        .ok()
        .filter(|value| !value.is_empty() && value != "unknown")
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
    fn tcp_submit_frame_round_trips_without_a_server() {
        let exchange = ExchangeConfig::default();
        let symbol = Symbol::new("BTC-USD").unwrap();
        let instrument = exchange.instrument(symbol).unwrap();
        let mut rng = ChaCha20Rng::seed_from_u64(11);
        let mut seq = 0u64;
        let mut commands = Vec::new();
        for _ in 0..3 {
            let generated = generate_order(
                symbol,
                instrument,
                &mut rng,
                "run",
                0,
                &mut seq,
                OrderType::Limit,
            );
            commands.push(command_from_generated(&generated, 42));
        }
        commands.push(command_from_cancel(
            &CancelRequest {
                symbol,
                order_id: OrderId(7),
                participant: Some("bench-0-7".into()),
            },
            42,
        ));

        let frame = TcpRequest::Submit {
            commands: commands.clone(),
        }
        .encode();
        let len = body_len(frame[..4].try_into().unwrap()).unwrap();
        assert_eq!(len, frame.len() - 4);
        assert_eq!(len, tcp_frame_bytes(commands.len()) - 4);
        match TcpRequest::decode_body(&frame[4..]).unwrap() {
            TcpRequest::Submit { commands: decoded } => {
                assert_eq!(decoded, commands);
                // One symbol per frame is a protocol requirement.
                assert!(decoded.iter().all(|command| command.symbol == symbol));
            }
            other => panic!("unexpected decoded request: {other:?}"),
        }
    }

    #[test]
    fn tcp_submit_ack_parses_and_keeps_the_server_clock() {
        let ack = TcpResponse::SubmitAck {
            partition: 3,
            base_offset: 100,
            count: 10,
            server_ns: 1_700_000_000_000_000_000,
        };
        let frame = ack.encode();
        let len = body_len(frame[..4].try_into().unwrap()).unwrap();
        assert_eq!(len, frame.len() - 4);
        match TcpResponse::decode_body(&frame[4..]).unwrap() {
            TcpResponse::SubmitAck {
                partition,
                base_offset,
                count,
                server_ns,
            } => {
                assert_eq!(partition, 3);
                assert_eq!(base_offset, 100);
                assert_eq!(count, 10);
                assert_eq!(server_ns, 1_700_000_000_000_000_000);
            }
            other => panic!("unexpected decoded response: {other:?}"),
        }
    }

    #[test]
    fn tcp_batch_pacing_keeps_the_offer_rate() {
        // A 100-command frame at 10k orders/s is one frame every 10 ms.
        let pacing = plan_pacing(10_000, 30, 5, 100).unwrap();
        assert_eq!(pacing.total_orders, 350_000);
        assert_eq!(pacing.total_slots, 3_500);
        assert_eq!(pacing.warmup_slots, 500);
        assert_eq!(pacing.period_ns, 10_000_000);
        let offered: u64 = (0..pacing.total_slots)
            .map(|slot| pacing.orders_in_slot(slot))
            .sum();
        assert_eq!(offered, pacing.total_orders);
        let measured: u64 = (pacing.warmup_slots..pacing.total_slots)
            .map(|slot| pacing.orders_in_slot(slot))
            .sum();
        assert_eq!(measured, 300_000);
    }

    #[test]
    fn tcp_inflight_window_stays_inside_the_byte_budget() {
        for commands in [1usize, 10, 100, 256] {
            let frame_bytes = tcp_frame_bytes(commands);
            let window = tcp_inflight_window(frame_bytes, 4_096);
            assert!(window >= 1);
            assert!(window * frame_bytes <= TCP_INFLIGHT_BYTES);
        }
        // The cap also respects a small max_in_flight.
        assert_eq!(tcp_inflight_window(tcp_frame_bytes(10), 2), 2);
        // 64 KiB / 7,807-byte frame = 8; / 19,975-byte frame = 3.
        assert_eq!(tcp_inflight_window(tcp_frame_bytes(100), 4_096), 8);
        assert_eq!(tcp_inflight_window(tcp_frame_bytes(256), 4_096), 3);
    }

    #[test]
    fn transport_is_recorded_in_the_workload_spec() {
        let exchange = ExchangeConfig::default();
        let default_config = BenchConfig::default();
        assert_eq!(default_config.transport, "http");
        assert_eq!(default_config.tcp_addr, "127.0.0.1:8081");
        let http = workload_spec(&default_config, &exchange, Transport::Http);
        assert_eq!(http.transport, "http");

        let tcp_config = BenchConfig {
            transport: "tcp".into(),
            ..BenchConfig::default()
        };
        let tcp = workload_spec(&tcp_config, &exchange, Transport::Tcp);
        assert_eq!(tcp.transport, "tcp");
        assert_eq!(tcp.batch_size, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_worker_records_one_sample_per_order_and_stages() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Stay up until the client half-closes. Returning immediately after
            // the last ack can race the client's read and turn a good run into
            // an EOF, which would make this test flaky.
            loop {
                let mut prefix = [0u8; 4];
                if socket.read_exact(&mut prefix).await.is_err() {
                    return;
                }
                let Ok(len) = body_len(prefix) else {
                    return;
                };
                let mut body = vec![0u8; len];
                if socket.read_exact(&mut body).await.is_err() {
                    return;
                }
                let Ok(TcpRequest::Submit { commands }) = TcpRequest::decode_body(&body) else {
                    return;
                };
                let ack = TcpResponse::SubmitAck {
                    partition: 1,
                    base_offset: 0,
                    count: commands.len() as u32,
                    server_ns: now_ns(),
                };
                if socket.write_all(&ack.encode()).await.is_err() {
                    return;
                }
            }
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let semaphore = Arc::new(Semaphore::new(4));
        let (job_tx, job_rx) = mpsc::unbounded_channel();
        let (sample_tx, mut sample_rx) = mpsc::channel(8);
        let worker = tokio::spawn(tcp_worker(
            stream,
            2,
            Duration::from_secs(2),
            job_rx,
            sample_tx,
        ));

        let symbol = Symbol::new("BTC-USD").unwrap();
        // The second frame's deadline is deliberately in the past: the pacer
        // was late by 25 ms. Its latency must be completion minus that absolute
        // deadline, not completion minus the write, and each order in the frame
        // inherits the same lateness (weight), not one sample per batch.
        let late_by = Duration::from_millis(25);
        for (position, weight) in [2u32, 3].into_iter().enumerate() {
            let commands: Vec<Command> = (0..weight)
                .map(|index| {
                    Command::new_order(
                        symbol,
                        Side::Buy,
                        OrderType::Limit,
                        Price(100),
                        Qty(1),
                        OrderId(u128::from(index)),
                        ParticipantId(u64::from(index)),
                        0,
                    )
                })
                .collect();
            let frame = TcpRequest::Submit { commands }.encode();
            let permit = Arc::clone(&semaphore).try_acquire_owned().unwrap();
            let (deadline, slip_us) = if position == 0 {
                (Instant::now(), 0)
            } else {
                (Instant::now() - late_by, late_by.as_micros() as u64)
            };
            job_tx
                .send(TcpJob {
                    frame,
                    weight,
                    deadline,
                    slip_us,
                    window: 0,
                    measured: true,
                    permit,
                })
                .unwrap();
        }
        drop(job_tx);

        worker.await.unwrap().unwrap();
        server.await.unwrap();
        let mut weights = Vec::new();
        let mut late_sample_seen = false;
        while let Some(sample) = sample_rx.recv().await {
            assert!(sample.ok);
            assert!(sample.stages.is_some());
            let stages = sample.stages.unwrap();
            assert!(stages.server_offset_ns.is_some());
            // Latency is measured from the deadline, so it is never smaller
            // than the slip recorded when the slot was dispatched.
            assert!(sample.latency_us >= sample.slip_us);
            if sample.slip_us > 0 {
                assert!(
                    sample.latency_us >= late_by.as_micros() as u64,
                    "latency {}µs ignored the {}µs late deadline",
                    sample.latency_us,
                    late_by.as_micros()
                );
                late_sample_seen = true;
            }
            weights.push(sample.weight);
        }
        assert!(late_sample_seen, "the late frame produced no sample");
        // One sample per frame carrying its order count, so 5 orders produce 5
        // latency observations, not 2.
        assert_eq!(weights, [2, 3]);
        assert_eq!(weights.iter().sum::<u32>(), 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_run_records_transport_and_stages_in_the_manifest() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    loop {
                        let mut prefix = [0u8; 4];
                        if socket.read_exact(&mut prefix).await.is_err() {
                            return;
                        }
                        let Ok(len) = body_len(prefix) else {
                            return;
                        };
                        let mut body = vec![0u8; len];
                        if socket.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let count = match TcpRequest::decode_body(&body) {
                            Ok(TcpRequest::Submit { commands }) => commands.len() as u32,
                            _ => return,
                        };
                        let ack = TcpResponse::SubmitAck {
                            partition: 0,
                            base_offset: 0,
                            count,
                            server_ns: now_ns(),
                        };
                        if socket.write_all(&ack.encode()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let output_dir =
            std::env::temp_dir().join(format!("whippet-bench-{}", uuid::Uuid::new_v4()));
        let config = BenchConfig {
            transport: "tcp".into(),
            tcp_addr: addr.to_string(),
            rate_per_sec: 100,
            duration_secs: 1,
            warmup_secs: 0,
            repetitions: 1,
            output_dir: output_dir.clone(),
            label: "tcp-test".into(),
            ..BenchConfig::default()
        };
        let manifest = run(config).await.unwrap();
        server.abort();

        assert_eq!(manifest.workload.transport, "tcp");
        manifest
            .check_invariants()
            .expect("the harness writes only manifests that satisfy the invariants");
        assert!(uuid::Uuid::parse_str(&manifest.run_id).is_ok());
        assert!(!manifest.stages.is_empty());
        let names: Vec<&str> = manifest
            .stages
            .iter()
            .map(|stage| stage.stage.as_str())
            .collect();
        assert!(names.contains(&"send_us"));
        assert!(names.contains(&"wire_us"));
        assert!(names.contains(&"server_offset_ns"));
        let send = manifest
            .stages
            .iter()
            .find(|stage| stage.stage == "send_us")
            .unwrap();
        assert!(send.latency.count > 0);
        assert!(send.latency.p99 >= send.latency.p50);
        // The manifest itself records the transport, not only the config.
        let path = output_dir.join(format!("{}.json", manifest.run_id));
        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("\"transport\": \"tcp\""));
        let restored: BenchManifest = serde_json::from_str(&written).unwrap();
        assert_eq!(restored, manifest);
        restored
            .check_invariants()
            .expect("the on-disk JSON round-trips into a valid manifest");

        // A run_id may not overwrite an earlier artifact: a second write to the
        // same name is refused and the first file is left intact.
        let error = write_manifest(&output_dir, &manifest.run_id, &manifest).unwrap_err();
        assert!(error.to_string().contains("already exists"), "{error}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
        let json_files = std::fs::read_dir(&output_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().to_string_lossy().ends_with(".json"))
            .count();
        assert_eq!(
            json_files, 1,
            "a refused write must not leave a second file"
        );
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_run_does_not_write_a_manifest() {
        let output_dir =
            std::env::temp_dir().join(format!("whippet-bench-{}", uuid::Uuid::new_v4()));
        let config = BenchConfig {
            transport: "tcp".into(),
            // Port 1 is reliably closed on localhost: the repetition fails
            // before the pacer starts, so no manifest can be plausible.
            tcp_addr: "127.0.0.1:1".into(),
            rate_per_sec: 100,
            duration_secs: 1,
            warmup_secs: 0,
            repetitions: 1,
            output_dir: output_dir.clone(),
            label: "failed-test".into(),
            ..BenchConfig::default()
        };
        assert!(run(config).await.is_err());
        assert!(
            !output_dir.exists() || std::fs::read_dir(&output_dir).unwrap().next().is_none(),
            "a failed run left an artifact behind"
        );
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    #[test]
    fn stage_summaries_are_empty_until_a_tcp_frame_records_one() {
        let mut send = new_histogram();
        let mut wire = new_histogram();
        let mut parse = new_histogram();
        let mut negative = new_offset_histogram();
        let mut positive = new_offset_histogram();
        assert!(stage_summaries(&send, &wire, &parse, &negative, &positive).is_empty());

        send.record(120).unwrap();
        wire.record(800).unwrap();
        parse.record(3).unwrap();
        record_offset(&mut negative, &mut positive, -4_000);
        let stages = stage_summaries(&send, &wire, &parse, &negative, &positive);
        let names: Vec<&str> = stages.iter().map(|stage| stage.stage.as_str()).collect();
        assert_eq!(
            names,
            ["send_us", "wire_us", "parse_us", "server_offset_ns"]
        );
        assert_eq!(stages[0].latency.p50, 120.0);
        assert_eq!(stages[1].latency.p50, 800.0);
        assert_eq!(stages[2].latency.p50, 3.0);
        assert!(stages[3].latency.p50 < 0.0);
    }

    #[test]
    fn transport_validation_rejects_unknown_values_and_empty_tcp_addr() {
        assert_eq!(parse_transport("HTTP").unwrap(), Transport::Http);
        assert_eq!(parse_transport("tcp").unwrap(), Transport::Tcp);
        assert!(parse_transport("quic").is_err());

        let unknown = BenchConfig {
            transport: "quic".into(),
            ..BenchConfig::default()
        };
        assert!(validate_config(&unknown).is_err());

        let empty_addr = BenchConfig {
            transport: "tcp".into(),
            tcp_addr: "  ".into(),
            ..BenchConfig::default()
        };
        assert!(validate_config(&empty_addr).is_err());

        let tcp = BenchConfig {
            transport: "tcp".into(),
            ..BenchConfig::default()
        };
        assert!(validate_config(&tcp).is_ok());
    }

    #[test]
    fn server_offset_summary_keeps_the_sign() {
        let mut negative = new_offset_histogram();
        let mut positive = new_offset_histogram();
        for offset in [-5_000i64, -1_000, 0, 2_000, 9_000] {
            record_offset(&mut negative, &mut positive, offset);
        }
        let summary = summarize_offset(&negative, &positive);
        assert_eq!(summary.count, 5);
        // HDR is exact below the sub-bucket count (2,048 ns here); the larger
        // magnitudes carry relative bucket precision, so allow a small band.
        assert!((-5_100.0..=-4_900.0).contains(&summary.min));
        assert!((8_900.0..=9_100.0).contains(&summary.max));
        // Five samples, rank 3: the signed median is exactly zero.
        assert_eq!(summary.p50, 0.0);
        assert!((900.0..=1_100.0).contains(&summary.mean));
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

    /// Nearest-rank percentile over a sorted vector, the brute-force definition
    /// the HDR summaries must agree with.
    fn sorted_percentile(sorted: &[u64], quantile: f64) -> f64 {
        assert!(!sorted.is_empty());
        let rank = ((sorted.len() as f64) * quantile).ceil().max(1.0) as usize;
        sorted[rank.min(sorted.len()) - 1] as f64
    }

    #[test]
    fn summarize_agrees_with_a_sorted_vector_percentile() {
        // Deterministic, deliberately uneven samples so the percentiles land on
        // different values instead of all on the median.
        let samples: Vec<u64> = (1..=10_000u64)
            .map(|index| (index * 37) % 50_000 + 1)
            .chain([1, 60_000])
            .collect();
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let mut histogram = new_histogram();
        for sample in &samples {
            histogram.record(*sample).unwrap();
        }
        let summary = summarize(&histogram);

        assert_eq!(summary.count, samples.len() as u64);
        assert!(summary.min <= sorted[0] as f64);
        assert!(summary.max >= *sorted.last().unwrap() as f64);
        // HDR keeps three significant digits, so a percentile is reported
        // within 0.1% of the brute-force rank. Allow one microsecond of slack.
        for (name, reported, quantile) in [
            ("p50", summary.p50, 0.50),
            ("p90", summary.p90, 0.90),
            ("p99", summary.p99, 0.99),
            ("p999", summary.p999, 0.999),
            ("p9999", summary.p9999, 0.9999),
        ] {
            let expected = sorted_percentile(&sorted, quantile);
            let tolerance = 1.0 + expected * 0.002;
            assert!(
                (reported - expected).abs() <= tolerance,
                "{name}: reported {reported} vs sorted {expected}"
            );
        }
        assert!(summary.min <= summary.p50);
        assert!(summary.p50 <= summary.p99);
        assert!(summary.p99 <= summary.p999);
        assert!(summary.p999 <= summary.max);
    }

    #[test]
    fn slot_deadlines_are_absolute_and_monotonic() {
        let start = Instant::now();
        assert_eq!(slot_deadline(start, 1_000_000, 0), start);
        assert_eq!(
            slot_deadline(start, 1_000_000, 1).duration_since(start),
            Duration::from_millis(1)
        );
        assert_eq!(
            slot_deadline(start, 1_000_000, 1_000).duration_since(start),
            Duration::from_secs(1)
        );
        for slot in 1..64u64 {
            assert!(
                slot_deadline(start, 1_000_000, slot) > slot_deadline(start, 1_000_000, slot - 1)
            );
        }
        // A late wakeup does not move the next deadline: the schedule is
        // absolute, so lateness accumulates as slip instead of being hidden.
        let late = start + Duration::from_millis(5);
        let deadline = slot_deadline(start, 1_000_000, 2);
        assert_eq!(deadline.duration_since(start), Duration::from_millis(2));
        assert_eq!(since_deadline_us(deadline, late), 3_000);
        // An early completion is clamped to zero; latency is never signed.
        assert_eq!(since_deadline_us(deadline, start), 0);
        // A zero-period schedule is not allowed to divide by zero anywhere.
        assert_eq!(slot_deadline(start, 0, 9), start);
    }

    #[test]
    fn steady_state_verdicts_are_skipped_or_flagged_not_passed() {
        let windows = |count: usize, last_value: u64| -> Vec<Histogram<u64>> {
            (0..count)
                .map(|index| {
                    let mut histogram = new_histogram();
                    let value = if index + 10 >= count {
                        last_value
                    } else {
                        1_000
                    };
                    for _ in 0..100 {
                        histogram.record(value).unwrap();
                    }
                    histogram
                })
                .collect()
        };

        // Fewer than 20 windows is skipped, not passed.
        let short = check_steady_state(&windows(19, 1_000));
        assert_eq!(short.verdict(), SteadyVerdict::Skipped);
        let labels = run_labels("dev-loop", 0, false, short.verdict());
        assert!(labels.contains(&"steady-state-skipped".to_string()));
        assert!(!labels.contains(&"steady-state".to_string()));

        // More than 5% drift between the first and last ten windows is flagged.
        let drifting = check_steady_state(&windows(30, 1_200));
        assert_eq!(drifting.verdict(), SteadyVerdict::Flagged);
        let labels = run_labels("dev-loop", 0, false, drifting.verdict());
        assert!(labels.contains(&"not-steady-state".to_string()));
        assert!(!labels.contains(&"steady-state".to_string()));

        // A flat run is within tolerance.
        let flat = check_steady_state(&windows(30, 1_000));
        assert_eq!(flat.verdict(), SteadyVerdict::Passed);
        let labels = run_labels("dev-loop", 0, false, flat.verdict());
        assert!(labels.contains(&"steady-state".to_string()));
    }

    #[test]
    fn an_overloaded_run_is_labelled_and_keeps_its_real_throughput() {
        let labels = run_labels("dev-loop", 7, false, SteadyVerdict::Skipped);
        assert_eq!(
            labels,
            ["dev-loop", "invalid-overload", "steady-state-skipped"]
        );
        let labels = run_labels("dev-loop", 0, true, SteadyVerdict::Passed);
        assert!(labels.contains(&"generator-bound".to_string()));
        // The scope label is always first so a reader cannot miss the host
        // class, whatever else is on the manifest.
        assert_eq!(labels.first().map(String::as_str), Some("dev-loop"));
    }

    #[tokio::test]
    async fn an_overloaded_slot_fabricates_no_latency_sample() {
        // A dropped slot sends nothing to the collector. The summary stays
        // empty and the run is labelled instead of inventing a sample.
        let (tx, rx) = mpsc::channel::<Sample>(8);
        drop(tx);
        let collected = collect(rx, 1).await;
        assert_eq!(collected.completed, 0);
        assert_eq!(collected.errors, 0);
        assert_eq!(collected.latency.count, 0);
        assert_eq!(collected.slip.count, 0);
        assert!(collected.latency.p99 == 0.0);
        let labels = run_labels("dev-loop", 1, false, SteadyVerdict::Skipped);
        assert!(labels.contains(&"invalid-overload".to_string()));
    }

    #[test]
    fn scope_labels_never_call_macos_or_a_container_a_headline() {
        let host = |virtualization: &str, os: &str| HostInfo {
            hostname: "host".into(),
            os: os.into(),
            kernel: "kernel".into(),
            cpu_model: "cpu".into(),
            physical_cores: 8,
            logical_cores: 8,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            virtualization: virtualization.into(),
            docker_version: None,
            governor: None,
            smt_enabled: None,
            isolated_cores: None,
        };
        assert_eq!(scope_label(&host("native", "Linux")), "headline");
        assert_eq!(scope_label(&host("native", "Darwin")), "dev-loop");
        assert_eq!(scope_label(&host("docker-desktop", "Linux")), "dev-loop");
        assert_eq!(scope_label(&host("orbstack", "Linux")), "dev-loop");
        assert_eq!(scope_label(&host("linux-vm", "Linux")), "dev-loop");
        assert_eq!(scope_label(&host("container", "Linux")), "dev-loop");
        // macOS can never produce a headline claim, whatever the CPU is.
        assert_ne!(scope_label(&host("native", "macOS")), "headline");
    }

    #[test]
    fn config_validation_rejects_an_empty_label_or_duplicate_symbols() {
        let empty_label = BenchConfig {
            label: "  ".into(),
            ..BenchConfig::default()
        };
        assert!(validate_config(&empty_label).is_err());

        let duplicate = BenchConfig {
            symbols: vec!["BTC-USD".into(), "BTC-USD".into()],
            ..BenchConfig::default()
        };
        assert!(validate_config(&duplicate).is_err());

        let empty_symbol = BenchConfig {
            symbols: vec!["".into()],
            ..BenchConfig::default()
        };
        assert!(validate_config(&empty_symbol).is_err());
        assert!(validate_config(&BenchConfig::default()).is_ok());
    }
}
