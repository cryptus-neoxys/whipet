// Licensed under the Apache License, Version 2.0. See LICENSE.

//! WebSocket fanout benchmark harness.
//!
//! Opens `N` concurrent WebSocket subscribers against `/ws?trades=T`, optionally
//! sends a `Subscribe` filter, drains warmup frames, and measures per-client and
//! aggregate frame rate, byte rate, and inter-frame gap percentiles over the
//! measured window without retaining frame payloads.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use whippet_core::api::{WsClientMessage, WsServerMessage};
use whippet_core::ids::Symbol;
use whippet_core::manifest::{HostInfo, LatencySummary, MANIFEST_SCHEMA_VERSION};

use crate::{
    BenchError, Result, git_dirty, git_sha, host_info, image_digest, new_histogram, scope_label,
    summarize,
};

/// A subscriber with no frame for more than one second is flagged as stalled.
pub const STALL_THRESHOLD_US: u64 = 1_000_000;
const STALL_THRESHOLD: Duration = Duration::from_micros(STALL_THRESHOLD_US);
const HISTOGRAM_MAX_US: u64 = 60_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutConfig {
    pub gateway: String,
    pub subscribers: usize,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub trades: usize,
    pub symbols: Vec<String>,
    pub output_dir: PathBuf,
    pub label: String,
    /// Real ingress load posted at `POST /api/orders` while subscribers run.
    /// `0` (default) disables the driver and preserves the old quiet-book
    /// measurement.
    #[serde(default)]
    pub ingress_rate: u64,
    /// No-subscriber baseline for the comparison mode. Loaded by the example
    /// from `--baseline-metrics PATH`; echoed in the manifest with a note that
    /// compares the `/metrics` deltas with and without subscribers.
    #[serde(default)]
    pub baseline: Option<BaselineSnapshot>,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self {
            gateway: "http://127.0.0.1:8080".into(),
            subscribers: 10,
            duration_secs: 30,
            warmup_secs: 5,
            trades: 0,
            symbols: Vec::new(),
            output_dir: PathBuf::from("artifacts/bench"),
            label: "fanout".into(),
            ingress_rate: 0,
            baseline: None,
        }
    }
}

/// Lightweight summary of one decoded server frame. The full frame is dropped
/// immediately so subscriber memory stays flat across long runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedFrame {
    State {
        sequence: u64,
        symbols: usize,
        trades: usize,
    },
    Notice {
        level: String,
        message: String,
    },
    Trade {
        symbol: Symbol,
        seq: u64,
    },
    Metrics {
        orders_ingressed: u64,
    },
}

/// Parse one WebSocket text frame from the gateway without retaining the
/// underlying `StateView`.
pub fn parse_server_frame(text: &str) -> Result<ParsedFrame> {
    let message: WsServerMessage = serde_json::from_str(text)?;
    Ok(match message {
        WsServerMessage::State { state } => ParsedFrame::State {
            sequence: state.sequence,
            symbols: state.symbols.len(),
            trades: state.trades.len(),
        },
        WsServerMessage::Notice { level, message } => ParsedFrame::Notice { level, message },
        WsServerMessage::Trade { trade } => ParsedFrame::Trade {
            symbol: trade.symbol,
            seq: trade.seq.0,
        },
        WsServerMessage::Metrics { metrics } => ParsedFrame::Metrics {
            orders_ingressed: metrics.orders_ingressed,
        },
    })
}

/// Tracks inter-frame gaps in microseconds and detects stalls (> 1 s without a
/// frame) for a single subscriber.
pub struct GapTracker {
    histogram: Histogram<u64>,
    last_frame_at: Option<Instant>,
    max_gap_us: u64,
    stalled: bool,
}

impl Default for GapTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl GapTracker {
    pub fn new() -> Self {
        Self {
            histogram: new_histogram(),
            last_frame_at: None,
            max_gap_us: 0,
            stalled: false,
        }
    }

    /// Record an explicit inter-frame gap in microseconds.
    pub fn record_gap_us(&mut self, gap_us: u64) {
        if gap_us > STALL_THRESHOLD_US {
            self.stalled = true;
        }
        self.max_gap_us = self.max_gap_us.max(gap_us);
        let clamped = gap_us.min(HISTOGRAM_MAX_US - 1);
        let _ = self.histogram.record(clamped);
    }

    /// Observe a frame arrival at `now`. During warmup (`measured == false`),
    /// only the timestamp is updated so the first measured frame has an
    /// accurate reference point.
    pub fn observe_frame(&mut self, now: Instant, measured: bool, window_start: Instant) {
        if measured {
            let reference = self.last_frame_at.unwrap_or(window_start);
            let gap_us = now.saturating_duration_since(reference).as_micros() as u64;
            if self.last_frame_at.is_some() {
                self.record_gap_us(gap_us);
            } else if gap_us > STALL_THRESHOLD_US {
                self.stalled = true;
                self.max_gap_us = self.max_gap_us.max(gap_us);
            }
        }
        self.last_frame_at = Some(now);
    }

    /// Check trailing silence at the end of the measured window.
    pub fn finish(&mut self, window_start: Instant, window_end: Instant) {
        let reference = self
            .last_frame_at
            .map_or(window_start, |last| last.max(window_start));
        let trailing_us = window_end.saturating_duration_since(reference).as_micros() as u64;
        if trailing_us > STALL_THRESHOLD_US {
            self.stalled = true;
            self.max_gap_us = self.max_gap_us.max(trailing_us);
        }
    }

    pub fn summary(&self) -> LatencySummary {
        summarize(&self.histogram)
    }

    pub fn stalled(&self) -> bool {
        self.stalled
    }

    pub fn max_gap_us(&self) -> u64 {
        self.max_gap_us
    }

    pub fn histogram(&self) -> &Histogram<u64> {
        &self.histogram
    }
}

/// Pure helper that summarizes a slice of inter-frame gaps (in microseconds)
/// and reports whether any gap exceeded the 1-second stall threshold.
pub fn summarize_gaps_us(gaps_us: &[u64]) -> (LatencySummary, bool) {
    let mut tracker = GapTracker::new();
    for &gap_us in gaps_us {
        tracker.record_gap_us(gap_us);
    }
    (tracker.summary(), tracker.stalled())
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub orders_ingressed_total: Option<u64>,
    pub md_sequence: Option<u64>,
    pub process_cpu_seconds_total: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FanoutMetricsScrape {
    pub before: Option<MetricsSnapshot>,
    pub after: Option<MetricsSnapshot>,
    pub orders_ingressed_delta: Option<u64>,
    pub md_sequence_delta: Option<u64>,
    pub process_cpu_seconds_delta: Option<f64>,
}

/// Achieved load of the optional ingress driver (`--ingress-rate R`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngressSummary {
    pub target_rate_per_sec: u64,
    pub sent: u64,
    pub ok: u64,
    pub errors: u64,
    pub elapsed_secs: f64,
    pub achieved_per_sec: f64,
}

/// No-subscriber baseline for the comparison mode. The example loads the
/// `/metrics` deltas from a `--ingress-rate 0`-style run's manifest so the
/// report can show whether subscribers slowed the engine.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BaselineSnapshot {
    pub source: String,
    pub orders_ingressed_delta: Option<u64>,
    pub md_sequence_delta: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSummary {
    pub client_id: usize,
    pub frames: u64,
    pub bytes: u64,
    pub frames_per_sec: f64,
    pub bytes_per_sec: f64,
    pub gap_p50_us: f64,
    pub gap_p99_us: f64,
    pub gap_max_us: f64,
    pub stalled: bool,
    pub parse_errors: u64,
}

/// Fanout benchmark manifest written to `--output-dir`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanoutManifest {
    pub schema_version: u32,
    pub kind: String,
    pub run_id: String,
    pub created_at: String,
    pub git_sha: String,
    pub git_dirty: bool,
    pub image_digest: Option<String>,
    pub scope: String,
    pub label: String,
    pub host: HostInfo,
    pub gateway: String,
    pub subscribers: usize,
    pub trades: usize,
    pub symbols: Vec<String>,
    pub warmup_secs: u64,
    pub duration_secs: u64,
    pub measured_secs: f64,
    pub frames_per_sec: f64,
    pub frames_per_sec_per_client: f64,
    pub bytes_per_sec_per_client: f64,
    pub aggregate_bytes_per_sec: f64,
    pub total_frames: u64,
    pub total_bytes: u64,
    pub stalled_clients: usize,
    pub gap_us: LatencySummary,
    pub clients: Vec<ClientSummary>,
    pub metrics: FanoutMetricsScrape,
    pub ingress: IngressSummary,
    pub baseline: Option<BaselineSnapshot>,
    pub notes: Vec<String>,
    pub labels: Vec<String>,
}

/// Parse Prometheus exposition text for the series tracked by the fanout
/// harness. Missing series remain `None`.
pub fn parse_prometheus_metrics(body: &str) -> MetricsSnapshot {
    let mut snapshot = MetricsSnapshot::default();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(raw_value) = parts.next() else {
            continue;
        };
        let metric_name = name.split('{').next().unwrap_or(name);
        match metric_name {
            "whippet_orders_ingressed_total" => {
                snapshot.orders_ingressed_total = raw_value.parse::<u64>().ok();
            }
            "whippet_md_sequence" => {
                snapshot.md_sequence = raw_value.parse::<u64>().ok();
            }
            "process_cpu_seconds_total" | "whippet_process_cpu_seconds_total" => {
                snapshot.process_cpu_seconds_total = raw_value.parse::<f64>().ok();
            }
            _ => {}
        }
    }
    snapshot
}

pub fn build_metrics_scrape(
    before: Option<MetricsSnapshot>,
    after: Option<MetricsSnapshot>,
) -> FanoutMetricsScrape {
    let orders_ingressed_delta = match (&before, &after) {
        (Some(b), Some(a)) => match (b.orders_ingressed_total, a.orders_ingressed_total) {
            (Some(start), Some(end)) => Some(end.saturating_sub(start)),
            _ => None,
        },
        _ => None,
    };
    let md_sequence_delta = match (&before, &after) {
        (Some(b), Some(a)) => match (b.md_sequence, a.md_sequence) {
            (Some(start), Some(end)) => Some(end.saturating_sub(start)),
            _ => None,
        },
        _ => None,
    };
    let process_cpu_seconds_delta = match (&before, &after) {
        (Some(b), Some(a)) => match (b.process_cpu_seconds_total, a.process_cpu_seconds_total) {
            (Some(start), Some(end)) if end >= start => Some(end - start),
            _ => None,
        },
        _ => None,
    };
    FanoutMetricsScrape {
        before,
        after,
        orders_ingressed_delta,
        md_sequence_delta,
        process_cpu_seconds_delta,
    }
}

pub fn ws_url(gateway: &str, trades: usize) -> Result<String> {
    let trimmed = gateway.trim().trim_end_matches('/');
    let base = if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_string()
    } else {
        return Err(BenchError::Config(format!(
            "gateway URL must start with http://, https://, ws://, or wss://, got {gateway:?}"
        )));
    };
    Ok(format!("{base}/ws?trades={trades}"))
}

pub fn metrics_url(gateway: &str) -> Result<String> {
    let trimmed = gateway.trim().trim_end_matches('/');
    let base = if let Some(rest) = trimmed.strip_prefix("ws://") {
        format!("http://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        return Err(BenchError::Config(format!(
            "gateway URL must start with http://, https://, ws://, or wss://, got {gateway:?}"
        )));
    };
    Ok(format!("{base}/metrics"))
}

/// Absolute-deadline pacing plan for the ingress driver. Pure so the pacing
/// math is unit-testable: `total_orders` POSTs spaced `period_ns` apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressPlan {
    pub total_orders: u64,
    pub period_ns: u64,
}

pub fn plan_ingress(rate_per_sec: u64, total_secs: u64) -> IngressPlan {
    if rate_per_sec == 0 || total_secs == 0 {
        return IngressPlan {
            total_orders: 0,
            period_ns: 0,
        };
    }
    IngressPlan {
        total_orders: rate_per_sec.saturating_mul(total_secs),
        period_ns: (1_000_000_000u64 / rate_per_sec).max(1),
    }
}

/// One minimal same-symbol ladder order. Unique client id and participant per
/// sequence number so repeated runs never collide and the engine's self-trade
/// prevention never fires between driver orders. Prices walk a ±20-tick
/// ladder around a fixed reference; sides alternate.
pub fn ingress_order_body(seq: u64, symbol: &str) -> serde_json::Value {
    let side = if seq.is_multiple_of(2) { "buy" } else { "sell" };
    let ladder = (seq % 40) as i64 - 20;
    serde_json::json!({
        "symbol": symbol,
        "side": side,
        "order_type": "limit",
        "price_cents": 6_500_000 + ladder * 100,
        "qty": 1 + (seq % 50),
        "client_order_id": format!("fanout-ingress-{seq}"),
        "participant": format!("fanout-ingress-p-{seq}"),
    })
}

/// Compare a no-subscriber baseline run with this run's `/metrics` deltas.
/// Pure so the comparison wording is unit-testable.
pub fn baseline_note(
    baseline: &BaselineSnapshot,
    current_orders_delta: Option<u64>,
    current_md_delta: Option<u64>,
    subscribers: usize,
) -> String {
    match (baseline.orders_ingressed_delta, current_orders_delta) {
        (Some(base), Some(current)) => {
            let pct = if base == 0 {
                0.0
            } else {
                (current as f64 - base as f64) / base as f64 * 100.0
            };
            format!(
                "Baseline comparison ({}): no-subscriber orders_ingressed_delta={} vs \
                 {} subscriber(s) orders_ingressed_delta={} ({:+.1}%). A negative drift means \
                 subscribers slowed the engine's ingress; md_sequence_delta baseline={:?} vs current={:?}.",
                baseline.source,
                base,
                subscribers,
                current,
                pct,
                baseline.md_sequence_delta,
                current_md_delta,
            )
        }
        _ => format!(
            "Baseline comparison ({}): /metrics deltas unavailable on one or both sides; \
             no subscriber-overhead conclusion can be drawn.",
            baseline.source,
        ),
    }
}

/// POST `POST /api/orders` at `rate_per_sec` for `total_secs` seconds on an
/// absolute-deadline schedule. A `rate_per_sec` of 0 disables the driver and
/// returns a zero summary. Requests run with bounded concurrency; every
/// issued request is counted as `ok` (2xx) or `errors`.
async fn run_ingress_driver(
    client: &Client,
    gateway: &str,
    rate_per_sec: u64,
    total_secs: u64,
    symbol: &str,
) -> IngressSummary {
    let plan = plan_ingress(rate_per_sec, total_secs);
    if plan.total_orders == 0 {
        return IngressSummary::default();
    }
    let url = format!("{}/api/orders", gateway.trim_end_matches('/'));
    let started = Instant::now();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(512));
    let mut tasks = tokio::task::JoinSet::new();
    for seq in 0..plan.total_orders {
        let deadline = started + Duration::from_nanos(plan.period_ns.saturating_mul(seq));
        let now = Instant::now();
        if deadline > now {
            tokio::time::sleep_until(deadline.into()).await;
        }
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            // Saturated driver: count the unsent slot as an error rather than
            // silently lowering the offered rate.
            tasks.spawn(async move { false });
            continue;
        };
        let body = ingress_order_body(seq, symbol);
        let request = client.post(&url).json(&body).build();
        let Ok(request) = request else {
            drop(permit);
            tasks.spawn(async move { false });
            continue;
        };
        let owned = client.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let response = owned.execute(request).await;
            matches!(response, Ok(response) if response.status().is_success())
        });
    }
    let mut ok = 0u64;
    let mut errors = 0u64;
    while let Some(outcome) = tasks.join_next().await {
        match outcome {
            Ok(true) => ok += 1,
            _ => errors += 1,
        }
    }
    let elapsed_secs = started.elapsed().as_secs_f64();
    let sent = ok + errors;
    IngressSummary {
        target_rate_per_sec: rate_per_sec,
        sent,
        ok,
        errors,
        elapsed_secs,
        achieved_per_sec: sent as f64 / elapsed_secs.max(f64::EPSILON),
    }
}

fn validate_config(config: &FanoutConfig) -> Result<Vec<Symbol>> {
    if config.subscribers == 0 {
        return Err(BenchError::Config("subscribers must be at least 1".into()));
    }
    if config.duration_secs == 0 {
        return Err(BenchError::Config(
            "duration_secs must be at least 1".into(),
        ));
    }
    let _ = ws_url(&config.gateway, config.trades)?;
    let _ = metrics_url(&config.gateway)?;
    config
        .symbols
        .iter()
        .map(|raw| Symbol::new(raw))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| BenchError::Config(error.to_string()))
}

async fn scrape_metrics(client: &Client, gateway: &str) -> Option<MetricsSnapshot> {
    let url = metrics_url(gateway).ok()?;
    let response = client.get(&url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.text().await.ok()?;
    Some(parse_prometheus_metrics(&body))
}

struct ClientRun {
    client_id: usize,
    frames: u64,
    bytes: u64,
    parse_errors: u64,
    tracker: GapTracker,
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn connect_subscriber(url: &str, subscribe_json: Option<&str>) -> Result<WsStream> {
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|error| BenchError::WebSocket(error.to_string()))?;
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        let _ = stream.set_nodelay(true);
    }
    if let Some(payload) = subscribe_json {
        socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .map_err(|error| BenchError::WebSocket(error.to_string()))?;
    }
    Ok(socket)
}

async fn run_subscriber(
    client_id: usize,
    mut socket: WsStream,
    measure_start: Instant,
    measure_end: Instant,
) -> ClientRun {
    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut parse_errors = 0u64;
    let mut tracker = GapTracker::new();

    loop {
        let now = Instant::now();
        if now >= measure_end {
            break;
        }
        let remaining = measure_end.saturating_duration_since(now);
        let wait = remaining.min(STALL_THRESHOLD + Duration::from_millis(50));
        match tokio::time::timeout(wait, socket.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let received = Instant::now();
                if received >= measure_end {
                    break;
                }
                let measured = received >= measure_start;
                let frame_str: &str = text.as_ref();
                let frame_len = frame_str.len() as u64;
                if parse_server_frame(frame_str).is_err() {
                    if measured {
                        parse_errors += 1;
                    }
                    continue;
                }
                tracker.observe_frame(received, measured, measure_start);
                if measured {
                    frames += 1;
                    bytes += frame_len;
                }
            }
            Ok(Some(Ok(Message::Binary(bin)))) => {
                let received = Instant::now();
                if received >= measure_end {
                    break;
                }
                let measured = received >= measure_start;
                let frame_len = bin.len() as u64;
                match std::str::from_utf8(&bin)
                    .ok()
                    .and_then(|text| parse_server_frame(text).ok())
                {
                    Some(_) => {
                        tracker.observe_frame(received, measured, measure_start);
                        if measured {
                            frames += 1;
                            bytes += frame_len;
                        }
                    }
                    None => {
                        if measured {
                            parse_errors += 1;
                        }
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                if socket.send(Message::Pong(payload)).await.is_err() {
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) => {
                // Early disconnect during the measured window counts as a stall if
                // more than 1 s remains or elapsed without frames.
                break;
            }
            Ok(Some(Ok(_))) => {}
            Err(_) => {
                // Timed out waiting for a frame; if we are inside the measured
                // window and waited past the stall threshold, mark stalled and
                // keep listening until measure_end.
                let after_timeout = Instant::now();
                if after_timeout > measure_start {
                    let reference = tracker
                        .last_frame_at
                        .map_or(measure_start, |last| last.max(measure_start));
                    let silent_us = after_timeout
                        .saturating_duration_since(reference)
                        .as_micros() as u64;
                    if silent_us > STALL_THRESHOLD_US {
                        tracker.stalled = true;
                        tracker.max_gap_us = tracker.max_gap_us.max(silent_us);
                    }
                }
            }
        }
    }

    let _ = socket.close(None).await;
    tracker.finish(measure_start, measure_end);
    ClientRun {
        client_id,
        frames,
        bytes,
        parse_errors,
        tracker,
    }
}

pub async fn run_fanout(config: FanoutConfig) -> Result<FanoutManifest> {
    let symbols = validate_config(&config)?;
    let url = ws_url(&config.gateway, config.trades)?;
    let subscribe_json = if symbols.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&WsClientMessage::Subscribe {
            symbols,
        })?)
    };

    let http_client = Client::builder()
        .timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .build()?;

    let mut connect_tasks = Vec::with_capacity(config.subscribers);
    for _ in 0..config.subscribers {
        let url = url.clone();
        let sub = subscribe_json.clone();
        connect_tasks.push(tokio::spawn(async move {
            connect_subscriber(&url, sub.as_deref()).await
        }));
    }

    let mut sockets = Vec::with_capacity(config.subscribers);
    for task in connect_tasks {
        let socket = task
            .await
            .map_err(|error| BenchError::Config(format!("connect task failed: {error}")))??;
        sockets.push(socket);
    }

    let before_metrics = scrape_metrics(&http_client, &config.gateway).await;
    let start = Instant::now();
    let measure_start = start + Duration::from_secs(config.warmup_secs);
    let measure_end = measure_start + Duration::from_secs(config.duration_secs);

    // The ingress driver covers warmup plus the measured window so the
    // subscribers observe the engine under load for the whole run.
    let ingress_symbol = config
        .symbols
        .first()
        .cloned()
        .unwrap_or_else(|| "BTC-USD".into());
    let ingress_client = http_client.clone();
    let ingress_gateway = config.gateway.clone();
    let ingress_rate = config.ingress_rate;
    let ingress_secs = config.warmup_secs + config.duration_secs;
    let ingress_task = tokio::spawn(async move {
        run_ingress_driver(
            &ingress_client,
            &ingress_gateway,
            ingress_rate,
            ingress_secs,
            &ingress_symbol,
        )
        .await
    });

    let mut tasks = Vec::with_capacity(sockets.len());
    for (client_id, socket) in sockets.into_iter().enumerate() {
        tasks.push(tokio::spawn(run_subscriber(
            client_id,
            socket,
            measure_start,
            measure_end,
        )));
    }

    let mut runs = Vec::with_capacity(tasks.len());
    for task in tasks {
        let run = task
            .await
            .map_err(|error| BenchError::Config(format!("subscriber task failed: {error}")))?;
        runs.push(run);
    }
    let ingress = ingress_task
        .await
        .map_err(|error| BenchError::Config(format!("ingress driver task failed: {error}")))?;
    let after_metrics = scrape_metrics(&http_client, &config.gateway).await;

    let measured_secs = config.duration_secs as f64;
    let mut pooled_gaps = new_histogram();
    let mut total_frames = 0u64;
    let mut total_bytes = 0u64;
    let mut stalled_clients = 0usize;
    let mut clients = Vec::with_capacity(runs.len());

    for run in runs {
        total_frames += run.frames;
        total_bytes += run.bytes;
        if run.tracker.stalled() {
            stalled_clients += 1;
        }
        let _ = pooled_gaps.add(run.tracker.histogram());
        let summary = run.tracker.summary();
        clients.push(ClientSummary {
            client_id: run.client_id,
            frames: run.frames,
            bytes: run.bytes,
            frames_per_sec: run.frames as f64 / measured_secs.max(f64::EPSILON),
            bytes_per_sec: run.bytes as f64 / measured_secs.max(f64::EPSILON),
            gap_p50_us: summary.p50,
            gap_p99_us: summary.p99,
            gap_max_us: (run.tracker.max_gap_us() as f64).max(summary.max),
            stalled: run.tracker.stalled(),
            parse_errors: run.parse_errors,
        });
    }

    let sub_count = config.subscribers.max(1) as f64;
    let frames_per_sec = total_frames as f64 / measured_secs.max(f64::EPSILON);
    let frames_per_sec_per_client = frames_per_sec / sub_count;
    let aggregate_bytes_per_sec = total_bytes as f64 / measured_secs.max(f64::EPSILON);
    let bytes_per_sec_per_client = aggregate_bytes_per_sec / sub_count;

    let host = host_info();
    let scope = scope_label(&host);
    let mut labels = vec![scope.clone(), "fanout".into()];
    if stalled_clients > 0 {
        labels.push("stalled-subscriber".into());
    }

    let metrics = build_metrics_scrape(before_metrics, after_metrics);
    let mut notes = vec![
        format!(
            "20 Hz WebSocket fanout measurement over {} subscriber(s), trades={}, symbols={}.",
            config.subscribers,
            config.trades,
            if config.symbols.is_empty() {
                "all".to_string()
            } else {
                config.symbols.join(",")
            }
        ),
        "Frames are decoded and discarded immediately; payloads are never stored in the harness."
            .into(),
        "A client with no frame for >1 s (1,000,000 µs) during the measured window is flagged as stalled."
            .into(),
    ];
    if ingress.sent > 0 {
        notes.push(format!(
            "Ingress driver: target {} orders/s for {} s (warmup + measured), sent {} (ok {}, errors {}), \
             achieved {:.0} orders/s over {:.1} s via POST /api/orders.",
            ingress.target_rate_per_sec,
            ingress_secs,
            ingress.sent,
            ingress.ok,
            ingress.errors,
            ingress.achieved_per_sec,
            ingress.elapsed_secs,
        ));
    } else {
        notes.push(
            "Ingress driver off (--ingress-rate 0): the book carries background traffic only; \
             use this shape as the no-subscriber baseline for the comparison mode."
                .into(),
        );
    }
    if metrics.before.is_some() && metrics.after.is_some() {
        if metrics
            .after
            .as_ref()
            .and_then(|snap| snap.process_cpu_seconds_total)
            .is_none()
        {
            notes
                .push("/metrics did not expose process_cpu_seconds_total; skipped cleanly.".into());
        }
    } else {
        notes.push("/metrics scrape was unavailable during the run; skipped cleanly.".into());
    }
    if stalled_clients > 0 {
        notes.push(format!(
            "WARNING: {stalled_clients} of {} subscriber(s) stalled (>1 s without a frame).",
            config.subscribers
        ));
    }
    if let Some(baseline) = config.baseline.clone() {
        notes.push(baseline_note(
            &baseline,
            metrics.orders_ingressed_delta,
            metrics.md_sequence_delta,
            config.subscribers,
        ));
    }

    let run_id = uuid::Uuid::new_v4().to_string();
    let manifest = FanoutManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        kind: "fanout".into(),
        run_id: run_id.clone(),
        created_at: Utc::now().to_rfc3339(),
        git_sha: git_sha(),
        git_dirty: git_dirty(),
        image_digest: image_digest(),
        scope,
        label: config.label.clone(),
        host,
        gateway: config.gateway.clone(),
        subscribers: config.subscribers,
        trades: config.trades,
        symbols: config.symbols.clone(),
        warmup_secs: config.warmup_secs,
        duration_secs: config.duration_secs,
        measured_secs,
        frames_per_sec,
        frames_per_sec_per_client,
        bytes_per_sec_per_client,
        aggregate_bytes_per_sec,
        total_frames,
        total_bytes,
        stalled_clients,
        gap_us: summarize(&pooled_gaps),
        clients,
        metrics,
        ingress,
        baseline: config.baseline.clone(),
        notes,
        labels,
    };

    std::fs::create_dir_all(&config.output_dir)?;
    let path = config.output_dir.join(format!("{run_id}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&manifest)?)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::api::{BookView, MetricsView, StateView};
    use whippet_core::config::Mode;
    use whippet_core::ids::Seq;

    fn sample_state_view() -> StateView {
        let book = BookView {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(7),
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid_cents: Some(6_500_000),
            best_ask_cents: Some(6_500_100),
            spread_cents: Some(100),
            mid_cents: Some(6_500_050.0),
            last_trade_price_cents: None,
            last_trade_qty: 0,
            last_trade_side: None,
            depth_bid_qty: 10,
            depth_ask_qty: 12,
        };
        StateView {
            mode: Mode::Durable,
            venue: "whippet".into(),
            symbols: vec![book],
            trades: Vec::new(),
            metrics: MetricsView::empty(Mode::Durable, 16),
            freshness_ms: 4,
            sequence: 42,
            iggy_addr: "iggy:8090".into(),
            journal: "iggy".into(),
            guarantee: "durable".into(),
            dashboard_build: None,
        }
    }

    #[test]
    fn gap_percentile_helper_computes_percentiles_and_detects_stalls() {
        // 100 gaps around 50 ms (50,000 µs), none exceeding 1 s.
        let mut gaps: Vec<u64> = (0..99).map(|i| 49_000 + (i % 3) * 1_000).collect();
        gaps.push(65_000);
        let (summary, stalled) = summarize_gaps_us(&gaps);
        assert_eq!(summary.count, 100);
        assert!(!stalled);
        assert!((49_000.0..=51_000.0).contains(&summary.p50));
        assert!((51_000.0..=66_000.0).contains(&summary.p99));
        assert!(summary.p50 <= summary.p99);

        // A single gap > 1_000_000 µs (1 s) flags the subscriber as stalled.
        gaps.push(1_250_000);
        let (stalled_summary, stalled_flag) = summarize_gaps_us(&gaps);
        assert!(stalled_flag);
        assert!(stalled_summary.max >= 1_240_000.0);
    }

    #[test]
    fn client_parses_state_frame_and_notice_frame() {
        let state_msg = WsServerMessage::State {
            state: sample_state_view(),
        };
        let state_json = serde_json::to_string(&state_msg).unwrap();
        let parsed_state = parse_server_frame(&state_json).unwrap();
        assert_eq!(
            parsed_state,
            ParsedFrame::State {
                sequence: 42,
                symbols: 1,
                trades: 0,
            }
        );

        let notice_msg = WsServerMessage::Notice {
            level: "pong".into(),
            message: "1700000000000000000".into(),
        };
        let notice_json = serde_json::to_string(&notice_msg).unwrap();
        let parsed_notice = parse_server_frame(&notice_json).unwrap();
        assert_eq!(
            parsed_notice,
            ParsedFrame::Notice {
                level: "pong".into(),
                message: "1700000000000000000".into(),
            }
        );
    }

    #[test]
    fn fanout_manifest_json_shape_contains_required_fields() {
        let (gap_us, _) = summarize_gaps_us(&[50_000, 50_100, 51_000]);
        let host = HostInfo {
            hostname: "m4".into(),
            os: "Darwin".into(),
            kernel: "24.0.0".into(),
            cpu_model: "Apple M4".into(),
            physical_cores: 10,
            logical_cores: 10,
            memory_bytes: 16 * 1024 * 1024 * 1024,
            virtualization: "docker-desktop".into(),
            docker_version: Some("Docker version 29.0".into()),
            governor: None,
            smt_enabled: None,
            isolated_cores: None,
        };
        let scope = scope_label(&host);
        let manifest = FanoutManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            kind: "fanout".into(),
            run_id: "fanout-test-run".into(),
            created_at: "2026-11-15T00:00:00Z".into(),
            git_sha: "deadbeef".into(),
            git_dirty: false,
            image_digest: None,
            scope: scope.clone(),
            label: "dev-loop-fanout".into(),
            host,
            gateway: "http://127.0.0.1:8080".into(),
            subscribers: 10,
            trades: 0,
            symbols: vec!["BTC-USD".into()],
            warmup_secs: 5,
            duration_secs: 30,
            measured_secs: 30.0,
            frames_per_sec: 200.0,
            frames_per_sec_per_client: 20.0,
            bytes_per_sec_per_client: 40_000.0,
            aggregate_bytes_per_sec: 400_000.0,
            total_frames: 6_000,
            total_bytes: 12_000_000,
            stalled_clients: 0,
            gap_us,
            clients: vec![ClientSummary {
                client_id: 0,
                frames: 600,
                bytes: 1_200_000,
                frames_per_sec: 20.0,
                bytes_per_sec: 40_000.0,
                gap_p50_us: 50_000.0,
                gap_p99_us: 51_000.0,
                gap_max_us: 51_000.0,
                stalled: false,
                parse_errors: 0,
            }],
            metrics: build_metrics_scrape(
                Some(MetricsSnapshot {
                    orders_ingressed_total: Some(1_000),
                    md_sequence: Some(500),
                    process_cpu_seconds_total: None,
                }),
                Some(MetricsSnapshot {
                    orders_ingressed_total: Some(1_600),
                    md_sequence: Some(1_100),
                    process_cpu_seconds_total: None,
                }),
            ),
            ingress: IngressSummary {
                target_rate_per_sec: 10_000,
                sent: 300_000,
                ok: 299_900,
                errors: 100,
                elapsed_secs: 30.0,
                achieved_per_sec: 10_000.0,
            },
            baseline: Some(BaselineSnapshot {
                source: "baseline.json".into(),
                orders_ingressed_delta: Some(300_000),
                md_sequence_delta: Some(600),
            }),
            notes: vec!["dev-loop fanout test".into()],
            labels: vec![scope, "fanout".into()],
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["kind"], "fanout");
        assert_eq!(value["run_id"], "fanout-test-run");
        assert_eq!(value["git_sha"], "deadbeef");
        assert_eq!(value["git_dirty"], false);
        assert_eq!(value["scope"], "dev-loop");
        assert_eq!(value["label"], "dev-loop-fanout");
        assert_eq!(value["gateway"], "http://127.0.0.1:8080");
        assert_eq!(value["subscribers"], 10);
        assert_eq!(value["trades"], 0);
        assert_eq!(value["symbols"][0], "BTC-USD");
        assert_eq!(value["duration_secs"], 30);
        assert_eq!(value["frames_per_sec"], 200.0);
        assert_eq!(value["frames_per_sec_per_client"], 20.0);
        assert_eq!(value["bytes_per_sec_per_client"], 40_000.0);
        assert_eq!(value["aggregate_bytes_per_sec"], 400_000.0);
        assert!(value["gap_us"]["p50"].as_f64().unwrap() > 0.0);
        assert!(value["gap_us"]["p99"].as_f64().unwrap() > 0.0);
        assert_eq!(value["metrics"]["orders_ingressed_delta"], 600);
        assert_eq!(value["metrics"]["md_sequence_delta"], 600);
        assert!(value["metrics"]["process_cpu_seconds_delta"].is_null());
        assert_eq!(value["ingress"]["target_rate_per_sec"], 10_000);
        assert_eq!(value["ingress"]["sent"], 300_000);
        assert_eq!(value["ingress"]["errors"], 100);
        assert_eq!(value["baseline"]["orders_ingressed_delta"], 300_000);

        let decoded: FanoutManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.run_id, "fanout-test-run");
        assert_eq!(decoded.subscribers, 10);
    }

    #[test]
    fn ingress_pacing_matches_the_offered_rate() {
        let off = plan_ingress(0, 35);
        assert_eq!(
            off,
            IngressPlan {
                total_orders: 0,
                period_ns: 0,
            }
        );

        let plan = plan_ingress(10_000, 35);
        assert_eq!(plan.total_orders, 350_000);
        assert_eq!(plan.period_ns, 100_000);

        let single = plan_ingress(1_000, 30);
        assert_eq!(single.total_orders, 30_000);
        assert_eq!(single.period_ns, 1_000_000);
    }

    #[test]
    fn ingress_order_bodies_form_a_unique_ladder() {
        let first = ingress_order_body(0, "BTC-USD");
        let second = ingress_order_body(1, "BTC-USD");
        assert_eq!(first["symbol"], "BTC-USD");
        assert_eq!(first["side"], "buy");
        assert_eq!(second["side"], "sell");
        assert_ne!(
            first["client_order_id"], second["client_order_id"],
            "client ids must be unique per order"
        );
        assert_ne!(
            first["participant"], second["participant"],
            "participants must be unique per order"
        );
        // The ladder walks ±20 ticks around the reference price.
        let price = first["price_cents"].as_i64().unwrap();
        assert!((6_498_000..=6_502_000).contains(&price));
    }

    #[test]
    fn baseline_note_compares_deltas_with_and_without_subscribers() {
        let baseline = BaselineSnapshot {
            source: "baseline.json".into(),
            orders_ingressed_delta: Some(300_000),
            md_sequence_delta: Some(600),
        };
        let note = baseline_note(&baseline, Some(294_000), Some(590), 10);
        assert!(note.contains("baseline.json"));
        assert!(note.contains("300000"));
        assert!(note.contains("294000"));
        assert!(note.contains("-2.0%"));

        let missing = baseline_note(&baseline, None, None, 10);
        assert!(missing.contains("unavailable"));
    }

    #[test]
    fn prometheus_parser_extracts_orders_sequence_and_optional_cpu() {
        let without_cpu = "\
# HELP whippet_orders_ingressed_total Orders accepted.
# TYPE whippet_orders_ingressed_total counter
whippet_orders_ingressed_total 12345
# HELP whippet_md_sequence Newest MD sequence.
# TYPE whippet_md_sequence gauge
whippet_md_sequence 6789
";
        let snap = parse_prometheus_metrics(without_cpu);
        assert_eq!(snap.orders_ingressed_total, Some(12_345));
        assert_eq!(snap.md_sequence, Some(6_789));
        assert_eq!(snap.process_cpu_seconds_total, None);

        let with_cpu = format!("{without_cpu}process_cpu_seconds_total 14.25\n");
        let snap_cpu = parse_prometheus_metrics(&with_cpu);
        assert_eq!(snap_cpu.process_cpu_seconds_total, Some(14.25));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn websocket_client_subscribes_and_parses_state_and_notice_frames() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state_json = serde_json::to_string(&WsServerMessage::State {
            state: sample_state_view(),
        })
        .unwrap();
        let notice_json = serde_json::to_string(&WsServerMessage::Notice {
            level: "info".into(),
            message: "subscribed".into(),
        })
        .unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let incoming = ws.next().await.unwrap().unwrap();
            match incoming {
                Message::Text(text) => {
                    let msg: WsClientMessage = serde_json::from_str(text.as_ref()).unwrap();
                    match msg {
                        WsClientMessage::Subscribe { symbols } => {
                            assert_eq!(symbols, vec![Symbol::new("BTC-USD").unwrap()]);
                        }
                        other => panic!("unexpected client message: {other:?}"),
                    }
                }
                other => panic!("unexpected ws frame: {other:?}"),
            }
            ws.send(Message::Text(state_json.into())).await.unwrap();
            ws.send(Message::Text(notice_json.into())).await.unwrap();
            let _ = ws.close(None).await;
        });

        let url = ws_url(&format!("http://{addr}"), 5).unwrap();
        assert!(url.ends_with("/ws?trades=5"));
        let sub_json = serde_json::to_string(&WsClientMessage::Subscribe {
            symbols: vec![Symbol::new("BTC-USD").unwrap()],
        })
        .unwrap();
        let mut client = connect_subscriber(&url, Some(&sub_json)).await.unwrap();

        let first = match client.next().await.unwrap().unwrap() {
            Message::Text(text) => parse_server_frame(text.as_ref()).unwrap(),
            other => panic!("unexpected first frame: {other:?}"),
        };
        assert_eq!(
            first,
            ParsedFrame::State {
                sequence: 42,
                symbols: 1,
                trades: 0,
            }
        );

        let second = match client.next().await.unwrap().unwrap() {
            Message::Text(text) => parse_server_frame(text.as_ref()).unwrap(),
            other => panic!("unexpected second frame: {other:?}"),
        };
        assert_eq!(
            second,
            ParsedFrame::Notice {
                level: "info".into(),
                message: "subscribed".into(),
            }
        );

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_fanout_measures_subscribers_and_writes_manifest() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state_json = serde_json::to_string(&WsServerMessage::State {
            state: sample_state_view(),
        })
        .unwrap();

        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let frame = state_json.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let mut interval = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        interval.tick().await;
                        if ws.send(Message::Text(frame.clone().into())).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let output_dir =
            std::env::temp_dir().join(format!("whippet-fanout-{}", uuid::Uuid::new_v4()));
        let config = FanoutConfig {
            gateway: format!("http://{addr}"),
            subscribers: 2,
            duration_secs: 1,
            warmup_secs: 0,
            trades: 0,
            symbols: Vec::new(),
            output_dir: output_dir.clone(),
            label: "dev-loop-fanout".into(),
            ingress_rate: 0,
            baseline: None,
        };
        let manifest = run_fanout(config).await.unwrap();
        server.abort();

        assert_eq!(manifest.kind, "fanout");
        assert_eq!(manifest.subscribers, 2);
        assert_eq!(manifest.clients.len(), 2);
        assert!(manifest.total_frames >= 20);
        assert!(manifest.frames_per_sec > 10.0);
        assert!(manifest.aggregate_bytes_per_sec > manifest.bytes_per_sec_per_client);
        assert_eq!(manifest.stalled_clients, 0);
        assert!(manifest.gap_us.p50 > 0.0);
        assert!(manifest.gap_us.p99 >= manifest.gap_us.p50);

        let path = output_dir.join(format!("{}.json", manifest.run_id));
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&output_dir);
    }
}
