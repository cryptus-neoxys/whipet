// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Open-loop benchmark harness.
//!
//! The pacer schedules every request at an absolute deadline. Latency is
//! measured from that deadline, not from the moment the request happened to
//! leave, which is the coordinated-omission correction. If the generator
//! cannot keep up, `scheduler_slip` grows and the run says so.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use hdrhistogram::Histogram;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};
use whippet_core::api::OrderRequest;
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::Symbol;
use whippet_core::manifest::{
    BenchManifest, HostInfo, LatencySummary, MANIFEST_SCHEMA_VERSION, RepetitionResult,
    ThroughputSummary, WorkloadSpec,
};
use whippet_core::protocol::{OrderType, Side};

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
        }
    }
}

struct Sample {
    latency_us: u64,
    slip_us: u64,
    ok: bool,
}

struct RepStats {
    throughput: ThroughputSummary,
    latency: LatencySummary,
    measured_secs: f64,
}

pub async fn run(config: BenchConfig) -> Result<BenchManifest> {
    if config.rate_per_sec == 0 {
        return Err(BenchError::Config("rate_per_sec must be positive".into()));
    }
    let exchange = ExchangeConfig::default();
    let client = Client::builder()
        .pool_max_idle_per_host(512)
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let mut repetitions = Vec::new();

    for index in 0..config.repetitions.max(1) {
        tracing::info!(
            repetition = index,
            rate = config.rate_per_sec,
            duration = config.duration_secs,
            "starting benchmark repetition"
        );
        let started_at = Utc::now().to_rfc3339();
        let stats = run_repetition(&client, &config, &exchange, &run_id, index).await?;
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
        batch_size: 1,
        encryption: false,
    };

    let host = host_info();
    let labels = vec![scope_label(&host)];
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
        notes: vec![
            "Open-loop absolute-deadline pacing; latency measured from the scheduled deadline."
                .into(),
            "M4/Docker Desktop is a dev-loop measurement, not a headline claim.".into(),
            "HTTP ingress is a convenience path; the Linux headline uses TCP on a native host."
                .into(),
        ],
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

async fn run_repetition(
    client: &Client,
    config: &BenchConfig,
    exchange: &ExchangeConfig,
    run_id: &str,
    repetition: u32,
) -> Result<RepStats> {
    let total_secs = config.warmup_secs + config.duration_secs;
    let total_ticks = config.rate_per_sec.saturating_mul(total_secs);
    let warmup_ticks = config.rate_per_sec.saturating_mul(config.warmup_secs);
    let period_ns = 1_000_000_000u64 / config.rate_per_sec;
    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let (tx, rx) = mpsc::channel::<Sample>(1_000_000);
    let collector = tokio::spawn(collect(rx));

    let symbols: Vec<Symbol> = config
        .symbols
        .iter()
        .map(|symbol| Symbol::new(symbol))
        .collect::<std::result::Result<_, _>>()
        .map_err(|error| BenchError::Config(error.to_string()))?;
    if symbols.is_empty() {
        return Err(BenchError::Config("no symbols configured".into()));
    }
    let mut rng = ChaCha20Rng::seed_from_u64(config.seed ^ u64::from(repetition));
    let url = format!("{}/api/orders", config.base_url.trim_end_matches('/'));
    let start = Instant::now();
    let mut scheduled = 0u64;
    let mut spawn_errors = 0u64;

    for tick in 0..total_ticks {
        let deadline = start + Duration::from_nanos(period_ns.saturating_mul(tick));
        sleep_until(deadline).await;
        let now = Instant::now();
        let slip_us = now.saturating_duration_since(deadline).as_micros() as u64;
        let measured = tick >= warmup_ticks;
        if measured {
            scheduled += 1;
        }

        let body = make_order(&symbols, exchange, &mut rng, run_id, repetition, tick);
        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                spawn_errors += 1;
                if measured {
                    let _ = tx
                        .send(Sample {
                            latency_us: slip_us,
                            slip_us,
                            ok: false,
                        })
                        .await;
                }
                continue;
            }
        };

        let client = client.clone();
        let url = url.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = client.post(&url).json(&body).send().await;
            let completed = Instant::now();
            let ok = result
                .as_ref()
                .map(|response| response.status().is_success())
                .unwrap_or(false);
            if measured {
                let latency_us = completed.saturating_duration_since(deadline).as_micros() as u64;
                let _ = tx
                    .send(Sample {
                        latency_us,
                        slip_us,
                        ok,
                    })
                    .await;
            }
            drop(permit);
        });
    }

    // Drain in-flight requests before closing the collector.
    let _ = semaphore.acquire_many(config.max_in_flight as u32).await;
    drop(tx);
    let (latency, slip, completed, errors) = collector
        .await
        .map_err(|error| BenchError::Config(error.to_string()))?;
    let measured_secs = if config.duration_secs == 0 {
        start.elapsed().as_secs_f64()
    } else {
        config.duration_secs as f64
    };
    let throughput = ThroughputSummary {
        target_per_sec: config.rate_per_sec,
        achieved_per_sec: completed as f64 / measured_secs.max(f64::EPSILON),
        goodput_per_sec: completed.saturating_sub(errors) as f64 / measured_secs.max(f64::EPSILON),
        error_count: errors + spawn_errors,
        scheduled_samples: scheduled,
        completed_samples: completed,
        scheduler_slip: slip.clone(),
    };
    Ok(RepStats {
        throughput,
        latency,
        measured_secs,
    })
}

async fn collect(mut rx: mpsc::Receiver<Sample>) -> (LatencySummary, LatencySummary, u64, u64) {
    let mut latency = Histogram::<u64>::new_with_max(60_000_000, 3).expect("valid bounds");
    let mut slip = Histogram::<u64>::new_with_max(60_000_000, 3).expect("valid bounds");
    let mut completed = 0u64;
    let mut errors = 0u64;
    while let Some(sample) = rx.recv().await {
        completed += 1;
        if !sample.ok {
            errors += 1;
        }
        let _ = latency.record(sample.latency_us.min(59_999_999));
        let _ = slip.record(sample.slip_us.min(59_999_999));
    }
    (summarize(&latency), summarize(&slip), completed, errors)
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

async fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline <= now {
        return;
    }
    let remaining = deadline - now;
    if remaining > Duration::from_micros(400) {
        tokio::time::sleep(remaining - Duration::from_micros(300)).await;
    }
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

fn make_order(
    symbols: &[Symbol],
    exchange: &ExchangeConfig,
    rng: &mut ChaCha20Rng,
    run_id: &str,
    repetition: u32,
    tick: u64,
) -> OrderRequest {
    use rand::Rng;
    let symbol = symbols[(tick as usize) % symbols.len()];
    let instrument = exchange.instrument(symbol).expect("configured symbol");
    let mid = instrument.reference_price().0;
    let offset = rng.gen_range(0..=10i64);
    let buy = tick.is_multiple_of(2);
    let price_ticks = if buy { mid - offset } else { mid + offset };
    OrderRequest {
        symbol,
        side: if buy { Side::Buy } else { Side::Sell },
        order_type: OrderType::Limit,
        price_cents: Some(price_ticks * instrument.tick_size_cents),
        qty: rng.gen_range(1..=50),
        client_order_id: Some(format!("bench-{run_id}-{repetition}-{tick}")),
        participant: Some(format!("bench-{tick}")),
    }
}

fn scope_label(host: &HostInfo) -> String {
    if host.virtualization == "native" && host.os.eq_ignore_ascii_case("linux") {
        "headline".into()
    } else {
        "dev-loop".into()
    }
}

fn host_info() -> HostInfo {
    let os = command("uname", &["-s"]).unwrap_or_else(|| std::env::consts::OS.into());
    let kernel = command("uname", &["-r"]).unwrap_or_default();
    let (cpu_model, logical_cores, memory_bytes) = platform_cpu();
    let virtualization = std::env::var("WHIPPET_VIRTUALIZATION").unwrap_or_else(|_| {
        if Path::new("/.dockerenv").exists()
            || std::fs::read_to_string("/proc/1/cgroup")
                .map(|contents| contents.contains("docker") || contents.contains("containerd"))
                .unwrap_or(false)
        {
            // A container on a Linux host still reports Linux. Never let that
            // become a headline number.
            "docker-desktop".into()
        } else if cfg!(target_os = "linux") {
            "native".into()
        } else {
            "dev-loop".into()
        }
    });
    HostInfo {
        hostname: command("hostname", &[]).unwrap_or_else(|| "unknown".into()),
        os,
        kernel,
        cpu_model,
        physical_cores: logical_cores,
        logical_cores,
        memory_bytes,
        virtualization,
        docker_version: command("docker", &["--version"]),
        governor: std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .ok()
            .map(|value| value.trim().to_string()),
        smt_enabled: None,
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
}
