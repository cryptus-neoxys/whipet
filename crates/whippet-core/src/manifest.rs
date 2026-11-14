// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Benchmark manifest. Every result published by Whippet carries one.
//!
//! The schema is intentionally explicit about hardware, virtualization,
//! durability, payload, and repetition count. A number without a manifest is
//! not a result; it is an anecdote.

use serde::{Deserialize, Serialize};

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostInfo {
    pub hostname: String,
    pub os: String,
    pub kernel: String,
    pub cpu_model: String,
    pub physical_cores: usize,
    pub logical_cores: usize,
    pub memory_bytes: u64,
    /// `native`, `docker-desktop`, `orbstack`, `linux-vm`, ...
    pub virtualization: String,
    pub docker_version: Option<String>,
    pub governor: Option<String>,
    pub smt_enabled: Option<bool>,
    pub isolated_cores: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub name: String,
    pub mode: String,
    pub transport: String,
    pub payload_bytes: usize,
    pub symbols: Vec<String>,
    pub partitions: u32,
    pub rate_per_sec: u64,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub repetitions: u32,
    pub subscribers: u32,
    pub seed: u64,
    pub durability: String,
    pub consumer_offset_durability: String,
    pub batch_size: u32,
    pub encryption: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    /// All values in microseconds.
    pub count: u64,
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub p9999: f64,
    pub max: f64,
    pub p50_ci95: Option<[f64; 2]>,
    pub p99_ci95: Option<[f64; 2]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageLatency {
    pub stage: String,
    pub latency: LatencySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputSummary {
    pub target_per_sec: u64,
    pub achieved_per_sec: f64,
    pub goodput_per_sec: f64,
    pub error_count: u64,
    pub scheduled_samples: u64,
    pub completed_samples: u64,
    /// Generator lateness: completed minus scheduled deadline, in microseconds.
    pub scheduler_slip: LatencySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub created_at: String,
    pub git_sha: String,
    pub git_dirty: bool,
    pub image_digest: Option<String>,
    pub host: HostInfo,
    pub workload: WorkloadSpec,
    pub throughput: ThroughputSummary,
    pub latency: LatencySummary,
    pub stages: Vec<StageLatency>,
    pub repetitions: Vec<RepetitionResult>,
    pub notes: Vec<String>,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepetitionResult {
    pub index: u32,
    pub started_at: String,
    pub duration_secs: f64,
    pub throughput: ThroughputSummary,
    pub latency: LatencySummary,
    pub stages: Vec<StageLatency>,
}

impl BenchManifest {
    /// Human-readable label for the machine class. Never call Docker Desktop
    /// numbers a headline result.
    pub fn scope_label(&self) -> &'static str {
        match self.host.virtualization.as_str() {
            "native" if self.host.os.to_ascii_lowercase().contains("linux") => "headline",
            "docker-desktop" | "orbstack" | "linux-vm" => "dev-loop",
            _ => "dev-loop",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_desktop_is_dev_loop() {
        let manifest = BenchManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            run_id: "run".into(),
            created_at: "2026-11-14T00:00:00Z".into(),
            git_sha: "deadbeef".into(),
            git_dirty: false,
            image_digest: None,
            host: HostInfo {
                hostname: "m4".into(),
                os: "macOS".into(),
                kernel: "Darwin".into(),
                cpu_model: "Apple M4".into(),
                physical_cores: 10,
                logical_cores: 10,
                memory_bytes: 16 * 1024 * 1024 * 1024,
                virtualization: "docker-desktop".into(),
                docker_version: Some("29.9.2".into()),
                governor: None,
                smt_enabled: None,
                isolated_cores: None,
            },
            workload: WorkloadSpec {
                name: "L1".into(),
                mode: "durable".into(),
                transport: "tcp".into(),
                payload_bytes: 64,
                symbols: vec!["BTC-USD".into()],
                partitions: 16,
                rate_per_sec: 1_000,
                duration_secs: 30,
                warmup_secs: 5,
                repetitions: 1,
                subscribers: 0,
                seed: 7,
                durability: "persisted".into(),
                consumer_offset_durability: "persisted".into(),
                batch_size: 1,
                encryption: false,
            },
            throughput: ThroughputSummary {
                target_per_sec: 1_000,
                achieved_per_sec: 999.0,
                goodput_per_sec: 999.0,
                error_count: 0,
                scheduled_samples: 30_000,
                completed_samples: 30_000,
                scheduler_slip: empty_latency(),
            },
            latency: empty_latency(),
            stages: vec![],
            repetitions: vec![],
            notes: vec![],
            labels: vec!["dev-loop".into()],
        };
        assert_eq!(manifest.scope_label(), "dev-loop");
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
}
