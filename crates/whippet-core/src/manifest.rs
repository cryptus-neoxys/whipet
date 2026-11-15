// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Benchmark manifest. Every result published by Whippet carries one.
//!
//! The schema is intentionally explicit about hardware, virtualization,
//! durability, payload, and repetition count. A number without a manifest is
//! not a result; it is an anecdote.
//!
//! [`BenchManifest::check_invariants`] makes the honesty rules executable:
//!
//! - percentiles are ordered and bounded by `min`/`max`, sample counts agree
//!   with the throughput block, and error/goodput arithmetic re-derives;
//! - a zero-sample summary is legal only when the measured window scheduled
//!   zero events, or when a saturated in-flight cap labelled the run
//!   `invalid-overload` (dropped slots never invent a latency sample);
//! - the headline latency/throughput/stages are the median-p99 repetition,
//!   not a best run and not an average;
//! - stage summaries are ordered, uniquely named, and non-negative except for
//!   the signed `server_offset_ns` clock comparison;
//! - workload rates, durations, repetitions, transport, and batch sizes are
//!   valid, and the open-loop schedule bound matches the sample count;
//! - a dirty tree, an unknown commit, or a missing image digest is written
//!   down in `notes` instead of being silently blank;
//! - the scope label agrees with the host (`headline` only for native Linux)
//!   and validity labels carry the matching note.

use serde::{Deserialize, Serialize};

use crate::api::MAX_BATCH_ORDERS;

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Re-deriving `achieved_per_sec * duration_secs` from a manifest can differ
/// from `completed_samples` by a floating-point rounding step. Anything larger
/// than this relative tolerance is a fabricated rate, not rounding.
const RATE_RELATIVE_TOLERANCE: f64 = 1e-6;

/// A stage whose value is a signed clock offset (nanoseconds) rather than a
/// non-negative duration (microseconds). The harness writes
/// `server_offset_ns`, and it may legitimately be negative.
const SIGNED_STAGE_NAME: &str = "server_offset_ns";

/// Labels whose meaning is a validity verdict. At most one may appear.
const STEADY_STATE_LABELS: [&str; 3] = ["steady-state", "not-steady-state", "steady-state-skipped"];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl HostInfo {
    /// Human-readable label for the machine class. Never call Docker Desktop
    /// or macOS numbers a headline result.
    pub fn scope_label(&self) -> &'static str {
        if self.virtualization == "native" && self.os.to_ascii_lowercase().contains("linux") {
            "headline"
        } else {
            "dev-loop"
        }
    }

    /// The manifest must identify the host it ran on. `memory_bytes` is left
    /// free because a platform that cannot report it records zero rather than
    /// inventing a number.
    pub fn check_invariants(&self) -> Result<(), String> {
        for (name, value) in [
            ("hostname", &self.hostname),
            ("os", &self.os),
            ("kernel", &self.kernel),
            ("cpu_model", &self.cpu_model),
            ("virtualization", &self.virtualization),
        ] {
            if value.trim().is_empty() {
                return Err(format!(
                    "host.{name} is empty; the manifest must name the host"
                ));
            }
        }
        if self.logical_cores == 0 {
            return Err("host.logical_cores is zero".into());
        }
        if self.physical_cores == 0 {
            return Err("host.physical_cores is zero".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl WorkloadSpec {
    /// Upper bound on the events the measured window can schedule: the nominal
    /// `rate x duration`, plus the partial final batch that whole-batch warmup
    /// trimming can leave behind (`0..batch_size` extra orders).
    pub fn measured_event_upper_bound(&self) -> u64 {
        self.rate_per_sec
            .saturating_mul(self.duration_secs)
            .saturating_add(u64::from(self.batch_size.saturating_sub(1)))
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        for (name, value) in [
            ("name", &self.name),
            ("mode", &self.mode),
            ("transport", &self.transport),
            ("durability", &self.durability),
            (
                "consumer_offset_durability",
                &self.consumer_offset_durability,
            ),
        ] {
            if value.trim().is_empty() {
                return Err(format!("workload.{name} is empty"));
            }
        }
        if self.transport != "http" && self.transport != "tcp" {
            return Err(format!(
                "workload.transport must be \"http\" or \"tcp\", got {:?}",
                self.transport
            ));
        }
        if self.payload_bytes == 0 {
            return Err("workload.payload_bytes is zero".into());
        }
        if self.symbols.is_empty() {
            return Err("workload.symbols is empty; a run must name at least one symbol".into());
        }
        for (index, symbol) in self.symbols.iter().enumerate() {
            if symbol.trim().is_empty() {
                return Err(format!("workload.symbols[{index}] is empty"));
            }
            if self.symbols[..index].contains(symbol) {
                return Err(format!("workload.symbols contains {symbol} more than once"));
            }
        }
        if self.partitions == 0 {
            return Err("workload.partitions is zero".into());
        }
        if self.rate_per_sec == 0 {
            return Err("workload.rate_per_sec is zero; an open-loop schedule needs a rate".into());
        }
        if self.repetitions == 0 {
            return Err("workload.repetitions is zero; a run needs at least one repetition".into());
        }
        if self.batch_size == 0 || self.batch_size as usize > MAX_BATCH_ORDERS {
            return Err(format!(
                "workload.batch_size must be 1..={MAX_BATCH_ORDERS}, got {}",
                self.batch_size
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencySummary {
    /// All values in microseconds, except `server_offset_ns` stages (ns).
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

impl LatencySummary {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Check ordering, finiteness, and the sign rule. `label` names the
    /// summary in the error; `allow_negative` is true only for signed stages
    /// such as `server_offset_ns`.
    ///
    /// A zero-sample summary must be all zeros rather than a leftover number.
    pub fn check_ordered(&self, label: &str, allow_negative: bool) -> Result<(), String> {
        let points = [
            ("min", self.min),
            ("p50", self.p50),
            ("p90", self.p90),
            ("p99", self.p99),
            ("p999", self.p999),
            ("p9999", self.p9999),
            ("max", self.max),
        ];
        for (name, value) in points {
            if !value.is_finite() {
                return Err(format!("{label}: {name} is not a finite number ({value})"));
            }
            if !allow_negative && value < 0.0 {
                return Err(format!(
                    "{label}: {name} is negative ({value}); a duration cannot be negative"
                ));
            }
        }
        if !self.mean.is_finite() {
            return Err(format!(
                "{label}: mean is not a finite number ({})",
                self.mean
            ));
        }
        if !allow_negative && self.mean < 0.0 {
            return Err(format!(
                "{label}: mean is negative ({}); a duration cannot be negative",
                self.mean
            ));
        }
        for (name, interval) in [("p50_ci95", self.p50_ci95), ("p99_ci95", self.p99_ci95)] {
            if let Some([low, high]) = interval {
                if !low.is_finite() || !high.is_finite() {
                    return Err(format!("{label}: {name} is not finite ([{low}, {high}])"));
                }
                if low > high {
                    return Err(format!("{label}: {name} is inverted ([{low}, {high}])"));
                }
                if !allow_negative && low < 0.0 {
                    return Err(format!(
                        "{label}: {name} is negative ([{low}, {high}]); a duration cannot be negative"
                    ));
                }
            }
        }
        if self.count == 0 {
            let any_nonzero = points.iter().any(|(_, value)| *value != 0.0) || self.mean != 0.0;
            if any_nonzero {
                return Err(format!(
                    "{label}: a zero-sample summary must be all zeros, not a stale value"
                ));
            }
            return Ok(());
        }
        for pair in points.windows(2) {
            let (lower_name, lower) = pair[0];
            let (upper_name, upper) = pair[1];
            if lower > upper {
                return Err(format!(
                    "{label}: percentiles are not ordered: {lower_name}={lower} > {upper_name}={upper}"
                ));
            }
        }
        if self.mean < self.min || self.mean > self.max {
            return Err(format!(
                "{label}: mean={} is outside the observed range [min={}, max={}]",
                self.mean, self.min, self.max
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageLatency {
    pub stage: String,
    pub latency: LatencySummary,
}

impl StageLatency {
    fn check_invariants(&self, label: &str) -> Result<(), String> {
        if self.stage.trim().is_empty() {
            return Err(format!("{label}: stage name is empty"));
        }
        if self.latency.count == 0 {
            return Err(format!(
                "{label}: stage {:?} has a zero-sample summary; emit the stage or omit it",
                self.stage
            ));
        }
        let allow_negative = self.stage == SIGNED_STAGE_NAME;
        self.latency
            .check_ordered(&format!("{label} stage {:?}", self.stage), allow_negative)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl ThroughputSummary {
    /// Check the throughput block against its own sample counts and the
    /// measured duration. `achieved_per_sec` must be derived from completed
    /// samples, never copied from the target or invented for an invalid run.
    pub fn check_invariants(
        &self,
        label: &str,
        duration_secs: f64,
        workload: &WorkloadSpec,
    ) -> Result<(), String> {
        if self.target_per_sec != workload.rate_per_sec {
            return Err(format!(
                "{label}: target_per_sec={} disagrees with workload.rate_per_sec={}",
                self.target_per_sec, workload.rate_per_sec
            ));
        }
        if !duration_secs.is_finite() || duration_secs < 0.0 {
            return Err(format!(
                "{label}: duration_secs must be finite and non-negative, got {duration_secs}"
            ));
        }
        for (name, value) in [
            ("achieved_per_sec", self.achieved_per_sec),
            ("goodput_per_sec", self.goodput_per_sec),
        ] {
            if !value.is_finite() {
                return Err(format!("{label}: {name} is not a finite number ({value})"));
            }
            if value < 0.0 {
                return Err(format!("{label}: {name} is negative ({value})"));
            }
        }
        if self.error_count > self.completed_samples {
            return Err(format!(
                "{label}: error_count={} exceeds completed_samples={}",
                self.error_count, self.completed_samples
            ));
        }
        if self.completed_samples > self.scheduled_samples {
            return Err(format!(
                "{label}: completed_samples={} exceeds scheduled_samples={}; a dropped slot \
                 cannot complete more work than was scheduled",
                self.completed_samples, self.scheduled_samples
            ));
        }
        if self.goodput_per_sec > self.achieved_per_sec {
            return Err(format!(
                "{label}: goodput_per_sec={} exceeds achieved_per_sec={}",
                self.goodput_per_sec, self.achieved_per_sec
            ));
        }
        self.scheduler_slip
            .check_ordered(&format!("{label} scheduler_slip"), false)?;
        if self.scheduler_slip.count != self.completed_samples {
            return Err(format!(
                "{label}: scheduler_slip.count={} disagrees with completed_samples={}; every \
                 completed sample carries exactly one slip sample",
                self.scheduler_slip.count, self.completed_samples
            ));
        }
        if duration_secs > 0.0 {
            let derived_achieved = self.completed_samples as f64 / duration_secs;
            let derived_goodput =
                (self.completed_samples - self.error_count) as f64 / duration_secs;
            check_rate(
                label,
                "achieved_per_sec",
                self.achieved_per_sec,
                derived_achieved,
            )?;
            check_rate(
                label,
                "goodput_per_sec",
                self.goodput_per_sec,
                derived_goodput,
            )?;
        } else if self.completed_samples != 0
            || self.achieved_per_sec != 0.0
            || self.goodput_per_sec != 0.0
        {
            return Err(format!(
                "{label}: a zero measured duration can only carry zero samples and zero \
                 throughput, got completed={} achieved={} goodput={}",
                self.completed_samples, self.achieved_per_sec, self.goodput_per_sec
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepetitionResult {
    pub index: u32,
    pub started_at: String,
    pub duration_secs: f64,
    pub throughput: ThroughputSummary,
    pub latency: LatencySummary,
    pub stages: Vec<StageLatency>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl BenchManifest {
    /// Human-readable label for the machine class. Never call Docker Desktop
    /// numbers a headline result.
    pub fn scope_label(&self) -> &'static str {
        self.host.scope_label()
    }

    /// Enforce the manifest's honesty rules. The harness calls this before it
    /// writes a manifest, and the tests call it on hand-built and round-tripped
    /// manifests.
    pub fn check_invariants(&self) -> Result<(), String> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(format!(
                "schema_version={} is not the current version {MANIFEST_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if self.run_id.trim().is_empty() {
            return Err("run_id is empty; it is also the manifest file name".into());
        }
        if self.run_id.len() > 128 || self.run_id.chars().any(char::is_whitespace) {
            return Err(format!(
                "run_id {:?} must be a single token of at most 128 characters",
                self.run_id
            ));
        }
        if self.created_at.trim().is_empty() || !self.created_at.contains('T') {
            return Err(format!(
                "created_at {:?} is not an RFC 3339 timestamp",
                self.created_at
            ));
        }
        self.host.check_invariants()?;
        self.workload.check_invariants()?;
        self.check_provenance()?;
        self.check_labels()?;
        self.check_notes()?;

        if self.repetitions.len() != self.workload.repetitions as usize {
            return Err(format!(
                "workload.repetitions={} but repetitions.len()={}",
                self.workload.repetitions,
                self.repetitions.len()
            ));
        }
        if self.repetitions.is_empty() {
            return Err("repetitions is empty; a manifest needs at least one repetition".into());
        }
        let event_bound = self.workload.measured_event_upper_bound();
        for (position, repetition) in self.repetitions.iter().enumerate() {
            if repetition.index as usize != position {
                return Err(format!(
                    "repetitions[{position}].index={} is not the position; repetitions must be \
                     kept in order",
                    repetition.index
                ));
            }
            repetition.check_invariants(&self.workload, event_bound, &self.labels)?;
        }

        check_stages("stages", &self.stages)?;
        self.latency.check_ordered("latency", false)?;
        if self.latency.count != self.throughput.completed_samples {
            return Err(format!(
                "latency.count={} disagrees with throughput.completed_samples={}; every \
                 completed sample is one latency sample",
                self.latency.count, self.throughput.completed_samples
            ));
        }

        // The headline is the repetition with the median p99, not the best run.
        let mut ordered: Vec<&RepetitionResult> = self.repetitions.iter().collect();
        ordered.sort_by(|left, right| left.latency.p99.total_cmp(&right.latency.p99));
        let median = ordered[ordered.len() / 2];
        if self.latency != median.latency {
            return Err(format!(
                "headline latency is not the median-p99 repetition (repetition {})",
                median.index
            ));
        }
        if self.throughput != median.throughput {
            return Err(format!(
                "headline throughput is not the median-p99 repetition (repetition {})",
                median.index
            ));
        }
        if self.stages != median.stages {
            return Err(format!(
                "headline stages are not the median-p99 repetition (repetition {})",
                median.index
            ));
        }
        self.throughput
            .check_invariants("throughput", median.duration_secs, &self.workload)
    }

    /// A dirty tree, an unknown commit, or an absent image digest must be
    /// reported in `notes`; a blank field is not a report.
    fn check_provenance(&self) -> Result<(), String> {
        if self.git_sha.trim().is_empty() {
            return Err("git_sha is empty; use \"unknown\" and say so in notes".into());
        }
        if self.git_sha == "unknown" && !notes_mention(&self.notes, "commit") {
            return Err(
                "git_sha is \"unknown\" but no note records that the run is not tied to a \
                        commit"
                    .into(),
            );
        }
        if self.git_dirty && !notes_mention(&self.notes, "dirty") {
            return Err("git_dirty is true but no note records the dirty working tree".into());
        }
        match &self.image_digest {
            Some(digest) if digest.trim().is_empty() => {
                Err("image_digest is present but empty; use null and say so in notes".into())
            }
            Some(_) => Ok(()),
            None if !notes_mention(&self.notes, "digest") => Err(
                "image_digest is null but no note records that no image digest was \
                     captured"
                    .into(),
            ),
            None => Ok(()),
        }
    }

    fn check_labels(&self) -> Result<(), String> {
        let scope = self.scope_label();
        let Some(first) = self.labels.first() else {
            return Err(format!(
                "labels is empty; the first label must be the scope {scope:?}"
            ));
        };
        if first.as_str() != scope {
            return Err(format!(
                "labels[0]={first:?} is not the host scope {scope:?}"
            ));
        }
        for (index, label) in self.labels.iter().enumerate() {
            if label.trim().is_empty() {
                return Err(format!("labels[{index}] is empty"));
            }
            if self.labels[..index].contains(label) {
                return Err(format!("labels contains {label:?} more than once"));
            }
        }
        let steady_labels: Vec<&str> = STEADY_STATE_LABELS
            .iter()
            .copied()
            .filter(|label| self.labels.iter().any(|present| present.as_str() == *label))
            .collect();
        if steady_labels.len() > 1 {
            return Err(format!(
                "labels contain conflicting steady-state verdicts: {steady_labels:?}"
            ));
        }
        let labelled = |label: &str| self.labels.iter().any(|present| present.as_str() == label);
        if labelled("invalid-overload") && !notes_mention(&self.notes, "overload") {
            return Err(
                "labels contain invalid-overload but no note explains the dropped slots".into(),
            );
        }
        if labelled("generator-bound") && !notes_mention(&self.notes, "generator") {
            return Err("labels contain generator-bound but no note explains the slip".into());
        }
        if labelled("not-steady-state") && !notes_mention(&self.notes, "steady") {
            return Err("labels contain not-steady-state but no note explains the drift".into());
        }
        if labelled("steady-state-skipped") && !notes_mention(&self.notes, "skipped") {
            return Err(
                "labels contain steady-state-skipped but no note says the check was \
                        skipped"
                    .into(),
            );
        }
        Ok(())
    }

    /// Notes are part of the result: the measurement method, the transport,
    /// and a non-default batch size must be on the record.
    fn check_notes(&self) -> Result<(), String> {
        if !notes_mention(&self.notes, "deadline") {
            return Err(
                "notes do not record that latency is measured from the scheduled deadline".into(),
            );
        }
        let transport = self.workload.transport.to_ascii_lowercase();
        if !notes_mention(&self.notes, &transport) {
            return Err(format!(
                "notes do not record the {:?} transport",
                self.workload.transport
            ));
        }
        if self.workload.batch_size > 1 && !notes_mention(&self.notes, "batch") {
            return Err(format!(
                "notes do not record batch_size={}",
                self.workload.batch_size
            ));
        }
        Ok(())
    }
}

impl RepetitionResult {
    fn check_invariants(
        &self,
        workload: &WorkloadSpec,
        event_bound: u64,
        labels: &[String],
    ) -> Result<(), String> {
        let label = format!("repetition {}", self.index);
        if self.started_at.trim().is_empty() || !self.started_at.contains('T') {
            return Err(format!(
                "{label}: started_at {:?} is not an RFC 3339 timestamp",
                self.started_at
            ));
        }
        self.throughput
            .check_invariants(&label, self.duration_secs, workload)?;
        self.latency
            .check_ordered(&format!("{label} latency"), false)?;
        if self.latency.count != self.throughput.completed_samples {
            return Err(format!(
                "{label}: latency.count={} disagrees with completed_samples={}",
                self.latency.count, self.throughput.completed_samples
            ));
        }
        if self.throughput.completed_samples > event_bound {
            return Err(format!(
                "{label}: completed_samples={} exceeds the {event_bound} events the measured \
                 window can schedule",
                self.throughput.completed_samples
            ));
        }
        if self.throughput.completed_samples == 0
            && event_bound > 0
            && !labels
                .iter()
                .any(|label| label.as_str() == "invalid-overload")
        {
            return Err(format!(
                "{label}: zero samples while the measured window scheduled up to {event_bound} \
                 events; only a run labelled invalid-overload may have no samples"
            ));
        }
        check_stages(&format!("{label} stages"), &self.stages)
    }
}

fn check_stages(label: &str, stages: &[StageLatency]) -> Result<(), String> {
    for (index, stage) in stages.iter().enumerate() {
        if stages[..index]
            .iter()
            .any(|other| other.stage == stage.stage)
        {
            return Err(format!(
                "{label}: stage {:?} appears more than once",
                stage.stage
            ));
        }
        stage.check_invariants(label)?;
    }
    Ok(())
}

fn check_rate(label: &str, name: &str, reported: f64, derived: f64) -> Result<(), String> {
    let tolerance = RATE_RELATIVE_TOLERANCE * derived.abs().max(1.0);
    if (reported - derived).abs() > tolerance {
        return Err(format!(
            "{label}: {name}={reported} is not the derived {derived} (sample count / \
             duration_secs); do not copy the target rate or invent a rate for an invalid run"
        ));
    }
    Ok(())
}

fn notes_mention(notes: &[String], needle: &str) -> bool {
    notes
        .iter()
        .any(|note| note.to_ascii_lowercase().contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(virtualization: &str, os: &str) -> HostInfo {
        HostInfo {
            hostname: "m4".into(),
            os: os.into(),
            kernel: "Darwin".into(),
            cpu_model: "Apple M4".into(),
            physical_cores: 10,
            logical_cores: 10,
            memory_bytes: 16 * 1024 * 1024 * 1024,
            virtualization: virtualization.into(),
            docker_version: Some("29.9.2".into()),
            governor: None,
            smt_enabled: None,
            isolated_cores: None,
        }
    }

    fn workload(repetitions: u32, batch_size: u32) -> WorkloadSpec {
        WorkloadSpec {
            name: "L1".into(),
            mode: "durable".into(),
            transport: "tcp".into(),
            payload_bytes: 64,
            symbols: vec!["BTC-USD".into()],
            partitions: 16,
            rate_per_sec: 1_000,
            duration_secs: 1,
            warmup_secs: 0,
            repetitions,
            subscribers: 0,
            seed: 7,
            durability: "persisted".into(),
            consumer_offset_durability: "persisted".into(),
            batch_size,
            encryption: false,
        }
    }

    fn summary(count: u64, base: f64) -> LatencySummary {
        LatencySummary {
            count,
            min: base,
            mean: base + 10.0,
            p50: base + 5.0,
            p90: base + 20.0,
            p99: base + 30.0,
            p999: base + 40.0,
            p9999: base + 40.0,
            max: base + 50.0,
            p50_ci95: None,
            p99_ci95: None,
        }
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

    fn repetition(index: u32, completed: u64) -> RepetitionResult {
        let duration_secs = 1.0;
        let latency = if completed == 0 {
            empty_latency()
        } else {
            summary(completed, 10.0)
        };
        let slip = if completed == 0 {
            empty_latency()
        } else {
            summary(completed, 1.0)
        };
        RepetitionResult {
            index,
            started_at: "2026-11-14T00:00:00Z".into(),
            duration_secs,
            throughput: ThroughputSummary {
                target_per_sec: 1_000,
                achieved_per_sec: completed as f64 / duration_secs,
                goodput_per_sec: completed as f64 / duration_secs,
                error_count: 0,
                scheduled_samples: completed,
                completed_samples: completed,
                scheduler_slip: slip,
            },
            latency,
            stages: vec![],
        }
    }

    fn valid_manifest() -> BenchManifest {
        let repetition = repetition(0, 1_000);
        BenchManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            run_id: "1b4e28ba-2fa1-11d2-883f-0016d3cca427".into(),
            created_at: "2026-11-14T00:00:00Z".into(),
            git_sha: "deadbeef".into(),
            git_dirty: false,
            image_digest: Some("sha256:abc".into()),
            host: host("native", "Linux"),
            workload: workload(1, 1),
            throughput: repetition.throughput.clone(),
            latency: repetition.latency.clone(),
            stages: vec![],
            repetitions: vec![repetition],
            notes: vec![
                "Open-loop absolute-deadline pacing; latency measured from the scheduled deadline."
                    .into(),
                "TCP ingress at 127.0.0.1:8081 with one persistent connection.".into(),
            ],
            labels: vec!["headline".into()],
        }
    }

    #[test]
    fn a_valid_manifest_passes_and_round_trips() {
        let manifest = valid_manifest();
        manifest.check_invariants().expect("the fixture is valid");
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let restored: BenchManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, manifest);
        restored
            .check_invariants()
            .expect("a round-tripped manifest keeps its invariants");
    }

    #[test]
    fn unordered_percentiles_are_rejected() {
        let mut manifest = valid_manifest();
        manifest.repetitions[0].latency.p99 = 500.0;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not ordered"), "{error}");

        let mut manifest = valid_manifest();
        manifest.throughput.scheduler_slip.p999 = 0.5;
        manifest.repetitions[0].throughput.scheduler_slip.p999 = 0.5;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not ordered"), "{error}");
    }

    #[test]
    fn negative_and_non_finite_values_are_rejected() {
        let mut manifest = valid_manifest();
        manifest.repetitions[0].latency.min = -1.0;
        manifest.latency.min = -1.0;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("negative"), "{error}");

        let mut manifest = valid_manifest();
        manifest.repetitions[0].latency.p50 = f64::NAN;
        manifest.latency.p50 = f64::NAN;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not a finite"), "{error}");
    }

    #[test]
    fn a_zero_sample_summary_needs_a_zero_event_window() {
        let mut manifest = valid_manifest();
        manifest.repetitions[0] = repetition(0, 0);
        manifest.throughput = manifest.repetitions[0].throughput.clone();
        manifest.latency = manifest.repetitions[0].latency.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("zero samples"), "{error}");

        // The same empty run is legal when the measured window is empty.
        let mut manifest = valid_manifest();
        manifest.workload.duration_secs = 0;
        manifest.workload.batch_size = 1;
        manifest.repetitions[0] = repetition(0, 0);
        manifest.throughput = manifest.repetitions[0].throughput.clone();
        manifest.latency = manifest.repetitions[0].latency.clone();
        manifest
            .check_invariants()
            .expect("a zero-event window has zero samples");

        // Or when a saturated in-flight cap dropped every slot.
        let mut manifest = valid_manifest();
        manifest.repetitions[0] = repetition(0, 0);
        manifest.throughput = manifest.repetitions[0].throughput.clone();
        manifest.latency = manifest.repetitions[0].latency.clone();
        manifest.labels.push("invalid-overload".into());
        manifest.notes.push(
            "INVALID as a latency measurement: 10 slot(s) were dropped as overload by the \
             in-flight cap."
                .into(),
        );
        manifest
            .check_invariants()
            .expect("an overloaded run is labelled instead of inventing samples");
    }

    #[test]
    fn a_stale_number_in_an_empty_summary_is_rejected() {
        let mut manifest = valid_manifest();
        manifest.workload.duration_secs = 0;
        manifest.repetitions[0] = repetition(0, 0);
        manifest.repetitions[0].latency.p99 = 42.0;
        manifest.throughput = manifest.repetitions[0].throughput.clone();
        manifest.latency = manifest.repetitions[0].latency.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("all zeros"), "{error}");
    }

    #[test]
    fn a_fabricated_rate_is_rejected() {
        let mut manifest = valid_manifest();
        manifest.throughput.achieved_per_sec = -1.0;
        manifest.repetitions[0].throughput.achieved_per_sec = -1.0;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("achieved_per_sec is negative"), "{error}");

        let mut manifest = valid_manifest();
        // The target rate is not the achieved rate.
        manifest.throughput.achieved_per_sec = 2_000.0;
        manifest.repetitions[0].throughput.achieved_per_sec = 2_000.0;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not the derived"), "{error}");

        let mut manifest = valid_manifest();
        manifest.repetitions[0].throughput.completed_samples = 999;
        manifest.repetitions[0].latency.count = 999;
        manifest.repetitions[0].throughput.scheduler_slip.count = 999;
        manifest.throughput = manifest.repetitions[0].throughput.clone();
        manifest.latency = manifest.repetitions[0].latency.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not the derived"), "{error}");
    }

    #[test]
    fn a_missing_digest_or_dirty_tree_must_be_reported() {
        let mut manifest = valid_manifest();
        manifest.image_digest = None;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("digest"), "{error}");

        manifest
            .notes
            .push("image_digest is null: WHIPPET_IMAGE_DIGEST was unset for this run.".into());
        manifest
            .check_invariants()
            .expect("a reported gap is honest");

        let mut manifest = valid_manifest();
        manifest.git_dirty = true;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("dirty"), "{error}");

        let mut manifest = valid_manifest();
        manifest.git_sha = "unknown".into();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("commit"), "{error}");
    }

    #[test]
    fn the_headline_must_be_the_median_p99_repetition() {
        let mut manifest = valid_manifest();
        manifest.workload.repetitions = 3;
        let mut high = repetition(0, 1_000);
        high.latency = summary(1_000, 100.0);
        manifest.repetitions = vec![high, repetition(1, 1_000), repetition(2, 1_000)];
        // A "best run" headline picked the high-p99 repetition instead of the
        // median (repetition 1).
        manifest.latency = summary(1_000, 100.0);
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("median-p99"), "{error}");

        let median = manifest.repetitions[1].clone();
        manifest.throughput = median.throughput.clone();
        manifest.latency = median.latency.clone();
        manifest.stages = median.stages.clone();
        manifest
            .check_invariants()
            .expect("the median-p99 repetition is the headline");
    }

    #[test]
    fn repetition_count_and_order_are_checked() {
        let mut manifest = valid_manifest();
        manifest.workload.repetitions = 2;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("repetitions.len()"), "{error}");

        let mut manifest = valid_manifest();
        manifest.workload.repetitions = 2;
        manifest.repetitions = vec![repetition(1, 1_000), repetition(0, 1_000)];
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("in order"), "{error}");
    }

    #[test]
    fn workload_fields_are_validated() {
        for mutate in [
            |spec: &mut WorkloadSpec| spec.rate_per_sec = 0,
            |spec: &mut WorkloadSpec| spec.repetitions = 0,
            |spec: &mut WorkloadSpec| spec.transport = "quic".into(),
            |spec: &mut WorkloadSpec| spec.batch_size = 0,
            |spec: &mut WorkloadSpec| spec.batch_size = MAX_BATCH_ORDERS as u32 + 1,
            |spec: &mut WorkloadSpec| spec.symbols.clear(),
            |spec: &mut WorkloadSpec| spec.partitions = 0,
        ] {
            let mut spec = workload(1, 1);
            mutate(&mut spec);
            assert!(spec.check_invariants().is_err(), "accepted {spec:?}");
        }
        workload(1, 1)
            .check_invariants()
            .expect("the baseline is valid");
    }

    #[test]
    fn the_event_bound_includes_the_partial_final_batch() {
        let single = workload(1, 1);
        assert_eq!(single.measured_event_upper_bound(), 1_000);
        let batch = workload(1, 10);
        assert_eq!(batch.measured_event_upper_bound(), 1_009);
    }

    #[test]
    fn stages_must_be_unique_ordered_and_non_negative() {
        let mut manifest = valid_manifest();
        let stage = StageLatency {
            stage: "send_us".into(),
            latency: summary(10, 1.0),
        };
        manifest.stages = vec![stage.clone(), stage];
        manifest.repetitions[0].stages = manifest.stages.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("more than once"), "{error}");

        let mut manifest = valid_manifest();
        manifest.stages = vec![StageLatency {
            stage: "send_us".into(),
            latency: summary(10, 1.0),
        }];
        manifest.stages[0].latency.p99 = 0.5;
        manifest.repetitions[0].stages = manifest.stages.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("not ordered"), "{error}");

        let mut manifest = valid_manifest();
        manifest.stages = vec![StageLatency {
            stage: "wire_us".into(),
            latency: summary(10, 1.0),
        }];
        manifest.stages[0].latency.p50 = -1.0;
        manifest.repetitions[0].stages = manifest.stages.clone();
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("negative"), "{error}");
    }

    #[test]
    fn the_server_offset_stage_is_signed() {
        let mut manifest = valid_manifest();
        let offset = StageLatency {
            stage: SIGNED_STAGE_NAME.into(),
            latency: LatencySummary {
                count: 10,
                min: -5_000.0,
                mean: -100.0,
                p50: -100.0,
                p90: 50.0,
                p99: 100.0,
                p999: 100.0,
                p9999: 100.0,
                max: 2_000.0,
                p50_ci95: None,
                p99_ci95: None,
            },
        };
        manifest.stages = vec![offset];
        manifest.repetitions[0].stages = manifest.stages.clone();
        manifest
            .check_invariants()
            .expect("a negative clock offset is a valid stage");
    }

    #[test]
    fn labels_must_carry_the_host_scope_and_matching_notes() {
        for (virtualization, os, expected) in [
            ("native", "Linux", "headline"),
            ("native", "Darwin", "dev-loop"),
            ("docker-desktop", "Linux", "dev-loop"),
            ("orbstack", "Linux", "dev-loop"),
            ("linux-vm", "Linux", "dev-loop"),
            ("container", "Linux", "dev-loop"),
            ("native", "Windows", "dev-loop"),
        ] {
            assert_eq!(host(virtualization, os).scope_label(), expected);
        }

        let mut manifest = valid_manifest();
        manifest.labels = vec!["dev-loop".into()];
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("host scope"), "{error}");

        let mut manifest = valid_manifest();
        manifest.labels.push("steady-state".into());
        manifest.labels.push("steady-state-skipped".into());
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("conflicting"), "{error}");
    }

    #[test]
    fn validity_labels_need_the_matching_note() {
        let mut manifest = valid_manifest();
        manifest.labels.push("generator-bound".into());
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("generator-bound"), "{error}");

        manifest.notes.push(
            "Repetition 0 is generator-bound: scheduler slip p99 is 10% of measured p99.".into(),
        );
        manifest.check_invariants().unwrap();

        let mut manifest = valid_manifest();
        manifest.labels.push("invalid-overload".into());
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("invalid-overload"), "{error}");
    }

    #[test]
    fn the_method_transport_and_batch_must_be_in_the_notes() {
        let mut manifest = valid_manifest();
        manifest.notes.retain(|note| !note.contains("deadline"));
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("deadline"), "{error}");

        let mut manifest = valid_manifest();
        manifest.notes.retain(|note| !note.contains("TCP"));
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("transport"), "{error}");

        let mut manifest = valid_manifest();
        manifest.workload.batch_size = 10;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("batch_size"), "{error}");
        manifest
            .notes
            .push("Batch mode: up to 10 orders per POST /api/orders/batch.".into());
        manifest.check_invariants().unwrap();
    }

    #[test]
    fn the_host_must_be_named() {
        let mut manifest = valid_manifest();
        manifest.host.cpu_model.clear();
        assert!(manifest.check_invariants().is_err());
        let mut manifest = valid_manifest();
        manifest.host.logical_cores = 0;
        assert!(manifest.check_invariants().is_err());
    }

    #[test]
    fn the_schema_version_is_pinned() {
        let mut manifest = valid_manifest();
        manifest.schema_version = MANIFEST_SCHEMA_VERSION + 1;
        let error = manifest.check_invariants().unwrap_err();
        assert!(error.contains("schema_version"), "{error}");
    }
}
