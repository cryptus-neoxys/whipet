// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Soak-test summary helpers.
//!
//! The soak script samples `/api/state` every 10 s during a long constant-rate
//! run and fails when the tail p99 drifts more than 5% between the first and
//! last 10% of samples, or when a shard reports failure. The drift arithmetic
//! lives here so it is unit-testable; `scripts/soak.sh` mirrors it in Python
//! for the shell-side summary.

/// Share of samples compared at each end of a soak run.
pub const SOAK_EDGE_FRACTION: f64 = 0.10;
/// Drift beyond this percentage fails the soak.
pub const SOAK_DRIFT_PCT: f64 = 5.0;
/// Minimum samples for a drift verdict; fewer is "skipped", never "passed".
pub const SOAK_MIN_SAMPLES: usize = 20;

/// One `/api/state` poll during the soak.
#[derive(Debug, Clone, Copy, Default)]
pub struct SoakSample {
    /// Seconds since the soak started.
    pub t_secs: u64,
    /// Server-reported ingress p50, microseconds.
    pub p50_us: f64,
    /// Server-reported ingress p99, microseconds.
    pub p99_us: f64,
    /// Server-reported consumer lag.
    pub lag: u64,
    /// Node RSS in bytes, when the sampler could read it.
    pub rss_bytes: Option<u64>,
}

/// Relative drift from `first` to `last`, in percent. A zero baseline drifts
/// 0% when the tail is also zero and +inf otherwise.
pub fn drift_pct(first: f64, last: f64) -> f64 {
    if first == 0.0 {
        if last == 0.0 { 0.0 } else { f64::INFINITY }
    } else {
        (last - first) / first * 100.0
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

/// Mean p99 of the first 10% of samples versus the last 10%. Returns `None`
/// when fewer than [`SOAK_MIN_SAMPLES`] samples were collected.
pub fn soak_drift_p99(samples: &[SoakSample]) -> Option<f64> {
    if samples.len() < SOAK_MIN_SAMPLES {
        return None;
    }
    let edge = ((samples.len() as f64 * SOAK_EDGE_FRACTION).floor() as usize).max(1);
    let first: Vec<f64> = samples
        .iter()
        .take(edge)
        .map(|sample| sample.p99_us)
        .collect();
    let last: Vec<f64> = samples
        .iter()
        .rev()
        .take(edge)
        .map(|sample| sample.p99_us)
        .collect();
    match (mean(&first), mean(&last)) {
        (Some(first), Some(last)) => Some(drift_pct(first, last)),
        _ => None,
    }
}

/// Outcome of the soak verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoakVerdict {
    /// p99 drift within ±5% and every shard healthy.
    Pass,
    /// p99 drift exceeded ±5% or a shard reported failure.
    Fail,
    /// Too few samples to compare the edges.
    Skipped,
}

/// Fail when a shard reported failure or when the p99 drift between the first
/// and last 10% of samples exceeds ±5%.
pub fn soak_verdict(samples: &[SoakSample], shards_ok: bool) -> SoakVerdict {
    if !shards_ok {
        return SoakVerdict::Fail;
    }
    match soak_drift_p99(samples) {
        None => SoakVerdict::Skipped,
        Some(drift) if drift > SOAK_DRIFT_PCT => SoakVerdict::Fail,
        Some(_) => SoakVerdict::Pass,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flat(n: usize, p99: f64) -> Vec<SoakSample> {
        (0..n)
            .map(|index| SoakSample {
                t_secs: index as u64 * 10,
                p50_us: p99 / 2.0,
                p99_us: p99,
                lag: 0,
                rss_bytes: None,
            })
            .collect()
    }

    #[test]
    fn drift_is_relative_to_the_first_edge() {
        assert_eq!(drift_pct(1_000.0, 1_000.0), 0.0);
        assert!((drift_pct(1_000.0, 1_060.0) - 6.0).abs() < 1e-9);
        assert!((drift_pct(1_000.0, 940.0) + 6.0).abs() < 1e-9);
        assert_eq!(drift_pct(0.0, 0.0), 0.0);
        assert_eq!(drift_pct(0.0, 5.0), f64::INFINITY);
    }

    #[test]
    fn flat_soak_passes_with_zero_drift() {
        let samples = flat(60, 1_000.0);
        assert_eq!(soak_drift_p99(&samples), Some(0.0));
        assert_eq!(soak_verdict(&samples, true), SoakVerdict::Pass);
    }

    #[test]
    fn drifting_tail_fails_the_soak() {
        let mut samples = flat(60, 1_000.0);
        for sample in samples.iter_mut().rev().take(6) {
            sample.p99_us = 1_200.0;
        }
        let drift = soak_drift_p99(&samples).unwrap();
        assert!(drift > 19.0, "drift was {drift}");
        assert_eq!(soak_verdict(&samples, true), SoakVerdict::Fail);
    }

    #[test]
    fn failed_shard_fails_even_a_flat_run() {
        let samples = flat(60, 1_000.0);
        assert_eq!(soak_verdict(&samples, false), SoakVerdict::Fail);
    }

    #[test]
    fn short_soak_is_skipped_not_passed() {
        let samples = flat(10, 1_000.0);
        assert_eq!(soak_drift_p99(&samples), None);
        assert_eq!(soak_verdict(&samples, true), SoakVerdict::Skipped);
    }
}
