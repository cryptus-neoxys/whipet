// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Bounded-storage guard: free-space measurement for the node's own data
//! volume and for Iggy's data volume.
//!
//! The improvement plan (section 4.3) asks for a volume-usage signal before the
//! log fills the disk: export free bytes, alert at configurable warning and
//! critical thresholds, and give readiness/admission a decision it can consult.
//! This module owns the measurement and that decision. It never deletes log
//! data and never touches order validation; retention stays a separate
//! problem (section 4.4).
//!
//! ```
//! use whippet::storage::{StorageConfig, StorageMonitor, StorageState};
//!
//! let monitor = StorageMonitor::new(StorageConfig::default());
//! // A stat failure is reported as `Warn`, never as `Critical`.
//! assert!(matches!(
//!     monitor.state(),
//!     StorageState::Ok | StorageState::Warn | StorageState::Critical
//! ));
//! ```

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tracing::{debug, info, warn};

/// Usage percentage that raises [`StorageState::Warn`] by default.
pub const DEFAULT_WARN_PCT: u8 = 70;
/// Usage percentage that raises [`StorageState::Critical`] by default.
pub const DEFAULT_CRIT_PCT: u8 = 85;
/// Default root of the node's data volume (`WHIPPET_DATA_DIR`).
pub const DEFAULT_DATA_DIR: &str = "/data";
/// Default mount point of Iggy's data volume (`WHIPPET_IGGY_DATA_DIR`).
pub const DEFAULT_IGGY_DATA_DIR: &str = "/iggy-data";
/// Name reported for the node's own data volume.
pub const WHIPPET_VOLUME: &str = "whippet";
/// Name reported for Iggy's data volume.
pub const IGGY_VOLUME: &str = "iggy";

/// Thresholds and volume roots for the storage monitor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    /// Usage at or above this percentage is a warning.
    pub warn_pct: u8,
    /// Usage at or above this percentage is critical.
    pub crit_pct: u8,
    /// Directory on the node's own data volume. The monitor samples the
    /// filesystem that holds it, not the directory's size.
    pub whippet_dir: PathBuf,
    /// Directory on Iggy's data volume, typically a read-only mount. `None`
    /// (or a path that does not exist at startup) means only the Whippet
    /// volume is sampled.
    pub iggy_dir: Option<PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            warn_pct: DEFAULT_WARN_PCT,
            crit_pct: DEFAULT_CRIT_PCT,
            whippet_dir: PathBuf::from(DEFAULT_DATA_DIR),
            iggy_dir: None,
        }
    }
}

impl StorageConfig {
    /// Reject thresholds that cannot produce a useful two-level alert: each
    /// must be inside `1..=99` and the warning must come first. A misconfigured
    /// pair is a startup error, not a silently ignored knob.
    pub fn validate(&self) -> Result<(), String> {
        if self.warn_pct == 0 || self.warn_pct > 99 {
            return Err(format!(
                "WHIPPET_DISK_WARN_PCT must be between 1 and 99, got {}",
                self.warn_pct
            ));
        }
        if self.crit_pct == 0 || self.crit_pct > 99 {
            return Err(format!(
                "WHIPPET_DISK_CRIT_PCT must be between 1 and 99, got {}",
                self.crit_pct
            ));
        }
        if self.warn_pct >= self.crit_pct {
            return Err(format!(
                "WHIPPET_DISK_WARN_PCT ({}) must be lower than WHIPPET_DISK_CRIT_PCT ({})",
                self.warn_pct, self.crit_pct
            ));
        }
        Ok(())
    }
}

/// Aggregate storage pressure. Ordered from healthiest to worst by
/// [`worse`], but deliberately not `Ord`: only the monitor's own combination
/// rule defines the ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageState {
    /// Every known volume is below the warning threshold.
    Ok,
    /// A known volume crossed the warning threshold, or a volume's usage could
    /// not be read. Alert, but do not stop accepting orders.
    Warn,
    /// A known volume crossed the critical threshold. Readiness and durable
    /// admission should fail here.
    Critical,
}

impl StorageState {
    /// Stable lowercase label for logs and, later, metric labels.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Critical => "critical",
        }
    }
}

/// One volume's free-space reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeStat {
    /// Volume label, [`WHIPPET_VOLUME`] or [`IGGY_VOLUME`].
    pub name: &'static str,
    /// Bytes available to this process, i.e. `f_bavail` scaled by the block
    /// size. Reserved blocks are not counted as free.
    pub free_bytes: u64,
    /// Bytes total on the filesystem.
    pub total_bytes: u64,
    /// Whole-percent usage, rounded up. `0` when unavailable.
    pub used_pct: u8,
    /// `false` when `statvfs` failed. The reading is then all zeroes and the
    /// volume contributes [`StorageState::Warn`], never `Critical`.
    pub available: bool,
}

impl VolumeStat {
    /// The placeholder for a volume whose usage could not be read.
    fn unknown(name: &'static str) -> Self {
        Self {
            name,
            free_bytes: 0,
            total_bytes: 0,
            used_pct: 0,
            available: false,
        }
    }
}

/// One complete sample. `Copy` on purpose: callers take it on every request
/// path and never share a lock or an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageSnapshot {
    /// The node's own data volume.
    pub whippet: VolumeStat,
    /// Iggy's data volume, when one is configured and mounted.
    pub iggy: Option<VolumeStat>,
    /// Worst state across the known volumes.
    pub state: StorageState,
    /// When this sample was taken.
    pub checked_at: Instant,
}

/// Samples both data volumes on refresh and caches the latest result.
///
/// The monitor is shared through an `Arc` and lives on `NodeContext`; the
/// gateway and TCP listener read [`StorageMonitor::state`] without doing I/O.
/// A background task calls [`StorageMonitor::refresh`] every five seconds.
pub struct StorageMonitor {
    config: StorageConfig,
    snapshot: RwLock<StorageSnapshot>,
}

impl StorageMonitor {
    /// Build the monitor and take the first sample immediately, so the state
    /// is known before the context is published.
    pub fn new(config: StorageConfig) -> Arc<Self> {
        let monitor = Arc::new(Self {
            config,
            snapshot: RwLock::new(StorageSnapshot {
                whippet: VolumeStat::unknown(WHIPPET_VOLUME),
                iggy: None,
                state: StorageState::Ok,
                checked_at: Instant::now(),
            }),
        });
        let first = monitor.collect();
        log_volume_availability(None, &first);
        monitor.store(first);
        monitor
    }

    /// Re-sample both volumes, cache and return the result.
    ///
    /// A `statvfs` failure is not an error here: it produces an unavailable
    /// reading, which contributes `Warn`. Ingress stays open on a transient
    /// stat failure; only measured critical usage blocks it.
    pub fn refresh(&self) -> StorageSnapshot {
        let previous = self.snapshot();
        let next = self.collect();
        log_volume_availability(Some(&previous), &next);
        self.store(next);
        next
    }

    /// The cached sample. Cheap: a lock read and a `Copy`.
    pub fn snapshot(&self) -> StorageSnapshot {
        *self
            .snapshot
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The cached aggregate state, for readiness and admission checks.
    pub fn state(&self) -> StorageState {
        self.snapshot().state
    }

    /// Thresholds and volume roots this monitor was built with.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    fn collect(&self) -> StorageSnapshot {
        let whippet = sample(WHIPPET_VOLUME, &self.config.whippet_dir);
        let iggy = self
            .config
            .iggy_dir
            .as_deref()
            .map(|dir| sample(IGGY_VOLUME, dir));
        StorageSnapshot {
            state: state_for(&whippet, iggy.as_ref(), &self.config),
            whippet,
            iggy,
            checked_at: Instant::now(),
        }
    }

    fn store(&self, snapshot: StorageSnapshot) {
        match self.snapshot.write() {
            Ok(mut guard) => *guard = snapshot,
            Err(poisoned) => *poisoned.into_inner() = snapshot,
        }
    }
}

/// Pure threshold decision, so tests can cover the boundaries without a disk.
///
/// `used_pct >= crit_pct` is critical, otherwise `used_pct >= warn_pct` is a
/// warning. An unvalidated pair (`crit_pct <= warn_pct`) still behaves: the
/// critical band wins.
pub fn classify(used_pct: u8, warn_pct: u8, crit_pct: u8) -> StorageState {
    if used_pct >= crit_pct {
        StorageState::Critical
    } else if used_pct >= warn_pct {
        StorageState::Warn
    } else {
        StorageState::Ok
    }
}

/// Worst state across the known volumes. An unreadable volume counts as
/// [`StorageState::Warn`]; it can never mask a different volume's `Critical`.
fn state_for(
    whippet: &VolumeStat,
    iggy: Option<&VolumeStat>,
    config: &StorageConfig,
) -> StorageState {
    let mut state = volume_state(whippet, config);
    if let Some(iggy) = iggy {
        state = worse(state, volume_state(iggy, config));
    }
    state
}

fn volume_state(stat: &VolumeStat, config: &StorageConfig) -> StorageState {
    if stat.available {
        classify(stat.used_pct, config.warn_pct, config.crit_pct)
    } else {
        StorageState::Warn
    }
}

fn worse(a: StorageState, b: StorageState) -> StorageState {
    match (a, b) {
        (StorageState::Critical, _) | (_, StorageState::Critical) => StorageState::Critical,
        (StorageState::Warn, _) | (_, StorageState::Warn) => StorageState::Warn,
        (StorageState::Ok, StorageState::Ok) => StorageState::Ok,
    }
}

/// Read one filesystem's usage.
///
/// `statvfs` reports a 32-bit `f_frsize` on some filesystems and zero on a few
/// old ones, so the block size falls back to `f_bsize`; free and total must use
/// the same unit or the percentage is nonsense.
fn sample(name: &'static str, dir: &Path) -> VolumeStat {
    match rustix::fs::statvfs(dir) {
        Ok(stat) => {
            let block = if stat.f_frsize == 0 {
                stat.f_bsize
            } else {
                stat.f_frsize
            };
            let total_bytes = stat.f_blocks.saturating_mul(block);
            let free_bytes = stat.f_bavail.saturating_mul(block);
            VolumeStat {
                name,
                free_bytes,
                total_bytes,
                used_pct: usage_pct(total_bytes, free_bytes),
                available: true,
            }
        }
        Err(error) => {
            // The error is debug-level detail; the transition log below is the
            // operator-facing signal, and it does not repeat every cycle.
            debug!(
                volume = name,
                path = %dir.display(),
                error = %error,
                "could not read filesystem usage"
            );
            VolumeStat::unknown(name)
        }
    }
}

/// Whole-percent usage, rounded up. Zero total (an empty or synthetic
/// filesystem) reads as `0`, never a division error.
fn usage_pct(total_bytes: u64, free_bytes: u64) -> u8 {
    if total_bytes == 0 {
        return 0;
    }
    let used = total_bytes.saturating_sub(free_bytes);
    // u128 keeps `used * 100` from wrapping on a multi-petabyte volume.
    ((u128::from(used) * 100).div_ceil(u128::from(total_bytes))).min(100) as u8
}

/// Log only availability transitions. A permanently unreadable mount does not
/// repeat this line every five seconds.
fn log_volume_availability(previous: Option<&StorageSnapshot>, next: &StorageSnapshot) {
    let volumes = [
        (
            previous.map(|snapshot| snapshot.whippet),
            Some(next.whippet),
        ),
        (previous.and_then(|snapshot| snapshot.iggy), next.iggy),
    ];
    for (before, after) in volumes {
        let Some(after) = after else { continue };
        if before.map(|stat| stat.available) == Some(after.available) {
            continue;
        }
        if after.available {
            info!(
                volume = after.name,
                free_bytes = after.free_bytes,
                used_pct = after.used_pct,
                "volume usage is readable"
            );
        } else {
            warn!(
                volume = after.name,
                "volume usage is unavailable; a stat failure alone reports warn, not critical"
            );
        }
    }
}

#[cfg(test)]
impl StorageMonitor {
    /// Test-only injection: serve a fixed snapshot without touching the
    /// filesystem, so a test can force `Critical` without filling a disk.
    pub fn from_snapshot(snapshot: StorageSnapshot) -> Arc<Self> {
        Arc::new(Self {
            config: StorageConfig::default(),
            snapshot: RwLock::new(snapshot),
        })
    }

    /// Test-only: replace the cached snapshot without sampling.
    pub fn set_snapshot(&self, snapshot: StorageSnapshot) {
        self.store(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(name: &'static str, used_pct: u8) -> VolumeStat {
        VolumeStat {
            name,
            free_bytes: 30,
            total_bytes: 100,
            used_pct,
            available: true,
        }
    }

    fn snapshot(
        whippet: VolumeStat,
        iggy: Option<VolumeStat>,
        state: StorageState,
    ) -> StorageSnapshot {
        StorageSnapshot {
            whippet,
            iggy,
            state,
            checked_at: Instant::now(),
        }
    }

    #[test]
    fn usage_rounds_up_and_never_wraps() {
        assert_eq!(usage_pct(0, 0), 0);
        assert_eq!(usage_pct(100, 100), 0);
        assert_eq!(usage_pct(100, 70), 30);
        assert_eq!(usage_pct(1_000, 999), 1);
        // 0.1% used still reads as one percent, so a threshold at 1 fires.
        assert_eq!(usage_pct(1_000, 1), 100);
        assert_eq!(usage_pct(100, 0), 100);
        // Free larger than total (a race between the two counters) saturates.
        assert_eq!(usage_pct(100, 200), 0);
        assert_eq!(usage_pct(u64::MAX, 0), 100);
    }

    #[test]
    fn thresholds_are_two_levels_with_the_critical_band_winning() {
        assert_eq!(classify(69, 70, 85), StorageState::Ok);
        assert_eq!(classify(70, 70, 85), StorageState::Warn);
        assert_eq!(classify(84, 70, 85), StorageState::Warn);
        assert_eq!(classify(85, 70, 85), StorageState::Critical);
        assert_eq!(classify(100, 70, 85), StorageState::Critical);
    }

    #[test]
    fn the_worst_known_volume_wins_and_unknown_is_only_a_warning() {
        let config = StorageConfig::default();
        assert_eq!(
            state_for(&stat(WHIPPET_VOLUME, 10), None, &config),
            StorageState::Ok
        );
        assert_eq!(
            state_for(
                &stat(WHIPPET_VOLUME, 10),
                Some(&stat(IGGY_VOLUME, 90)),
                &config
            ),
            StorageState::Critical
        );
        assert_eq!(
            state_for(
                &stat(WHIPPET_VOLUME, 75),
                Some(&stat(IGGY_VOLUME, 10)),
                &config
            ),
            StorageState::Warn
        );
        // An unreadable volume is a warning; it must not become critical on its
        // own, and it must not hide a critical sibling.
        assert_eq!(
            state_for(&VolumeStat::unknown(WHIPPET_VOLUME), None, &config),
            StorageState::Warn
        );
        assert_eq!(
            state_for(
                &VolumeStat::unknown(WHIPPET_VOLUME),
                Some(&stat(IGGY_VOLUME, 90)),
                &config
            ),
            StorageState::Critical
        );
    }

    #[test]
    fn config_validation_bounds_both_thresholds_and_their_order() {
        let mut config = StorageConfig::default();
        assert!(config.validate().is_ok());
        for (warn, crit) in [(70, 85), (1, 2), (98, 99)] {
            config.warn_pct = warn;
            config.crit_pct = crit;
            assert!(config.validate().is_ok(), "{warn}/{crit} must be accepted");
        }
        for (warn, crit) in [(0, 85), (70, 100), (100, 100), (85, 85), (90, 80), (0, 0)] {
            config.warn_pct = warn;
            config.crit_pct = crit;
            assert!(config.validate().is_err(), "{warn}/{crit} must be rejected");
        }
    }

    #[test]
    fn an_injected_critical_snapshot_is_readable_and_cloneable() {
        let injected = snapshot(
            stat(WHIPPET_VOLUME, 91),
            Some(stat(IGGY_VOLUME, 12)),
            StorageState::Critical,
        );
        let monitor = StorageMonitor::from_snapshot(injected);
        assert_eq!(monitor.state(), StorageState::Critical);
        let first = monitor.snapshot();
        let second = monitor.snapshot();
        assert_eq!(first, second);
        assert_eq!(first.whippet.used_pct, 91);
        assert_eq!(first.iggy.map(|stat| stat.used_pct), Some(12));
        assert_eq!(monitor.config().warn_pct, DEFAULT_WARN_PCT);

        // The injected snapshot is replaceable without sampling the disk.
        monitor.set_snapshot(snapshot(stat(WHIPPET_VOLUME, 10), None, StorageState::Ok));
        assert_eq!(monitor.state(), StorageState::Ok);
    }
}
