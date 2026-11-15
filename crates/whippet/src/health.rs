// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Explicit per-shard, per-role lifecycle.
//!
//! Readiness used to be a single `AtomicBool` that started `true` and was only
//! cleared after a failure. A node could therefore report ready before its
//! engine or market-data task had started, and the health view could not say
//! which shard or role was missing. This module makes the state machine
//! explicit: every active shard runs one engine task and one MD task, and each
//! role moves through the same states.
//!
//! ```text
//! Starting --started--> Running --shutdown--> Stopped
//!    |                     |
//!    +-------error---------+--> Failed      (terminal)
//! ```
//!
//! `Failed` and `Stopped` are terminal: a role never leaves them without a
//! process restart. The node is ready only when **every** active shard's engine
//! and MD roles are `Running`.
//!
//! The state is one `AtomicU8` per `(shard, role)`, so the readiness check and
//! every transition are lock-free. A transition happens once at startup, once
//! at shutdown, and once on the failure path — never per command.

use std::sync::atomic::{AtomicU8, Ordering};

use whippet_core::api::ShardHealthView;

/// Partitions this tracker can represent. `NodeConfig` rejects more than 256,
/// so every shard a node can route has a slot.
pub const MAX_SHARDS: usize = 256;

/// Two roles per shard: the engine (matching) task and the MD (market-data
/// fanout) task.
const ROLES: usize = 2;

/// The lifecycle of one shard role.
///
/// The numeric codes are the on-the-wire/atomic representation; keep them
/// stable and add new states at the end.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardState {
    /// The task is spawned (or about to be) and has not reached its loop.
    Starting = 0,
    /// The task finished its startup replay/initialisation and is serving.
    Running = 1,
    /// The task hit an error and stopped. Terminal until a process restart.
    Failed = 2,
    /// The task exited because the node is shutting down. Terminal.
    Stopped = 3,
}

impl ShardState {
    /// The stable string form used by `/api/health` and log fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }

    /// The atomic representation of this state.
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// Decode an atomic representation. `None` for an unknown byte.
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Starting),
            1 => Some(Self::Running),
            2 => Some(Self::Failed),
            3 => Some(Self::Stopped),
            _ => None,
        }
    }

    /// A role that will not run again until the process restarts.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Stopped)
    }

    /// Whether `next` is a legal successor of this state.
    ///
    /// `Starting -> Running | Failed`, `Running -> Failed | Stopped`, and the
    /// identity transition (repeating the current state) are legal. `Failed`
    /// and `Stopped` are terminal; in particular `Failed -> Running` is the bug
    /// this table exists to catch: a failed role must never silently become
    /// ready again.
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Starting, Self::Starting)
                | (Self::Starting, Self::Running)
                | (Self::Starting, Self::Failed)
                | (Self::Running, Self::Running)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Stopped)
                | (Self::Failed, Self::Failed)
                | (Self::Stopped, Self::Stopped)
        )
    }
}

/// Which of a shard's two tasks a state belongs to.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRole {
    /// The matching engine task (`run_engine_shard`).
    Engine = 0,
    /// The market-data fanout task (`run_md_shard`).
    Md = 1,
}

impl ShardRole {
    /// The stable string form used by `/api/health` and log fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Md => "md",
        }
    }

    const fn index(self) -> usize {
        self as u8 as usize
    }
}

/// Lifecycle state for every `(shard, role)` a node can run.
#[derive(Debug)]
pub struct ShardHealth {
    /// `states[shard * ROLES + role]`; zero-initialised, so every slot starts
    /// `Starting`.
    states: [AtomicU8; MAX_SHARDS * ROLES],
}

impl Default for ShardHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardHealth {
    /// A tracker with every slot `Starting`.
    pub const fn new() -> Self {
        Self {
            states: [const { AtomicU8::new(ShardState::Starting as u8) }; MAX_SHARDS * ROLES],
        }
    }

    /// The slot for `(shard, role)`, or `None` when the shard id is outside the
    /// representable range (a routing bug: `NodeConfig` caps partitions at 256).
    const fn slot(shard: u32, role: ShardRole) -> Option<usize> {
        if shard < MAX_SHARDS as u32 {
            Some(shard as usize * ROLES + role.index())
        } else {
            None
        }
    }

    /// The current state of one role.
    pub fn get(&self, shard: u32, role: ShardRole) -> ShardState {
        let Some(slot) = Self::slot(shard, role) else {
            debug_assert!(
                false,
                "shard {shard} exceeds the {MAX_SHARDS}-partition lifecycle table"
            );
            return ShardState::Failed;
        };
        self.load(&self.states[slot])
    }

    /// Apply a transition and return the previous state.
    ///
    /// An illegal transition is a bug: debug builds assert, and release builds
    /// keep the current state so a terminal `Failed`/`Stopped` can never be
    /// overwritten by a later `Running` (readiness must not recover from a
    /// failure without a restart). Repeating the current state is a no-op.
    pub fn set(&self, shard: u32, role: ShardRole, next: ShardState) -> ShardState {
        let Some(slot) = Self::slot(shard, role) else {
            debug_assert!(
                false,
                "shard {shard} exceeds the {MAX_SHARDS}-partition lifecycle table"
            );
            return ShardState::Failed;
        };
        let cell = &self.states[slot];
        let mut current = self.load(cell);
        loop {
            // `can_transition_to` includes the identity transition, so a
            // repeated `Stopped`/`Failed` is a no-op rather than a bug.
            let applied = if current.can_transition_to(next) {
                next
            } else {
                debug_assert!(
                    false,
                    "illegal shard transition {:?} -> {:?} for shard {shard} role {}",
                    current,
                    next,
                    role.as_str()
                );
                current
            };
            match cell.compare_exchange_weak(
                current.code(),
                applied.code(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return current,
                Err(observed) => current = Self::decode(observed),
            }
        }
    }

    /// True when every engine and MD role of every active shard is `Running`.
    ///
    /// An empty `active` set is vacuously ready; the node always starts its
    /// tracker with the configured shards, and the test harness decides
    /// explicitly whether it models started shards.
    pub fn all_running(&self, active: &[u32]) -> bool {
        active.iter().all(|&shard| {
            self.get(shard, ShardRole::Engine) == ShardState::Running
                && self.get(shard, ShardRole::Md) == ShardState::Running
        })
    }

    /// Per-shard state strings for the health view, in `active` order.
    pub fn snapshot(&self, active: &[u32]) -> Vec<ShardHealthView> {
        active
            .iter()
            .map(|&shard| ShardHealthView {
                partition: shard,
                engine: self.get(shard, ShardRole::Engine).as_str().to_string(),
                md: self.get(shard, ShardRole::Md).as_str().to_string(),
            })
            .collect()
    }

    fn load(&self, cell: &AtomicU8) -> ShardState {
        Self::decode(cell.load(Ordering::Acquire))
    }

    /// An unknown byte cannot come from this module; treat it as `Failed` so a
    /// corrupt slot fails closed instead of reading as ready.
    fn decode(code: u8) -> ShardState {
        ShardState::from_code(code).unwrap_or(ShardState::Failed)
    }
}

/// A copy of the current lifecycle, for a context built over the same node
/// (test helpers that override config). The copy is independent.
impl Clone for ShardHealth {
    fn clone(&self) -> Self {
        let copy = Self::new();
        for (to, from) in copy.states.iter().zip(self.states.iter()) {
            to.store(from.load(Ordering::Acquire), Ordering::Relaxed);
        }
        copy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The transition table, spelled out row by row so a change to
    /// `can_transition_to` has to update the expectation too.
    #[test]
    fn the_transition_table_is_exhaustive() {
        use ShardState::{Failed, Running, Starting, Stopped};
        let all = [Starting, Running, Failed, Stopped];
        for current in all {
            for next in all {
                let expected = matches!(
                    (current, next),
                    (Starting, Starting)
                        | (Starting, Running)
                        | (Starting, Failed)
                        | (Running, Running)
                        | (Running, Failed)
                        | (Running, Stopped)
                        | (Failed, Failed)
                        | (Stopped, Stopped)
                );
                assert_eq!(
                    current.can_transition_to(next),
                    expected,
                    "{current:?} -> {next:?}"
                );
            }
        }
    }

    #[test]
    fn a_failed_role_is_terminal() {
        use ShardState::{Failed, Running, Starting, Stopped};
        assert!(Starting.can_transition_to(Failed));
        assert!(Running.can_transition_to(Failed));
        for next in [Starting, Running, Stopped] {
            assert!(
                !Failed.can_transition_to(next),
                "a failed role must never become {next:?}"
            );
        }
        assert!(!Stopped.can_transition_to(Running));
        assert!(Failed.is_terminal() && Stopped.is_terminal());
        assert!(!Starting.is_terminal() && !Running.is_terminal());
    }

    #[test]
    fn state_codes_round_trip() {
        for state in [
            ShardState::Starting,
            ShardState::Running,
            ShardState::Failed,
            ShardState::Stopped,
        ] {
            assert_eq!(ShardState::from_code(state.code()), Some(state));
            assert!(!state.as_str().is_empty());
        }
        assert_eq!(ShardState::from_code(4), None);
    }

    #[test]
    fn a_fresh_tracker_is_not_ready() {
        let health = ShardHealth::new();
        let active = [0, 3, 7];
        assert!(!health.all_running(&active), "Starting is not Running");
        for &shard in &active {
            assert_eq!(health.get(shard, ShardRole::Engine), ShardState::Starting);
            assert_eq!(health.get(shard, ShardRole::Md), ShardState::Starting);
        }
        // An empty active set is vacuously ready: a node that runs no local
        // shard has nothing to wait for.
        assert!(health.all_running(&[]));
    }

    #[test]
    fn readiness_requires_both_roles_of_every_active_shard() {
        let health = ShardHealth::new();
        let active = [0, 3];
        let run = |shard, role| {
            health.set(shard, role, ShardState::Starting);
            health.set(shard, role, ShardState::Running);
        };
        run(0, ShardRole::Engine);
        run(0, ShardRole::Md);
        run(3, ShardRole::Engine);
        assert!(
            !health.all_running(&active),
            "shard 3's MD role has not started"
        );
        run(3, ShardRole::Md);
        assert!(health.all_running(&active));

        health.set(3, ShardRole::Md, ShardState::Failed);
        assert!(!health.all_running(&active), "one Failed role is enough");
    }

    #[test]
    fn a_stopped_role_is_not_ready() {
        let health = ShardHealth::new();
        let active = [1];
        health.set(1, ShardRole::Engine, ShardState::Running);
        health.set(1, ShardRole::Md, ShardState::Running);
        assert!(health.all_running(&active));
        health.set(1, ShardRole::Engine, ShardState::Stopped);
        assert!(!health.all_running(&active));
    }

    #[test]
    fn a_repeated_failure_is_idempotent() {
        let health = ShardHealth::new();
        let previous = health.set(2, ShardRole::Engine, ShardState::Failed);
        assert_eq!(previous, ShardState::Starting);
        let previous = health.set(2, ShardRole::Engine, ShardState::Failed);
        assert_eq!(
            previous,
            ShardState::Failed,
            "the second failure is a no-op, not an assertion failure"
        );
        assert_eq!(health.get(2, ShardRole::Engine), ShardState::Failed);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn running_after_failed_asserts_in_debug_builds() {
        let health = ShardHealth::new();
        health.set(0, ShardRole::Engine, ShardState::Failed);
        let previous = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            health.set(0, ShardRole::Engine, ShardState::Running)
        }));
        assert!(previous.is_err(), "a failed role must not become Running");
        // The panic happened before the store, so the state is still Failed.
        assert_eq!(health.get(0, ShardRole::Engine), ShardState::Failed);
    }

    #[test]
    fn the_snapshot_names_every_role() {
        let health = ShardHealth::new();
        health.set(4, ShardRole::Engine, ShardState::Running);
        health.set(4, ShardRole::Md, ShardState::Running);
        health.set(9, ShardRole::Engine, ShardState::Failed);
        let snapshot = health.snapshot(&[4, 9]);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].partition, 4);
        assert_eq!(snapshot[0].engine, "running");
        assert_eq!(snapshot[0].md, "running");
        assert_eq!(snapshot[1].partition, 9);
        assert_eq!(snapshot[1].engine, "failed");
        assert_eq!(snapshot[1].md, "starting");
    }

    #[test]
    fn a_clone_copies_the_current_state() {
        let health = ShardHealth::new();
        health.set(0, ShardRole::Engine, ShardState::Running);
        health.set(0, ShardRole::Md, ShardState::Running);
        let copy = health.clone();
        assert!(copy.all_running(&[0]));
        // Independent: the clone's later failure does not touch the original.
        copy.set(0, ShardRole::Engine, ShardState::Failed);
        assert!(health.all_running(&[0]));
        assert!(!copy.all_running(&[0]));
    }
}
