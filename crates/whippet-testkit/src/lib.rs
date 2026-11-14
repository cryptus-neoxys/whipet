// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic test kit for Whippet.
//!
//! The kit is deliberately synchronous: the engine is a pure state machine, so
//! every helper here can drive it directly without Tokio, Iggy, Docker, or a
//! network. The three pieces are:
//!
//! - [`builders`]: small command constructors with demo-safe defaults.
//! - [`golden`]: a fixed command log that produces a stable state hash and
//!   execution/market-data summaries.
//! - [`strategies`]: reusable proptest strategies for valid commands.
//!
//! `NodeContext` lives in the `whippet` binary/library crate, which depends on
//! this crate. Building it here would create a dependency cycle, so the
//! `TestNode` helper lives in `crates/whippet/tests/common/mod.rs` instead. The
//! testkit stays at the bottom of the graph and is reusable by every engine
//! test.

pub mod builders;
pub mod golden;
pub mod strategies;

pub use golden::{
    ExecutionSummary, FinalState, GOLDEN_HASH, GoldenLog, GoldenResult, MdSummary, scenario_config,
    standard_shard,
};
