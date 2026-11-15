// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Whippet's deterministic matching engine.
//!
//! One `ShardEngine` owns every book hashed to a partition. One OS thread per
//! shard is the single writer; the engine itself is synchronous and has no
//! dependency on Tokio or Iggy.

#![forbid(unsafe_code)]

pub mod book;
pub mod dedup;
pub mod engine;
pub mod risk;

pub use book::{Book, BookError, BookSnapshot, RestingOrder, hash_book_snapshot};
pub use dedup::{DedupEntry, DedupStatus, DedupTable, hash_dedup_entries};
pub use engine::{
    EngineError, EngineOutput, MAX_ORDER_QTY, ShardEngine, ShardSnapshot, SymbolConfigSnapshot,
    SymbolEngine, SymbolSnapshot, fill_symbol_hashes, hash_symbol_snapshot,
};
pub use risk::{RiskEntry, RiskExposure, RiskLimits, RiskTable, RiskViolation};
