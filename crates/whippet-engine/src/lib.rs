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

pub use book::{Book, BookError, BookSnapshot, RestingOrder};
pub use dedup::{DedupEntry, DedupStatus, DedupTable};
pub use engine::{
    EngineError, EngineOutput, MAX_ORDER_QTY, ShardEngine, ShardSnapshot, SymbolEngine,
    SymbolSnapshot,
};
