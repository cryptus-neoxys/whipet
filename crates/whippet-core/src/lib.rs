// Licensed under the Apache License, Version 2.0. See LICENSE.
//! Core types shared by every Whippet service.
//!
//! Everything in this crate is deterministic and allocation-free on the
//! matching path except for the explicit `Vec`-returning encoders. Prices are
//! integer ticks; quantities are integer units. There are no floats in the
//! protocol, the engine, or the state hash.

pub mod api;
pub mod codec;
pub mod config;
pub mod error;
pub mod headers;
pub mod ids;
pub mod manifest;
pub mod protocol;

pub use error::{CoreError, Result};
pub use ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};

/// Wire protocol version for Whippet payloads.
pub const WIRE_VERSION: u8 = 1;

/// Venue identifier used in message headers and the dashboard.
pub const VENUE: &str = "whippet";
