// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{CoreError, Result};

/// A venue symbol, stored as a fixed 16-byte NUL-padded ASCII field.
///
/// The fixed width keeps the hot path allocation-free and makes the wire
/// layout independent of string length. Validation happens at every boundary.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Symbol([u8; 16]);

impl Symbol {
    pub const WIDTH: usize = 16;

    pub fn new(value: &str) -> Result<Self> {
        if value.is_empty() || value.len() > Self::WIDTH {
            return Err(CoreError::InvalidSymbol(value.to_string()));
        }
        let mut bytes = [0u8; Self::WIDTH];
        for (index, byte) in value.bytes().enumerate() {
            if !(0x21..=0x7e).contains(&byte) {
                return Err(CoreError::InvalidSymbol(value.to_string()));
            }
            bytes[index] = byte;
        }
        Ok(Self(bytes))
    }

    /// Reconstruct a symbol from wire bytes. Rejects non-ASCII or malformed padding.
    pub fn from_wire(bytes: [u8; 16]) -> Result<Self> {
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(16);
        if end == 0 {
            return Err(CoreError::InvalidSymbol(String::new()));
        }
        if bytes[end..].iter().any(|&b| b != 0) {
            return Err(CoreError::InvalidSymbol("non-NUL padding".to_string()));
        }
        if bytes[..end].iter().any(|&b| !(0x21..=0x7e).contains(&b)) {
            return Err(CoreError::InvalidSymbol("non-ASCII symbol".to_string()));
        }
        Ok(Self(bytes))
    }

    pub fn as_str(&self) -> &str {
        let end = self.0.iter().position(|&b| b == 0).unwrap_or(16);
        // The constructor and `from_wire` only admit printable ASCII.
        std::str::from_utf8(&self.0[..end]).unwrap_or("")
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Stable partition assignment. Iggy does the same hash server-side when a
    /// messages key is used; Whippet pins the partition explicitly so gateway,
    /// engine, and simulator always agree.
    pub fn partition(&self, partitions: u32) -> u32 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in self.as_str().bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash % u64::from(partitions.max(1))) as u32
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.as_str())
    }
}

impl FromStr for Symbol {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl Serialize for Symbol {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(&value).map_err(serde::de::Error::custom)
    }
}

/// Client order identifier. 128 bits so a UUIDv4 fits without truncation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct OrderId(pub u128);

impl OrderId {
    /// Derive a stable 128-bit order id from a client idempotency key. The
    /// gateway and simulator use the same function, so a cancel can name an
    /// order without a round trip.
    pub fn derive(symbol: Symbol, client_order_id: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-order-id-v1");
        hasher.update(symbol.as_bytes());
        hasher.update(client_order_id.as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest.as_bytes()[..16]);
        Self(u128::from_le_bytes(bytes))
    }

    pub fn to_hex(self) -> String {
        format!("{:032x}", self.0)
    }

    pub fn from_hex(value: &str) -> Result<Self> {
        let trimmed = value.strip_prefix("0x").unwrap_or(value);
        u128::from_str_radix(trimmed, 16)
            .map(Self)
            .map_err(|_| CoreError::InvalidValue(format!("invalid order id: {value}")))
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl fmt::Debug for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrderId({})", self.to_hex())
    }
}

impl Serialize for OrderId {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for OrderId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_hex(&value).map_err(serde::de::Error::custom)
    }
}

/// Participant (client) identifier. The gateway maps authenticated principals
/// onto this stable integer; the engine never sees credentials.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default, Serialize, Deserialize,
)]
pub struct ParticipantId(pub u64);

impl ParticipantId {
    /// Stable mapping from an authenticated participant name to the engine's
    /// integer identity. The gateway performs this at the edge; the matcher
    /// only ever compares integers.
    pub fn derive(name: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-participant-v1");
        hasher.update(name.as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest.as_bytes()[..8]);
        Self(u64::from_le_bytes(bytes))
    }
}

/// A price expressed in integer ticks. The instrument maps ticks to quote
/// currency; the engine never performs floating-point arithmetic.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct Price(pub i64);

impl Price {
    pub const ZERO: Self = Self(0);

    pub fn ticks(self) -> i64 {
        self.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A quantity in integer units. Fractional lots are represented by choosing a
/// finer unit at the instrument level, never by floats.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct Qty(pub u64);

impl Qty {
    pub const ZERO: Self = Self(0);

    pub fn units(self) -> u64 {
        self.0
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic event sequence. One sequence space per symbol.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct Seq(pub u64);

impl Seq {
    pub fn next_seq(&mut self) -> Self {
        let current = *self;
        self.0 = self.0.wrapping_add(1);
        current
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_round_trips_through_wire_bytes() {
        let symbol = Symbol::new("BTC-USD").unwrap();
        assert_eq!(symbol.as_str(), "BTC-USD");
        let wire = *symbol.as_bytes();
        assert_eq!(Symbol::from_wire(wire).unwrap(), symbol);
    }

    #[test]
    fn symbol_rejects_bad_values() {
        assert!(Symbol::new("").is_err());
        assert!(Symbol::new("has space").is_err());
        assert!(Symbol::new("0123456789abcdefg").is_err());
        assert!(Symbol::from_wire([b'A', 0, b'B', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn order_id_hex_round_trip() {
        let id = OrderId(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        assert_eq!(OrderId::from_hex(&id.to_hex()).unwrap(), id);
    }

    #[test]
    fn partition_is_stable_and_pinned() {
        let btc = Symbol::new("BTC-USD").unwrap();
        let eth = Symbol::new("ETH-USD").unwrap();
        let sol = Symbol::new("SOL-USD").unwrap();
        assert_eq!(btc.partition(16), btc.partition(16));
        assert_eq!(btc.partition(16), 1);
        assert_eq!(eth.partition(16), 5);
        assert_eq!(sol.partition(16), 12);
    }

    #[test]
    fn order_and_participant_derivation_is_stable() {
        let symbol = Symbol::new("BTC-USD").unwrap();
        let id = OrderId::derive(symbol, "client-1");
        assert_eq!(id, OrderId::derive(symbol, "client-1"));
        assert_ne!(id, OrderId::derive(symbol, "client-2"));
        assert_ne!(
            id,
            OrderId::derive(Symbol::new("ETH-USD").unwrap(), "client-1")
        );
        assert_eq!(
            ParticipantId::derive("alice"),
            ParticipantId::derive("alice")
        );
        assert_ne!(ParticipantId::derive("alice"), ParticipantId::derive("bob"));
    }
}
