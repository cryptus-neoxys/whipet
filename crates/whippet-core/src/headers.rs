// Licensed under the Apache License, Version 2.0. See LICENSE.

use crate::ids::{Seq, Symbol};

pub const SCHEMA_ORDER: u16 = 1;
pub const SCHEMA_EXECUTION: u16 = 2;
pub const SCHEMA_MD_EVENT: u16 = 3;
pub const SCHEMA_MD_SNAPSHOT: u16 = 4;
pub const CODEC_FIXED_LE: &str = "fixed-le-v1";

/// Which header keys a message carries.
///
/// Iggy headers are `String -> String` and are therefore not free: each pair
/// costs two `String` allocations plus the wire bytes. The journal accepts any
/// key it is given (see `HeaderKeys` in `whippet-journal`), so the producer
/// chooses how much diagnostic metadata to attach.
///
/// Choose by message kind:
///
/// - [`HeaderMode::Full`] for orders and audit records. These are the messages
///   an operator reconstructs history from, so `venue`, `trace_id` and `codec`
///   must be on the wire.
/// - [`HeaderMode::Routing`] for executions and market-data events. Consumers
///   decode the fixed-layout payload and only need the routing/diagnostic keys
///   (`symbol`, `seq`, `type`, `schema`).
/// - [`HeaderMode::None`] only for a private/trusted transport that never
///   surfaces messages to an operator. Do not use it where an operator relies
///   on headers to identify a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderMode {
    /// All seven keys: `symbol`, `seq`, `venue`, `type`, `schema`, `trace_id`,
    /// `codec`.
    Full,
    /// The hot-path subset: `symbol`, `seq`, `type`, `schema`.
    Routing,
    /// No headers at all.
    None,
}

/// Header set attached to every Iggy message.
///
/// Iggy headers are `String -> String` on the wire. Whippet keeps the payload
/// fixed-layout and puts only routing/diagnostic metadata in headers so the
/// hot decoder never parses strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMeta {
    pub symbol: Symbol,
    pub seq: Seq,
    pub kind: &'static str,
    pub schema: u16,
    pub trace_id: u128,
    pub codec: &'static str,
    pub venue: &'static str,
}

impl MessageMeta {
    pub fn new(symbol: Symbol, seq: Seq, kind: &'static str, schema: u16, trace_id: u128) -> Self {
        Self {
            symbol,
            seq,
            kind,
            schema,
            trace_id,
            codec: CODEC_FIXED_LE,
            venue: crate::VENUE,
        }
    }

    pub fn pairs(&self) -> Vec<(String, String)> {
        self.pairs_with(HeaderMode::Full)
    }

    /// Build the header pairs for `mode`.
    ///
    /// `Full` keeps the historical seven-pair contract, in the same order:
    /// `symbol`, `seq`, `venue`, `type`, `schema`, `trace_id`, `codec`.
    /// `Routing` emits only `symbol`, `seq`, `type`, `schema`.
    pub fn pairs_with(&self, mode: HeaderMode) -> Vec<(String, String)> {
        match mode {
            HeaderMode::Full => {
                // Sized exactly: `Full` has seven pairs, `Routing` four, so
                // neither arm reallocates.
                let mut pairs = Vec::with_capacity(7);
                pairs.extend([
                    ("symbol".to_string(), self.symbol.to_string()),
                    ("seq".to_string(), seq_string(self.seq.0)),
                    ("venue".to_string(), self.venue.to_string()),
                    ("type".to_string(), self.kind.to_string()),
                    ("schema".to_string(), self.schema.to_string()),
                    ("trace_id".to_string(), format!("{:032x}", self.trace_id)),
                    ("codec".to_string(), self.codec.to_string()),
                ]);
                pairs
            }
            HeaderMode::Routing => {
                let mut pairs = Vec::with_capacity(4);
                pairs.extend([
                    ("symbol".to_string(), self.symbol.to_string()),
                    ("seq".to_string(), seq_string(self.seq.0)),
                    ("type".to_string(), self.kind.to_string()),
                    ("schema".to_string(), self.schema.to_string()),
                ]);
                pairs
            }
            HeaderMode::None => Vec::new(),
        }
    }

    /// Shorthand for [`Self::pairs_with`] with [`HeaderMode::Routing`].
    pub fn routing_pairs(&self) -> Vec<(String, String)> {
        self.pairs_with(HeaderMode::Routing)
    }

    /// Read the symbol header back from an Iggy message, if present.
    pub fn symbol_from_pairs<'a, I>(pairs: I) -> Option<Symbol>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        pairs
            .into_iter()
            .find(|(key, _)| *key == "symbol")
            .and_then(|(_, value)| Symbol::new(value).ok())
    }
}

/// Render a sequence number as decimal without going through `core::fmt`.
///
/// The digits are written right-aligned into a stack buffer and then copied
/// into an exactly-sized `String`, so the hot path pays one allocation and no
/// formatting machinery. `#![forbid(unsafe_code)]` rules out the usual
/// `unsafe { str::from_utf8_unchecked(..) }`; `from_utf8_lossy` on ASCII digits
/// borrows and `into_owned` copies.
fn seq_string(value: u64) -> String {
    let mut buf = [0u8; 20];
    let mut cursor = buf.len();
    let mut remaining = value;
    loop {
        cursor -= 1;
        buf[cursor] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    String::from_utf8_lossy(&buf[cursor..]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_meta() -> MessageMeta {
        MessageMeta::new(
            Symbol::new("ETH-USD").unwrap(),
            Seq(7),
            "order",
            SCHEMA_ORDER,
            0xabc,
        )
    }

    #[test]
    fn headers_include_the_routing_set() {
        let meta = order_meta();
        let pairs = meta.pairs();
        assert!(pairs.contains(&("symbol".into(), "ETH-USD".into())));
        assert!(pairs.contains(&("seq".into(), "7".into())));
        assert!(pairs.contains(&("trace_id".into(), "00000000000000000000000000000abc".into())));
    }

    #[test]
    fn full_mode_keeps_the_seven_pair_contract() {
        let meta = order_meta();
        assert_eq!(
            meta.pairs_with(HeaderMode::Full),
            vec![
                ("symbol".to_string(), "ETH-USD".to_string()),
                ("seq".to_string(), "7".to_string()),
                ("venue".to_string(), crate::VENUE.to_string()),
                ("type".to_string(), "order".to_string()),
                ("schema".to_string(), SCHEMA_ORDER.to_string()),
                (
                    "trace_id".to_string(),
                    "00000000000000000000000000000abc".to_string()
                ),
                ("codec".to_string(), CODEC_FIXED_LE.to_string()),
            ]
        );
        // `pairs()` is the same contract, so existing callers cannot drift.
        assert_eq!(meta.pairs(), meta.pairs_with(HeaderMode::Full));
    }

    #[test]
    fn routing_mode_drops_venue_trace_id_and_codec() {
        let meta = order_meta();
        let pairs = meta.pairs_with(HeaderMode::Routing);
        assert_eq!(pairs, meta.routing_pairs());
        assert_eq!(
            pairs,
            vec![
                ("symbol".to_string(), "ETH-USD".to_string()),
                ("seq".to_string(), "7".to_string()),
                ("type".to_string(), "order".to_string()),
                ("schema".to_string(), SCHEMA_ORDER.to_string()),
            ]
        );
        for absent in ["venue", "trace_id", "codec"] {
            assert!(
                !pairs.iter().any(|(key, _)| key == absent),
                "{absent} must not appear in a routing header set"
            );
        }
    }

    #[test]
    fn none_mode_emits_no_headers() {
        let meta = order_meta();
        assert!(meta.pairs_with(HeaderMode::None).is_empty());
    }

    #[test]
    fn symbol_reads_back_from_a_routing_set() {
        let meta = MessageMeta::new(
            Symbol::new("BTC-USD").unwrap(),
            Seq(42),
            "md_event",
            SCHEMA_MD_EVENT,
            0,
        );
        let pairs = meta.routing_pairs();
        let borrowed: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            MessageMeta::symbol_from_pairs(borrowed),
            Some(Symbol::new("BTC-USD").unwrap())
        );
    }

    #[test]
    fn seq_string_matches_display_for_boundaries() {
        for value in [0u64, 1, 7, 9, 10, 99, 100, 1_234_567_890, u64::MAX] {
            assert_eq!(seq_string(value), value.to_string(), "seq {value}");
        }
    }
}
