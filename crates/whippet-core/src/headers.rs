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

/// An inline, fixed-capacity header value.
///
/// Every value Whippet writes is bounded: a symbol is 16 bytes, a sequence is
/// at most 20 digits, a 128-bit trace id is 32 hex digits, and the static
/// venue/type/schema/codec strings are shorter than that. Storing the bytes
/// inline removes one heap allocation per header value while keeping the
/// journal's named representation allocation-cheap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderValue {
    bytes: [u8; 40],
    len: u8,
}

impl HeaderValue {
    pub const CAPACITY: usize = 40;

    /// Copy `value` into the inline buffer. Panics only if a future caller
    /// passes a value longer than [`Self::CAPACITY`]; every value built by
    /// [`MessageMeta`] is bounded well below it.
    pub fn new(value: &str) -> Self {
        assert!(
            value.len() <= Self::CAPACITY,
            "header value exceeds {} bytes",
            Self::CAPACITY
        );
        let mut bytes = [0u8; Self::CAPACITY];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Self {
            bytes,
            len: value.len() as u8,
        }
    }

    pub fn as_str(&self) -> &str {
        // Only `from_str` writes the bytes and it accepts `&str`, so the slice
        // is always valid UTF-8.
        std::str::from_utf8(&self.bytes[..usize::from(self.len)]).expect("header value is UTF-8")
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        Self::new(&value)
    }
}

impl std::fmt::Display for HeaderValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The fixed set of header keys Whippet writes.
///
/// Iggy header keys are `String`-typed on the wire, so a producer pays one
/// allocation per key per message when it builds `Vec<(String, String)>`.
/// Whippet only ever writes these seven names, so producers pass this enum
/// instead (see `MessageMeta::named_pairs` and `JournalMessage::new_named`)
/// and the key text is a compile-time constant. `as_str` yields the exact
/// wire name, so the two representations are byte-identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HeaderName {
    Symbol,
    Seq,
    Venue,
    Type,
    Schema,
    TraceId,
    Codec,
}

impl HeaderName {
    /// Every header name, in `HeaderMode::Full` wire order.
    pub const ALL: [HeaderName; 7] = [
        HeaderName::Symbol,
        HeaderName::Seq,
        HeaderName::Venue,
        HeaderName::Type,
        HeaderName::Schema,
        HeaderName::TraceId,
        HeaderName::Codec,
    ];

    /// The exact wire name. Callers rely on this being byte-identical to the
    /// keys produced by [`MessageMeta::pairs_with`].
    pub fn as_str(self) -> &'static str {
        match self {
            HeaderName::Symbol => "symbol",
            HeaderName::Seq => "seq",
            HeaderName::Venue => "venue",
            HeaderName::Type => "type",
            HeaderName::Schema => "schema",
            HeaderName::TraceId => "trace_id",
            HeaderName::Codec => "codec",
        }
    }
}

/// Error for [`std::str::FromStr`] on [`HeaderName`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownHeaderName(pub String);

impl std::fmt::Display for UnknownHeaderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown header name: {}", self.0)
    }
}

impl std::error::Error for UnknownHeaderName {}

impl std::str::FromStr for HeaderName {
    type Err = UnknownHeaderName;

    /// Parse a wire name back into a [`HeaderName`], for the journal's
    /// string-pair fallback and for consumers that still hold `String` keys.
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let known = match name {
            "symbol" => HeaderName::Symbol,
            "seq" => HeaderName::Seq,
            "venue" => HeaderName::Venue,
            "type" => HeaderName::Type,
            "schema" => HeaderName::Schema,
            "trace_id" => HeaderName::TraceId,
            "codec" => HeaderName::Codec,
            _ => return Err(UnknownHeaderName(name.to_string())),
        };
        Ok(known)
    }
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

    /// Build the header pairs for `mode` with constant-time keys.
    ///
    /// Same contract as [`Self::pairs_with`] — `Full` emits the historical
    /// seven pairs in the same order, `Routing` emits `symbol`, `seq`, `type`,
    /// `schema` — but the key is a [`HeaderName`] instead of a fresh `String`,
    /// so building this set allocates only the values. Use it with
    /// `JournalMessage::new_named` to keep keys allocation-free through the
    /// journal.
    pub fn named_pairs(&self, mode: HeaderMode) -> Vec<(HeaderName, HeaderValue)> {
        match mode {
            HeaderMode::Full => {
                // Sized exactly: `Full` has seven pairs, `Routing` four, so
                // neither arm reallocates.
                let mut pairs = Vec::with_capacity(7);
                pairs.extend([
                    (
                        HeaderName::Symbol,
                        HeaderValue::new(&self.symbol.to_string()),
                    ),
                    (HeaderName::Seq, HeaderValue::new(&seq_string(self.seq.0))),
                    (HeaderName::Venue, HeaderValue::new(self.venue)),
                    (HeaderName::Type, HeaderValue::new(self.kind)),
                    (
                        HeaderName::Schema,
                        HeaderValue::new(&self.schema.to_string()),
                    ),
                    (
                        HeaderName::TraceId,
                        HeaderValue::new(&format!("{:032x}", self.trace_id)),
                    ),
                    (HeaderName::Codec, HeaderValue::new(self.codec)),
                ]);
                pairs
            }
            HeaderMode::Routing => {
                let mut pairs = Vec::with_capacity(4);
                pairs.extend([
                    (
                        HeaderName::Symbol,
                        HeaderValue::new(&self.symbol.to_string()),
                    ),
                    (HeaderName::Seq, HeaderValue::new(&seq_string(self.seq.0))),
                    (HeaderName::Type, HeaderValue::new(self.kind)),
                    (
                        HeaderName::Schema,
                        HeaderValue::new(&self.schema.to_string()),
                    ),
                ]);
                pairs
            }
            HeaderMode::None => Vec::new(),
        }
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

    #[test]
    fn header_name_is_copy_and_matches_the_wire_names() {
        use std::str::FromStr;
        // `Copy` is the contract: passing a name must never move-or-clone.
        fn takes_by_value(name: HeaderName) -> HeaderName {
            let copy = name;
            let again = copy;
            assert_eq!(copy, again);
            again
        }
        for name in HeaderName::ALL {
            assert_eq!(takes_by_value(name), name);
            assert_eq!(
                HeaderName::from_str(name.as_str()),
                Ok(name),
                "from_str round-trips the wire name of {name:?}"
            );
        }
        assert!(matches!(
            HeaderName::from_str("custom"),
            Err(UnknownHeaderName(_))
        ));
        assert!(matches!(
            HeaderName::from_str("Symbol"),
            Err(UnknownHeaderName(_))
        ));
    }

    #[test]
    fn named_pairs_match_the_string_pairs_exactly() {
        let meta = order_meta();
        for mode in [HeaderMode::Full, HeaderMode::Routing, HeaderMode::None] {
            let named = meta.named_pairs(mode);
            let string = meta.pairs_with(mode);
            assert_eq!(
                named.len(),
                string.len(),
                "mode {mode:?} keeps the same pair count"
            );
            for ((name, value), (key, key_value)) in named.iter().zip(&string) {
                assert_eq!(name.as_str(), key, "mode {mode:?} keeps the same names");
                assert_eq!(
                    value.as_str(),
                    key_value,
                    "mode {mode:?} keeps the same values"
                );
            }
        }
        assert_eq!(meta.named_pairs(HeaderMode::None), Vec::new());
    }

    #[test]
    fn named_pairs_keep_the_full_order_and_routing_subset() {
        let meta = order_meta();
        let full = meta.named_pairs(HeaderMode::Full);
        assert_eq!(
            full.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            vec![
                HeaderName::Symbol,
                HeaderName::Seq,
                HeaderName::Venue,
                HeaderName::Type,
                HeaderName::Schema,
                HeaderName::TraceId,
                HeaderName::Codec,
            ]
        );
        let routing = meta.named_pairs(HeaderMode::Routing);
        assert_eq!(
            routing.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            vec![
                HeaderName::Symbol,
                HeaderName::Seq,
                HeaderName::Type,
                HeaderName::Schema,
            ]
        );
    }

    #[test]
    fn symbol_reads_back_from_a_named_routing_set() {
        let meta = MessageMeta::new(
            Symbol::new("BTC-USD").unwrap(),
            Seq(42),
            "md_event",
            SCHEMA_MD_EVENT,
            0,
        );
        let named = meta.named_pairs(HeaderMode::Routing);
        let borrowed: Vec<(&str, &str)> = named
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            MessageMeta::symbol_from_pairs(borrowed),
            Some(meta.symbol),
            "the named set reads back like the string set"
        );
    }
}
