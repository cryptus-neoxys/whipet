// Licensed under the Apache License, Version 2.0. See LICENSE.

use crate::ids::{Seq, Symbol};

pub const SCHEMA_ORDER: u16 = 1;
pub const SCHEMA_EXECUTION: u16 = 2;
pub const SCHEMA_MD_EVENT: u16 = 3;
pub const SCHEMA_MD_SNAPSHOT: u16 = 4;
pub const CODEC_FIXED_LE: &str = "fixed-le-v1";

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
        vec![
            ("symbol".to_string(), self.symbol.to_string()),
            ("seq".to_string(), self.seq.0.to_string()),
            ("venue".to_string(), self.venue.to_string()),
            ("type".to_string(), self.kind.to_string()),
            ("schema".to_string(), self.schema.to_string()),
            ("trace_id".to_string(), format!("{:032x}", self.trace_id)),
            ("codec".to_string(), self.codec.to_string()),
        ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_include_the_routing_set() {
        let meta = MessageMeta::new(
            Symbol::new("ETH-USD").unwrap(),
            Seq(7),
            "order",
            SCHEMA_ORDER,
            0xabc,
        );
        let pairs = meta.pairs();
        assert!(pairs.contains(&("symbol".into(), "ETH-USD".into())));
        assert!(pairs.contains(&("seq".into(), "7".into())));
        assert!(pairs.contains(&("trace_id".into(), "00000000000000000000000000000abc".into())));
    }
}
