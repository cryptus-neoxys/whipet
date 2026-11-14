// Licensed under the Apache License, Version 2.0. See LICENSE.

//! CPU-only microbenchmarks for the Iggy append edge.
//!
//! These measure the work between "payload ready" and "IggyMessage ready to
//! send": header key resolution and frame construction. They need no broker, so
//! they run anywhere `cargo bench` runs. Network, durability, and confirmation
//! latency are deliberately out of scope.
//!
//! ```text
//! cargo bench -p whippet-journal --bench append
//! ```

use std::collections::BTreeMap;
use std::hint::black_box;
use std::str::FromStr;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};
use whippet_core::headers::{MessageMeta, SCHEMA_ORDER};
use whippet_core::ids::{Seq, Symbol};
use whippet_journal::JournalMessage;

/// A 64-byte fixed-layout payload, roughly an encoded order.
const PAYLOAD: &[u8] = &[0u8; 64];

fn headers() -> Vec<(String, String)> {
    MessageMeta::new(
        Symbol::new("BTC-USD").expect("static symbol"),
        Seq(42),
        "order",
        SCHEMA_ORDER,
        0xabc,
    )
    .pairs()
}

/// The pre-cache path: parse every header key and value per message.
fn frame_parsing_keys(headers: &[(String, String)]) -> IggyMessage {
    let mut user_headers = BTreeMap::new();
    for (key, value) in headers {
        user_headers.insert(
            HeaderKey::from_str(key).expect("key"),
            HeaderValue::from_str(value).expect("value"),
        );
    }
    IggyMessage::builder()
        .payload(Bytes::from_static(PAYLOAD))
        .user_headers(user_headers)
        .build()
        .expect("frame")
}

/// The cached path: clone the pre-parsed key, parse only the value.
fn frame_cached_keys(keys: &[HeaderKey], headers: &[(String, String)]) -> IggyMessage {
    let mut user_headers = BTreeMap::new();
    for (key, (_, value)) in keys.iter().zip(headers) {
        user_headers.insert(key.clone(), HeaderValue::from_str(value).expect("value"));
    }
    IggyMessage::builder()
        .payload(Bytes::from_static(PAYLOAD))
        .user_headers(user_headers)
        .build()
        .expect("frame")
}

fn header_key_resolution(c: &mut Criterion) {
    let headers = headers();
    let keys: Vec<HeaderKey> = headers
        .iter()
        .map(|(key, _)| HeaderKey::from_str(key).expect("key"))
        .collect();

    let mut group = c.benchmark_group("append_frame_headers");
    group.bench_function("parse_each_key", |b| {
        b.iter(|| frame_parsing_keys(black_box(&headers)));
    });
    group.bench_function("cached_keys", |b| {
        b.iter(|| frame_cached_keys(black_box(&keys), black_box(&headers)));
    });
    group.finish();
}

fn message_construction(c: &mut Criterion) {
    let headers = headers();
    let static_headers: Vec<(&'static str, String)> = vec![
        ("symbol", "BTC-USD".to_string()),
        ("seq", "42".to_string()),
        ("venue", "whippet".to_string()),
        ("type", "order".to_string()),
        ("schema", SCHEMA_ORDER.to_string()),
        ("trace_id", "00000000000000000000000000000abc".to_string()),
        ("codec", "fixed-le-v1".to_string()),
    ];

    let mut group = c.benchmark_group("journal_message");
    group.bench_function("owned_pairs", |b| {
        b.iter(|| JournalMessage::new(black_box(PAYLOAD.to_vec()), black_box(headers.clone())));
    });
    group.bench_function("static_pairs", |b| {
        b.iter(|| {
            JournalMessage::new_static(
                black_box(PAYLOAD.to_vec()),
                black_box(static_headers.clone()),
            )
        });
    });
    group.finish();
}

criterion_group!(benches, header_key_resolution, message_construction);
criterion_main!(benches);
