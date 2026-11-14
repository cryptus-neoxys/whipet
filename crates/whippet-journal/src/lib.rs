// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The Iggy edge.
//!
//! Whippet treats Iggy as a partitioned log, not a queue. This crate owns the
//! only async Iggy client code in the system: topic bootstrap, appends, polls,
//! and manual consumer-offset commits. The matcher never sees an `await`.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, CommitPolicy, Consumer, ConsumerOffsetClient, Credentials, HeaderKey,
    HeaderValue, Identifier, IggyByteSize, IggyClient, IggyDuration, IggyError, IggyExpiry,
    IggyMessage, MaxTopicSize, MessageClient, PendingOperationsConfig, PollOptions, PolledMessages,
    PollingStrategy, StreamClient, StreamCreateOptions, SystemClient, TopicClient,
    TopicCreateOptions,
};
use serde::{Deserialize, Serialize};
use whippet_core::config::ExchangeConfig;

pub mod topic;

pub use topic::Topic;

pub const STREAM_TRADING: &str = "whippet-trading";
pub const STREAM_MD: &str = "whippet-md";
pub const STREAM_OPS: &str = "whippet-ops";

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("iggy error: {0}")]
    Iggy(#[from] IggyError),
    #[error("journal is not connected")]
    NotConnected,
    #[error("append returned no confirmation")]
    NoConfirmation,
    #[error("invalid journal configuration: {0}")]
    InvalidConfig(String),
}

impl JournalError {
    /// The retained Iggy operation that blocks a lane, when the SDK reports one.
    ///
    /// `LaneBlocked`, `PartitionLaneBlocked`, `OperationUncertain`, and
    /// `OperationNotRetained` all carry the operation id needed to settle the
    /// lane with `resolve_operation`/`abandon_operation`.
    pub fn operation_id(&self) -> Option<u64> {
        match self {
            Self::Iggy(error) => error.operation_id(),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, JournalError>;

#[derive(Debug, Clone)]
pub struct JournalMessage {
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl JournalMessage {
    pub fn new(payload: Vec<u8>, headers: Vec<(String, String)>) -> Self {
        Self { payload, headers }
    }

    /// Build a message whose header keys are compile-time constants.
    ///
    /// The pairs are still stored as `(String, String)` because
    /// [`JournalMessage`] carries the wire-format header set; this only spares
    /// call sites the `.to_string()` noise. The per-message key parsing cost is
    /// removed on the Iggy side by the connection's header-key cache, so a
    /// separate `&'static str` representation would not save anything here.
    pub fn new_static(payload: Vec<u8>, headers: Vec<(&'static str, String)>) -> Self {
        Self {
            payload,
            headers: headers
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JournalRecord {
    pub offset: u64,
    pub timestamp_ns: u64,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl JournalRecord {
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
}

/// A record read without touching its user headers.
///
/// Consumers that only decode payloads use this to skip the header TLV parse,
/// the `HeaderKey`/`HeaderValue` allocations, and the `String` copies. The
/// payload is `Bytes` so the Iggy path hands over the frame's buffer (a
/// refcount bump) instead of copying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadRecord {
    pub offset: u64,
    pub timestamp_ns: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub partition: u32,
    /// Base offset of the first chunk. Chunks are contiguous only within a
    /// chunk; see [`IggyJournal::append`].
    pub base_offset: u64,
    /// Total messages accepted across all chunks.
    pub count: u32,
}

/// Maximum number of non-routing header names remembered per connection.
const HEADER_KEY_CACHE_LIMIT: usize = 32;

/// Parsed `HeaderKey`s for the routing header set.
///
/// Iggy's `HeaderKey` owns a `Bytes` copy of the key text, so
/// `HeaderKey::from_str` allocates on every call. The routing set is fixed, so
/// each key is parsed once per connection and cloned per message; cloning a
/// `HeaderKey` is a refcount bump, not an allocation.
struct HeaderKeys {
    symbol: HeaderKey,
    seq: HeaderKey,
    venue: HeaderKey,
    /// The `type` header (named `kind` because `type` is a keyword).
    kind: HeaderKey,
    schema: HeaderKey,
    trace_id: HeaderKey,
    codec: HeaderKey,
    /// Header names outside the routing set, parsed once and reused. Bounded so
    /// a producer cannot grow the cache without limit.
    extra: Mutex<HashMap<String, HeaderKey>>,
}

impl HeaderKeys {
    fn new() -> Result<Self> {
        Ok(Self {
            symbol: parse_header_key("symbol")?,
            seq: parse_header_key("seq")?,
            venue: parse_header_key("venue")?,
            kind: parse_header_key("type")?,
            schema: parse_header_key("schema")?,
            trace_id: parse_header_key("trace_id")?,
            codec: parse_header_key("codec")?,
            extra: Mutex::new(HashMap::new()),
        })
    }

    /// Resolve a header name to its parsed key. Routing names compare by `&str`
    /// so the hot path never hashes.
    fn get(&self, name: &str) -> Result<HeaderKey> {
        let known = match name {
            "symbol" => &self.symbol,
            "seq" => &self.seq,
            "venue" => &self.venue,
            "type" => &self.kind,
            "schema" => &self.schema,
            "trace_id" => &self.trace_id,
            "codec" => &self.codec,
            _ => return self.unknown(name),
        };
        Ok(known.clone())
    }

    fn unknown(&self, name: &str) -> Result<HeaderKey> {
        let mut extra = self
            .extra
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(key) = extra.get(name) {
            return Ok(key.clone());
        }
        let key = parse_header_key(name)?;
        if extra.len() < HEADER_KEY_CACHE_LIMIT {
            extra.insert(name.to_string(), key.clone());
        }
        Ok(key)
    }
}

fn parse_header_key(name: &str) -> Result<HeaderKey> {
    Ok(HeaderKey::from_str(name)?)
}

/// Build an Iggy frame from a payload and string header pairs.
///
/// Header keys come from the connection's [`HeaderKeys`] cache, so a known key
/// costs a refcount bump instead of a fresh `HeaderKey` allocation. Only the
/// header values are parsed per message.
fn build_iggy_message(
    keys: &HeaderKeys,
    payload: Bytes,
    headers: &[(String, String)],
) -> Result<IggyMessage> {
    let mut user_headers = BTreeMap::new();
    for (key, value) in headers {
        user_headers.insert(keys.get(key)?, HeaderValue::from_str(value)?);
    }
    Ok(IggyMessage::builder()
        .payload(payload)
        .user_headers(user_headers)
        .build()?)
}

/// The largest batch Whippet hands to a single `send_messages` call.
///
/// Iggy's `message_bus.max_batch` defaults to 256; a batch of 256 input orders
/// can fan out to more execution and MD records than that, so appends are
/// chunked here rather than relying on the broker default.
const MAX_APPEND_BATCH: usize = 256;

fn append_chunks<T>(messages: &mut [T]) -> impl Iterator<Item = &mut [T]> {
    messages.chunks_mut(MAX_APPEND_BATCH)
}

/// Cached stream/topic identifiers.
///
/// `Identifier::named` copies the name into a fresh `Vec<u8>`. The topic layout
/// is fixed at six entries, so the identifiers are built once per connection
/// and borrowed per operation.
struct TopicIds {
    by_topic: HashMap<Topic, (Identifier, Identifier)>,
}

impl TopicIds {
    fn new() -> Result<Self> {
        let mut by_topic = HashMap::with_capacity(Topic::ALL.len());
        for topic in Topic::ALL {
            by_topic.insert(
                topic,
                (
                    Identifier::named(topic.stream())?,
                    Identifier::named(topic.name())?,
                ),
            );
        }
        Ok(Self { by_topic })
    }

    fn get(&self, topic: Topic) -> Result<(&Identifier, &Identifier)> {
        let (stream, topic_id) = self.by_topic.get(&topic).ok_or_else(|| {
            JournalError::InvalidConfig(format!("topic {topic} is not in the journal layout"))
        })?;
        Ok((stream, topic_id))
    }
}

/// The Iggy-backed journal.
pub struct IggyJournal {
    client: Arc<IggyClient>,
    address: String,
    // Boxed so the `Journal` enum stays small; the caches are built once and
    // only read afterwards.
    header_keys: Box<HeaderKeys>,
    ids: Box<TopicIds>,
}

impl IggyJournal {
    /// Connect with a username/password. The local demo uses the bootstrap
    /// account; production-style PATs and scoped users are Phase 2.
    pub async fn connect(address: &str, username: &str, password: &str) -> Result<Self> {
        let client = IggyClient::builder()
            .with_tcp()
            .with_server_address(address.to_string())
            // Demo headroom: Iggy's default pool (256 active/retained mutations,
            // 64 MiB of frozen frames) can fill under batch or concurrent
            // gateway load. This only sizes the client-side frame pool; it is
            // not a durability or offset-semantics change.
            .with_pending_operations(PendingOperationsConfig {
                max_operations: NonZeroUsize::new(4096).expect("4096 is non-zero"),
                max_bytes: NonZeroUsize::new(256 * 1024 * 1024).expect("256 MiB is non-zero"),
            })
            .with_no_delay()
            .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string().into(),
            )))
            .build()?;
        client.connect().await?;
        Ok(Self {
            client: Arc::new(client),
            address: address.to_string(),
            header_keys: Box::new(HeaderKeys::new()?),
            ids: Box::new(TopicIds::new()?),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn ping(&self) -> bool {
        self.client.ping().await.is_ok()
    }

    /// Settle a retained Iggy operation whose outcome is unknown.
    ///
    /// When the broker restarts under load, a mutation can be left with an
    /// unknown outcome and its lane stays blocked: retrying the operation
    /// returns `LaneBlocked(op)` forever. The SDK requires either
    /// `resolve_operation` (replay the frame and prove the outcome) or
    /// `abandon_operation` (free the lane and accept the unknown outcome).
    /// Returns whether the operation was settled; it never panics.
    pub async fn recover_operation(&self, operation_id: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        match self.client.resolve_operation(operation_id, deadline).await {
            Ok(()) => {
                tracing::info!(operation_id, "resolved retained Iggy operation");
                true
            }
            Err(error) => {
                tracing::warn!(
                    operation_id,
                    error = %error,
                    "could not resolve retained Iggy operation; abandoning it"
                );
                match self.client.abandon_operation(operation_id).await {
                    Ok(()) => {
                        tracing::info!(operation_id, "abandoned retained Iggy operation");
                        true
                    }
                    Err(error) => {
                        tracing::warn!(
                            operation_id,
                            error = %error,
                            "could not abandon retained Iggy operation"
                        );
                        false
                    }
                }
            }
        }
    }

    /// Run one Iggy operation, recovering a blocked lane and retrying once.
    ///
    /// The caller awaits the first attempt and passes its result here. The
    /// retry is a single extra attempt, never recursive: an operation that
    /// fails twice returns the second error to the caller.
    async fn retry_after<T, F, Fut>(&self, first: Result<T>, retry: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>> + Send,
    {
        match first {
            Ok(value) => Ok(value),
            Err(error) => match self.recover_lane(&error).await {
                Some(_) => retry().await,
                None => Err(error),
            },
        }
    }

    /// Settle the retained operation behind `error`, if it carries one.
    async fn recover_lane(&self, error: &JournalError) -> Option<u64> {
        let operation_id = error.operation_id()?;
        tracing::warn!(
            operation_id,
            error = %error,
            "Iggy lane blocked by an operation with an unknown outcome; recovering and retrying once"
        );
        self.recover_operation(operation_id).await;
        Some(operation_id)
    }

    /// Create streams and topics if absent. Existing topics are left alone.
    pub async fn ensure_topics(&self, exchange: &ExchangeConfig) -> Result<()> {
        for stream in [STREAM_TRADING, STREAM_MD, STREAM_OPS] {
            let stream_id = Identifier::named(stream)?;
            if self.client.get_stream(&stream_id).await?.is_none() {
                self.client
                    .create_stream(stream, &StreamCreateOptions::default())
                    .await?;
                tracing::info!(stream, "created Iggy stream");
            }
        }

        for topic in Topic::ALL {
            let (stream_id, topic_id) = self.ids.get(topic)?;
            match self.client.get_topic(stream_id, topic_id).await? {
                Some(existing) => {
                    let expected = topic.partition_count(exchange);
                    if existing.partitions_count != expected {
                        return Err(JournalError::InvalidConfig(format!(
                            "topic {}/{} has {} partitions but this configuration expects {}; \
                             reset the Iggy data volume or use the original partition count",
                            topic.stream(),
                            topic.name(),
                            existing.partitions_count,
                            expected
                        )));
                    }
                }
                None => {
                    let options = topic_create_options(topic, exchange);
                    self.client
                        .create_topic(stream_id, topic.name(), &options)
                        .await?;
                    tracing::info!(
                        stream = topic.stream(),
                        topic = topic.name(),
                        partitions = topic.partition_count(exchange),
                        durability = ?topic.durability(),
                        "created Iggy topic"
                    );
                }
            }
        }
        Ok(())
    }

    /// Append a borrowed batch. Payloads are copied into the Iggy frames; use
    /// [`IggyJournal::append_owned`] when the caller can give up the messages.
    ///
    /// Batches larger than 256 messages (Iggy's default `message_bus.max_batch`)
    /// are split into contiguous `send_messages` calls. [`AppendResult::base_offset`]
    /// is the first chunk's base offset and [`AppendResult::count`] is the total
    /// number of messages; offsets are contiguous only within a chunk, so callers
    /// that need a per-record identity should use the per-message offsets or
    /// sequence numbers. If a later chunk fails this returns an error: the retry
    /// is at-least-once, and the engine/downstream dedup absorbs duplicates.
    pub async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> Result<AppendResult> {
        if messages.is_empty() {
            return Ok(empty_append(partition));
        }
        let mut iggy_messages = Vec::with_capacity(messages.len());
        for message in messages {
            iggy_messages.push(
                self.iggy_message(Bytes::copy_from_slice(&message.payload), &message.headers)?,
            );
        }
        let count = messages.len() as u32;
        let first = self
            .dispatch(topic, partition, &mut iggy_messages, count)
            .await;
        self.retry_after(first, || {
            self.dispatch(topic, partition, &mut iggy_messages, count)
        })
        .await
    }

    /// Append a batch the caller no longer needs.
    ///
    /// The payload buffers move into the Iggy frames instead of being copied,
    /// which removes one allocation and one memcpy per message. Chunking and
    /// error semantics match [`IggyJournal::append`].
    pub async fn append_owned(
        &self,
        topic: Topic,
        partition: u32,
        messages: Vec<JournalMessage>,
    ) -> Result<AppendResult> {
        if messages.is_empty() {
            return Ok(empty_append(partition));
        }
        let count = messages.len() as u32;
        let mut iggy_messages = Vec::with_capacity(messages.len());
        for message in messages {
            let JournalMessage { payload, headers } = message;
            iggy_messages.push(self.iggy_message(Bytes::from(payload), &headers)?);
        }
        let first = self
            .dispatch(topic, partition, &mut iggy_messages, count)
            .await;
        self.retry_after(first, || {
            self.dispatch(topic, partition, &mut iggy_messages, count)
        })
        .await
    }

    /// Map one journal message onto an Iggy frame, reusing the cached header
    /// keys instead of parsing a `HeaderKey` per message.
    fn iggy_message(&self, payload: Bytes, headers: &[(String, String)]) -> Result<IggyMessage> {
        build_iggy_message(&self.header_keys, payload, headers)
    }

    async fn dispatch(
        &self,
        topic: Topic,
        partition: u32,
        iggy_messages: &mut [IggyMessage],
        count: u32,
    ) -> Result<AppendResult> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let mut result: Option<AppendResult> = None;
        for chunk in append_chunks(iggy_messages) {
            let response = self
                .client
                .send_messages(
                    stream_id,
                    topic_id,
                    &iggy::prelude::Partitioning::partition_id(partition),
                    chunk,
                )
                .await?;
            let confirmation = response
                .confirmations
                .first()
                .ok_or(JournalError::NoConfirmation)?;
            result.get_or_insert(AppendResult {
                partition: confirmation.partition_id,
                base_offset: confirmation.base_offset,
                count,
            });
        }
        // `count > 0` guarantees at least one chunk, so the result is present.
        result.ok_or(JournalError::NoConfirmation)
    }

    pub async fn read(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
    ) -> Result<Vec<JournalRecord>> {
        let first = self
            .poll(topic, partition, consumer, offset, count, max_wait_ms)
            .await;
        let polled = self
            .retry_after(first, || {
                self.poll(topic, partition, consumer, offset, count, max_wait_ms)
            })
            .await?;
        let mut records = Vec::with_capacity(polled.messages.len());
        for message in polled.messages {
            let headers = match message.user_headers_map() {
                Ok(Some(map)) => map
                    .into_iter()
                    .map(|(key, value)| (key.to_string_value(), value.to_string_value()))
                    .collect(),
                Ok(None) => Vec::new(),
                Err(error) => {
                    tracing::warn!(
                        offset = message.header.offset,
                        error = %error,
                        "ignoring malformed Iggy user headers; the payload is still decodable"
                    );
                    Vec::new()
                }
            };
            records.push(JournalRecord {
                offset: message.header.offset,
                timestamp_ns: message.header.timestamp,
                payload: message.payload.to_vec(),
                headers,
            });
        }
        Ok(records)
    }

    /// Poll records without parsing their user headers.
    ///
    /// Consumers that only decode payloads should use this: it skips the header
    /// TLV decode, the `HeaderKey`/`HeaderValue` allocations, and the `String`
    /// copies that [`IggyJournal::read`] performs.
    pub async fn read_payloads(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
    ) -> Result<Vec<PayloadRecord>> {
        let first = self
            .poll(topic, partition, consumer, offset, count, max_wait_ms)
            .await;
        let polled = self
            .retry_after(first, || {
                self.poll(topic, partition, consumer, offset, count, max_wait_ms)
            })
            .await?;
        let mut records = Vec::with_capacity(polled.messages.len());
        for message in polled.messages {
            records.push(PayloadRecord {
                offset: message.header.offset,
                timestamp_ns: message.header.timestamp,
                payload: message.payload,
            });
        }
        Ok(records)
    }

    async fn poll(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
    ) -> Result<PolledMessages> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        let options = PollOptions {
            count,
            max_wait: IggyDuration::from(Duration::from_millis(max_wait_ms)),
            min_count: 1,
            max_bytes: 32 * 1024 * 1024,
            commit: CommitPolicy::Manual,
        };
        Ok(self
            .client
            .poll_messages(
                stream_id,
                topic_id,
                partition,
                &consumer,
                &PollingStrategy::offset(offset),
                options,
            )
            .await?)
    }

    pub async fn commit(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> Result<()> {
        let first = self.store_offset(topic, partition, consumer, offset).await;
        self.retry_after(first, || {
            self.store_offset(topic, partition, consumer, offset)
        })
        .await
    }

    async fn store_offset(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> Result<()> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        self.client
            .store_consumer_offset(&consumer, stream_id, topic_id, Some(partition), offset)
            .await?;
        Ok(())
    }

    pub async fn committed(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> Result<Option<u64>> {
        let first = self.load_offset(topic, partition, consumer).await;
        self.retry_after(first, || self.load_offset(topic, partition, consumer))
            .await
    }

    async fn load_offset(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> Result<Option<u64>> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        let info = self
            .client
            .get_consumer_offset(&consumer, stream_id, topic_id, Some(partition))
            .await?;
        Ok(info.map(|info| info.stored_offset))
    }

    /// Records between the committed consumer offset and the partition head.
    /// `committed` is the last processed offset (inclusive).
    pub async fn lag(&self, topic: Topic, partition: u32, committed: u64) -> Result<u64> {
        let first = self.lag_inner(topic, partition, committed).await;
        self.retry_after(first, || self.lag_inner(topic, partition, committed))
            .await
    }

    async fn lag_inner(&self, topic: Topic, partition: u32, committed: u64) -> Result<u64> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let Some(details) = self.client.get_topic(stream_id, topic_id).await? else {
            return Ok(0);
        };
        let Some(details) = details.partitions.iter().find(|item| item.id == partition) else {
            return Ok(0);
        };
        Ok(details.current_offset.saturating_sub(committed))
    }
}

fn topic_create_options(topic: Topic, exchange: &ExchangeConfig) -> TopicCreateOptions {
    let partitions = topic.partition_count(exchange);
    let max_size = match topic {
        Topic::Orders | Topic::Executions | Topic::Md => 8 * 1024 * 1024 * 1024,
        _ => 2 * 1024 * 1024 * 1024,
    };
    TopicCreateOptions {
        partitions_count: Some(partitions),
        message_expiry: Some(IggyExpiry::ExpireDuration(IggyDuration::from(
            Duration::from_secs(topic.retention_secs()),
        ))),
        max_topic_size: Some(MaxTopicSize::Custom(IggyByteSize::from(max_size))),
        segment_size: Some(IggyByteSize::from(64 * 1024 * 1024)),
        durability: topic.durability(),
        consumer_offset_durability: topic.consumer_offset_durability(),
        messages_required_to_save: Some(1024),
        size_of_messages_required_to_save: Some(IggyByteSize::from(1024 * 1024)),
        ..TopicCreateOptions::default()
    }
}

fn empty_append(partition: u32) -> AppendResult {
    AppendResult {
        partition,
        base_offset: 0,
        count: 0,
    }
}

/// A process-local log with the same semantics as the Iggy adapter.
///
/// This exists for unit tests, `cargo test` in CI without a broker, and an
/// explicit `WHIPPET_JOURNAL=memory` developer escape hatch. The demo and the
/// published benchmarks always use `Journal::Iggy`.
pub struct MemoryJournal {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    logs: BTreeMap<(Topic, u32), Vec<MemoryRecord>>,
    committed: BTreeMap<(Topic, u32, String), u64>,
}

#[derive(Clone)]
struct MemoryRecord {
    offset: u64,
    timestamp_ns: u64,
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl Default for MemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryJournal {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MemoryState::default()),
        }
    }

    pub fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> AppendResult {
        let mut state = self.state.lock().expect("memory journal poisoned");
        let log = state.logs.entry((topic, partition)).or_default();
        let base_offset = log.len() as u64;
        log.extend(
            messages
                .iter()
                .enumerate()
                .map(|(index, message)| MemoryRecord {
                    offset: base_offset + index as u64,
                    timestamp_ns: now_ns(),
                    payload: message.payload.clone(),
                    headers: message.headers.clone(),
                }),
        );
        AppendResult {
            partition,
            base_offset,
            count: messages.len() as u32,
        }
    }

    /// Consume the messages instead of cloning their payloads.
    pub fn append_owned(
        &self,
        topic: Topic,
        partition: u32,
        messages: Vec<JournalMessage>,
    ) -> AppendResult {
        let mut state = self.state.lock().expect("memory journal poisoned");
        let log = state.logs.entry((topic, partition)).or_default();
        let base_offset = log.len() as u64;
        let count = messages.len() as u32;
        log.reserve(messages.len());
        for (index, message) in messages.into_iter().enumerate() {
            log.push(MemoryRecord {
                offset: base_offset + index as u64,
                timestamp_ns: now_ns(),
                payload: message.payload,
                headers: message.headers,
            });
        }
        AppendResult {
            partition,
            base_offset,
            count,
        }
    }

    pub fn read(
        &self,
        topic: Topic,
        partition: u32,
        offset: u64,
        count: u32,
    ) -> Vec<JournalRecord> {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .logs
            .get(&(topic, partition))
            .map(|log| {
                log.iter()
                    .filter(|record| record.offset >= offset)
                    .take(count as usize)
                    .map(|record| JournalRecord {
                        offset: record.offset,
                        timestamp_ns: record.timestamp_ns,
                        payload: record.payload.clone(),
                        headers: record.headers.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn read_payloads(
        &self,
        topic: Topic,
        partition: u32,
        offset: u64,
        count: u32,
    ) -> Vec<PayloadRecord> {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .logs
            .get(&(topic, partition))
            .map(|log| {
                log.iter()
                    .filter(|record| record.offset >= offset)
                    .take(count as usize)
                    .map(|record| PayloadRecord {
                        offset: record.offset,
                        timestamp_ns: record.timestamp_ns,
                        payload: Bytes::from(record.payload.clone()),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn commit(&self, topic: Topic, partition: u32, consumer: &str, offset: u64) {
        let mut state = self.state.lock().expect("memory journal poisoned");
        state
            .committed
            .insert((topic, partition, consumer.to_string()), offset);
    }

    pub fn committed(&self, topic: Topic, partition: u32, consumer: &str) -> Option<u64> {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .committed
            .get(&(topic, partition, consumer.to_string()))
            .copied()
    }

    pub fn high_watermark(&self, topic: Topic, partition: u32) -> u64 {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .logs
            .get(&(topic, partition))
            .map_or(0, |log| log.len() as u64)
    }
}

/// A journal handle. The node uses `Iggy`; tests use `Memory`.
pub enum Journal {
    Iggy(IggyJournal),
    Memory(MemoryJournal),
}

impl Journal {
    pub fn memory() -> Self {
        Self::Memory(MemoryJournal::new())
    }

    pub async fn ensure_topics(&self, exchange: &ExchangeConfig) -> Result<()> {
        match self {
            Self::Iggy(journal) => journal.ensure_topics(exchange).await,
            Self::Memory(_) => Ok(()),
        }
    }

    pub async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> Result<AppendResult> {
        match self {
            Self::Iggy(journal) => journal.append(topic, partition, messages).await,
            Self::Memory(journal) => Ok(journal.append(topic, partition, messages)),
        }
    }

    /// Append a batch the caller no longer needs, moving payloads into the
    /// journal instead of copying them.
    pub async fn append_owned(
        &self,
        topic: Topic,
        partition: u32,
        messages: Vec<JournalMessage>,
    ) -> Result<AppendResult> {
        match self {
            Self::Iggy(journal) => journal.append_owned(topic, partition, messages).await,
            Self::Memory(journal) => Ok(journal.append_owned(topic, partition, messages)),
        }
    }

    pub async fn read(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
    ) -> Result<Vec<JournalRecord>> {
        match self {
            Self::Iggy(journal) => {
                journal
                    .read(topic, partition, consumer, offset, count, max_wait_ms)
                    .await
            }
            Self::Memory(journal) => Ok(journal.read(topic, partition, offset, count)),
        }
    }

    /// Read records without parsing user headers.
    pub async fn read_payloads(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
    ) -> Result<Vec<PayloadRecord>> {
        match self {
            Self::Iggy(journal) => {
                journal
                    .read_payloads(topic, partition, consumer, offset, count, max_wait_ms)
                    .await
            }
            Self::Memory(journal) => Ok(journal.read_payloads(topic, partition, offset, count)),
        }
    }

    pub async fn commit(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> Result<()> {
        match self {
            Self::Iggy(journal) => journal.commit(topic, partition, consumer, offset).await,
            Self::Memory(journal) => {
                journal.commit(topic, partition, consumer, offset);
                Ok(())
            }
        }
    }

    pub async fn committed(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> Result<Option<u64>> {
        match self {
            Self::Iggy(journal) => journal.committed(topic, partition, consumer).await,
            Self::Memory(journal) => Ok(journal.committed(topic, partition, consumer)),
        }
    }

    pub async fn ping(&self) -> bool {
        match self {
            Self::Iggy(journal) => journal.ping().await,
            Self::Memory(_) => true,
        }
    }

    /// Settle a retained Iggy operation whose outcome is unknown.
    ///
    /// The memory journal has no lanes, so it always returns `false`.
    pub async fn recover_operation(&self, operation_id: u64) -> bool {
        match self {
            Self::Iggy(journal) => journal.recover_operation(operation_id).await,
            Self::Memory(_) => false,
        }
    }

    pub async fn lag(&self, topic: Topic, partition: u32, committed: u64) -> Result<u64> {
        match self {
            Self::Iggy(journal) => journal.lag(topic, partition, committed).await,
            Self::Memory(journal) => Ok(journal
                .high_watermark(topic, partition)
                .saturating_sub(committed.saturating_add(1))),
        }
    }

    pub fn is_iggy(&self) -> bool {
        matches!(self, Self::Iggy(_))
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::Durability;

    #[test]
    fn memory_journal_offsets_and_commits() {
        let journal = MemoryJournal::new();
        let first = journal.append(
            Topic::Orders,
            3,
            &[
                JournalMessage::new(vec![1], vec![]),
                JournalMessage::new(vec![2], vec![]),
            ],
        );
        assert_eq!(first.base_offset, 0);
        assert_eq!(first.count, 2);
        let second = journal.append(Topic::Orders, 3, &[JournalMessage::new(vec![3], vec![])]);
        assert_eq!(second.base_offset, 2);
        let records = journal.read(Topic::Orders, 3, 1, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        journal.commit(Topic::Orders, 3, "engine-p3", 1);
        assert_eq!(journal.committed(Topic::Orders, 3, "engine-p3"), Some(1));
        assert_eq!(journal.high_watermark(Topic::Orders, 3), 3);
    }

    #[test]
    fn header_keys_cache_the_routing_set_and_unknown_names() {
        let keys = HeaderKeys::new().expect("routing header keys parse");
        for name in [
            "symbol", "seq", "venue", "type", "schema", "trace_id", "codec",
        ] {
            assert_eq!(
                keys.get(name).expect("known key"),
                HeaderKey::from_str(name).expect("reference key"),
                "cached key must match a freshly parsed key for {name}"
            );
        }

        let custom = keys.get("custom").expect("unknown key parses");
        assert_eq!(custom, HeaderKey::from_str("custom").unwrap());
        assert_eq!(
            keys.get("custom").expect("cached unknown key"),
            custom,
            "unknown keys are parsed once and reused"
        );
        assert_eq!(keys.extra.lock().unwrap().len(), 1);
    }

    #[test]
    fn topic_identifiers_are_cached_per_topic() {
        let ids = TopicIds::new().expect("topic identifiers");
        for topic in Topic::ALL {
            let (first_stream, first_topic) = ids.get(topic).expect("cached identifiers");
            let (second_stream, second_topic) = ids.get(topic).expect("cached identifiers");
            assert!(
                std::ptr::eq(first_stream, second_stream),
                "stream identifier is reused for {topic}"
            );
            assert!(
                std::ptr::eq(first_topic, second_topic),
                "topic identifier is reused for {topic}"
            );
        }

        // Orders and Executions share a stream but keep their own topic id.
        let (orders_stream, orders_topic) = ids.get(Topic::Orders).unwrap();
        let (executions_stream, executions_topic) = ids.get(Topic::Executions).unwrap();
        assert_eq!(orders_stream, executions_stream);
        assert_ne!(orders_topic, executions_topic);
    }

    #[test]
    fn journal_futures_are_send() {
        fn assert_send<T: Send>(future: T) {
            drop(future);
        }

        let journal = Journal::memory();
        assert_send(journal.append(Topic::Orders, 0, &[]));
        assert_send(journal.append_owned(Topic::Orders, 0, Vec::new()));
        assert_send(journal.read(Topic::Orders, 0, "consumer", 0, 1, 0));
        assert_send(journal.read_payloads(Topic::Orders, 0, "consumer", 0, 1, 0));
        assert_send(journal.commit(Topic::Orders, 0, "consumer", 0));
        assert_send(journal.committed(Topic::Orders, 0, "consumer"));
        assert_send(journal.lag(Topic::Orders, 0, 0));
        assert_send(journal.recover_operation(1));
    }

    #[test]
    fn operation_errors_expose_their_operation_id() {
        // These are the errors a broker restart under load produces; the
        // journal edge needs the id to settle the lane.
        assert_eq!(
            JournalError::from(IggyError::LaneBlocked(7)).operation_id(),
            Some(7)
        );
        assert_eq!(
            JournalError::from(IggyError::OperationUncertain(11)).operation_id(),
            Some(11)
        );
        assert_eq!(
            JournalError::from(IggyError::OperationNotRetained(5)).operation_id(),
            Some(5)
        );
        assert_eq!(
            JournalError::from(IggyError::PartitionLaneBlocked(3, 9)).operation_id(),
            Some(9)
        );

        // Errors that carry no retained operation must not trigger recovery.
        assert_eq!(
            JournalError::from(IggyError::InvalidConfiguration).operation_id(),
            None
        );
        assert_eq!(JournalError::NotConnected.operation_id(), None);
        assert_eq!(JournalError::NoConfirmation.operation_id(), None);
    }

    #[test]
    fn append_chunks_respect_the_iggy_batch_limit() {
        // Iggy's `message_bus.max_batch` default; the chunker must not exceed it.
        assert_eq!(MAX_APPEND_BATCH, 256);

        let mut empty: Vec<u32> = Vec::new();
        assert_eq!(append_chunks(&mut empty).count(), 0);

        let mut exact: Vec<u32> = (0..256).collect();
        let sizes: Vec<usize> = append_chunks(&mut exact).map(|chunk| chunk.len()).collect();
        assert_eq!(sizes, vec![256]);

        let mut fanout: Vec<u32> = (0..1_000).collect();
        let sizes: Vec<usize> = append_chunks(&mut fanout)
            .map(|chunk| chunk.len())
            .collect();
        assert_eq!(sizes, vec![256, 256, 256, 232]);
        assert_eq!(
            sizes.iter().sum::<usize>(),
            1_000,
            "chunking must not drop messages"
        );
    }

    #[test]
    fn iggy_message_round_trips_the_routing_headers() {
        let keys = HeaderKeys::new().expect("routing header keys parse");
        let headers = vec![
            ("symbol".to_string(), "BTC-USD".to_string()),
            ("seq".to_string(), "7".to_string()),
            ("venue".to_string(), "whippet".to_string()),
            ("type".to_string(), "order".to_string()),
            ("schema".to_string(), "1".to_string()),
            (
                "trace_id".to_string(),
                "0000000000000000000000000000000a".to_string(),
            ),
            ("codec".to_string(), "fixed-le-v1".to_string()),
        ];
        let message =
            build_iggy_message(&keys, Bytes::from_static(b"payload"), &headers).expect("frame");
        let decoded = message
            .user_headers_map()
            .expect("headers decode")
            .expect("headers present");
        assert_eq!(decoded.len(), headers.len());
        for (name, value) in &headers {
            let key = keys.get(name).expect("known key");
            assert_eq!(
                decoded.get(&key).map(HeaderValue::to_string_value),
                Some(value.clone()),
                "header {name} round-trips"
            );
        }
    }

    #[test]
    fn new_static_keeps_the_header_pairs() {
        let message = JournalMessage::new_static(
            vec![9],
            vec![("symbol", "ETH-USD".to_string()), ("seq", "3".to_string())],
        );
        assert_eq!(message.payload, vec![9]);
        assert_eq!(
            message.headers,
            vec![
                ("symbol".to_string(), "ETH-USD".to_string()),
                ("seq".to_string(), "3".to_string()),
            ]
        );
    }

    #[test]
    fn memory_append_owned_matches_borrowed_append() {
        let journal = MemoryJournal::new();
        let borrowed = journal.append(
            Topic::Orders,
            0,
            &[
                JournalMessage::new(vec![1, 2], vec![("n".to_string(), "0".to_string())]),
                JournalMessage::new(vec![3], vec![]),
            ],
        );
        let owned = journal.append_owned(
            Topic::Orders,
            0,
            vec![
                JournalMessage::new(vec![4], vec![]),
                JournalMessage::new(vec![5, 6], vec![]),
            ],
        );
        assert_eq!(owned.base_offset, borrowed.count as u64);
        assert_eq!(owned.count, 2);

        let records = journal.read(Topic::Orders, 0, 0, 10);
        assert_eq!(
            records
                .iter()
                .map(|record| (record.offset, record.payload.clone()))
                .collect::<Vec<_>>(),
            vec![(0, vec![1, 2]), (1, vec![3]), (2, vec![4]), (3, vec![5, 6])]
        );
        assert_eq!(records[0].header("n"), Some("0"));

        // An empty owned append is a no-op that still reports the partition.
        let empty = journal.append_owned(Topic::Orders, 0, Vec::new());
        assert_eq!(empty.partition, 0);
        assert_eq!(
            empty.base_offset, 4,
            "empty append reports the current head"
        );
        assert_eq!(empty.count, 0);
        assert_eq!(journal.high_watermark(Topic::Orders, 0), 4);
    }

    #[test]
    fn memory_read_payloads_matches_read_without_headers() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Orders,
            1,
            &[
                JournalMessage::new(vec![7], vec![("n".to_string(), "0".to_string())]),
                JournalMessage::new(vec![8], vec![]),
            ],
        );

        let full = journal.read(Topic::Orders, 1, 0, 10);
        let payloads = journal.read_payloads(Topic::Orders, 1, 0, 10);
        assert_eq!(payloads.len(), full.len());
        for (payload, record) in payloads.iter().zip(full.iter()) {
            assert_eq!(payload.offset, record.offset);
            assert_eq!(payload.timestamp_ns, record.timestamp_ns);
            assert_eq!(
                payload.payload.as_ref(),
                record.payload.as_slice(),
                "payload bytes match the full read path"
            );
        }

        assert!(journal.read_payloads(Topic::Orders, 1, 2, 10).is_empty());
        assert!(journal.read_payloads(Topic::Orders, 9, 0, 10).is_empty());
    }

    #[test]
    fn topics_are_pinned_to_the_documented_layout() {
        let exchange = ExchangeConfig::default();
        assert_eq!(Topic::Orders.partition_count(&exchange), 16);
        assert_eq!(Topic::Orders.stream(), STREAM_TRADING);
        assert_eq!(Topic::Md.stream(), STREAM_MD);
        assert_eq!(Topic::Alerts.stream(), STREAM_OPS);
        assert_eq!(Topic::Orders.durability(), Durability::Persisted);
        assert_eq!(Topic::Md.durability(), Durability::Replicated);
    }
}
