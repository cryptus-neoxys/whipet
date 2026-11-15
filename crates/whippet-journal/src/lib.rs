// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The Iggy edge.
//!
//! Whippet treats Iggy as a partitioned log, not a queue. This crate owns the
//! only async Iggy client code in the system: topic bootstrap, appends, polls,
//! and manual consumer-offset commits. The matcher never sees an `await`.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, CommitPolicy, Consumer, ConsumerOffsetClient, Credentials, HeaderKey,
    HeaderValue as IggyHeaderValue, Identifier, IggyByteSize, IggyClient, IggyDuration, IggyError,
    IggyExpiry, IggyMessage, MaxTopicSize, MessageClient, PendingOperationsConfig, PollOptions,
    PolledMessages, PollingStrategy, StreamClient, StreamCreateOptions, SystemClient, TopicClient,
    TopicCreateOptions,
};
use serde::{Deserialize, Serialize};
use whippet_core::config::ExchangeConfig;
use whippet_core::headers::{HeaderName, HeaderValue as MetaHeaderValue};

pub mod group;
pub mod topic;

pub use group::{GroupCommit, GroupCommitConfig, GroupCommitStats};
pub use topic::Topic;

pub const STREAM_TRADING: &str = "whippet-trading";
pub const STREAM_MD: &str = "whippet-md";
pub const STREAM_OPS: &str = "whippet-ops";

#[derive(Clone, Debug, thiserror::Error)]
pub enum JournalError {
    #[error("iggy error: {0}")]
    Iggy(#[from] IggyError),
    #[error("journal is not connected")]
    NotConnected,
    #[error("append returned no confirmation")]
    NoConfirmation,
    #[error("invalid journal configuration: {0}")]
    InvalidConfig(String),
    /// A group-commit writer task is gone; the submission was never appended.
    #[error("journal group-commit writer stopped")]
    WriterStopped,
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

/// One journal message: a payload and its header set.
///
/// The payload is [`Bytes`], so the borrowed append hands it to the Iggy frame
/// with a refcount bump instead of a copy, and the owned append moves it
/// outright. The constructors still take `Vec<u8>`; `Bytes::from(Vec<u8>)` is
/// zero-copy, so building a message costs no extra copy.
#[derive(Debug, Clone)]
pub struct JournalMessage {
    pub payload: Bytes,
    pub headers: JournalHeaders,
}

/// A message's header set, in the representation the producer chose.
///
/// The named variant carries [`HeaderName`] keys, so the header names cost no
/// per-message allocation and map straight onto the connection's cached
/// [`HeaderKey`]s when an Iggy frame is built. The pair variant keeps the
/// historical `String -> String` shape for tests, tools, and callers that use
/// keys outside the fixed routing set.
#[derive(Debug, Clone)]
pub enum JournalHeaders {
    Named(Vec<(HeaderName, MetaHeaderValue)>),
    Pairs(Vec<(String, String)>),
}

impl JournalHeaders {
    /// Whether this set uses the named representation.
    pub fn is_named(&self) -> bool {
        matches!(self, Self::Named(_))
    }

    /// Copy the set as string pairs.
    ///
    /// Allocates a key `String` per pair when the set is named, so only call
    /// this on paths that need string keys (memory journal, record reads).
    pub fn to_pairs(&self) -> Vec<(String, String)> {
        match self {
            Self::Named(named) => named
                .iter()
                .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            Self::Pairs(pairs) => pairs.clone(),
        }
    }

    /// Convert the set into string pairs, reusing the buffer when possible.
    pub fn into_pairs(self) -> Vec<(String, String)> {
        match self {
            Self::Named(named) => named
                .into_iter()
                .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
                .collect(),
            Self::Pairs(pairs) => pairs,
        }
    }
}

impl JournalMessage {
    pub fn new(payload: Vec<u8>, headers: Vec<(String, String)>) -> Self {
        Self {
            payload: Bytes::from(payload),
            headers: JournalHeaders::Pairs(headers),
        }
    }

    /// Build a message whose header keys are compile-time constants.
    ///
    /// The pairs are still stored as `(String, String)` because
    /// [`JournalMessage`] carries the wire-format header set; this only spares
    /// call sites the `.to_string()` noise. Use [`JournalMessage::new_named`]
    /// instead on the produce path: it keeps the keys allocation-free end to
    /// end.
    pub fn new_static(payload: Vec<u8>, headers: Vec<(&'static str, String)>) -> Self {
        Self {
            payload: Bytes::from(payload),
            headers: JournalHeaders::Pairs(
                headers
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value))
                    .collect(),
            ),
        }
    }

    /// Build a message with enum header keys.
    ///
    /// The keys are [`HeaderName`] values, so nothing allocates for the names:
    /// not here, and not when the journal maps them onto the connection's
    /// cached `HeaderKey`s while building the Iggy frame. Header *values*
    /// stay owned. Use [`whippet_core::headers::MessageMeta::named_pairs`] to
    /// build the argument.
    pub fn new_named(payload: Vec<u8>, headers: Vec<(HeaderName, MetaHeaderValue)>) -> Self {
        Self {
            payload: Bytes::from(payload),
            headers: JournalHeaders::Named(headers),
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

/// Default client-side pending-operations byte budget, per client.
///
/// Demo headroom: Iggy's default pool (256 active/retained mutations, 64 MiB
/// of frozen frames) can fill under batch or concurrent gateway load. This
/// only sizes the client-side frame pool; it is not a durability or
/// offset-semantics change. The engine journal pool splits its own budget
/// across clients via `IggyJournal::connect_with_pending`.
pub const DEFAULT_PENDING_MAX_BYTES: usize = 256 * 1024 * 1024;

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

    /// Resolve a [`HeaderName`] to its parsed key.
    ///
    /// The seven routing keys are parsed once per connection in [`Self::new`],
    /// so this arm cannot fail and costs one refcount bump per pair — no
    /// `HeaderKey::from_str`, no `String` key.
    fn named(&self, name: HeaderName) -> HeaderKey {
        match name {
            HeaderName::Symbol => self.symbol.clone(),
            HeaderName::Seq => self.seq.clone(),
            HeaderName::Venue => self.venue.clone(),
            HeaderName::Type => self.kind.clone(),
            HeaderName::Schema => self.schema.clone(),
            HeaderName::TraceId => self.trace_id.clone(),
            HeaderName::Codec => self.codec.clone(),
        }
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
        user_headers.insert(keys.get(key)?, IggyHeaderValue::from_str(value)?);
    }
    Ok(IggyMessage::builder()
        .payload(payload)
        .user_headers(user_headers)
        .build()?)
}

/// Build an Iggy frame from a payload and enum-keyed header pairs.
///
/// Same wire result as [`build_iggy_message`]: the keys resolve through the
/// connection's cached `HeaderKey`s ([`HeaderKeys::named`]) without ever
/// materialising a `String` key. Only the values are parsed per message.
fn build_iggy_message_named(
    keys: &HeaderKeys,
    payload: Bytes,
    headers: &[(HeaderName, MetaHeaderValue)],
) -> Result<IggyMessage> {
    let mut user_headers = BTreeMap::new();
    for (name, value) in headers {
        user_headers.insert(
            keys.named(*name),
            IggyHeaderValue::from_str(value.as_str())?,
        );
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

/// Maximum number of `Consumer` identifiers remembered per connection.
const CONSUMER_CACHE_LIMIT: usize = 64;

/// Cached `Consumer` identifiers, keyed by `(topic, partition, consumer name)`.
///
/// `Consumer::new(Identifier::named(name)?)` copies the name into a fresh
/// `Vec<u8>` and hashes it. The consume path builds one on every poll and the
/// commit path builds one for every offset read/write, so the parsed consumer
/// is kept here and reused. A poll for a known consumer therefore allocates
/// nothing for the identifier. The cache is bounded, and it is never touched
/// by the append path.
struct ConsumerCache {
    by_partition: Mutex<ConsumerMap>,
}

/// Consumers grouped by `(topic, partition)`.
///
/// Nested so a lookup by `&str` does not allocate a key: `HashMap<String,
/// _>::get` borrows the name. The outer key set is at most the topic layout's
/// partition count.
type ConsumerMap = HashMap<(Topic, u32), HashMap<String, Arc<Consumer>>>;

impl ConsumerCache {
    fn new() -> Self {
        Self {
            by_partition: Mutex::new(HashMap::new()),
        }
    }

    /// Return the cached consumer for this `(topic, partition, name)`, parsing
    /// it once. A miss past the cache limit returns a fresh consumer without
    /// growing the cache; behaviour is unchanged, only the reuse is lost.
    fn get(&self, topic: Topic, partition: u32, name: &str) -> Result<Arc<Consumer>> {
        let mut cache = self
            .by_partition
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(consumer) = cache
            .get(&(topic, partition))
            .and_then(|entries| entries.get(name))
        {
            return Ok(Arc::clone(consumer));
        }
        let consumer = Arc::new(Consumer::new(Identifier::named(name)?));
        if cached_consumer_count(&cache) < CONSUMER_CACHE_LIMIT {
            cache
                .entry((topic, partition))
                .or_default()
                .insert(name.to_string(), Arc::clone(&consumer));
        }
        Ok(consumer)
    }

    /// Number of cached consumers. Test-only; the cache has no runtime readers
    /// of its size.
    #[cfg(test)]
    fn entry_count(&self) -> usize {
        let cache = self
            .by_partition
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cached_consumer_count(&cache)
    }
}

fn cached_consumer_count(cache: &ConsumerMap) -> usize {
    cache.values().map(HashMap::len).sum()
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
    /// Consume/commit-side consumer identifiers; never used by appends.
    consumers: Box<ConsumerCache>,
}

impl IggyJournal {
    /// Connect with a username/password. The local demo uses the bootstrap
    /// account; production-style PATs and scoped users are Phase 2.
    ///
    /// Uses [`DEFAULT_PENDING_MAX_BYTES`] as the client-side pending budget.
    /// Call [`IggyJournal::connect_with_pending`] to size it explicitly.
    pub async fn connect(address: &str, username: &str, password: &str) -> Result<Self> {
        Self::connect_with_pending(address, username, password, DEFAULT_PENDING_MAX_BYTES).await
    }

    /// Connect with an explicit client-side pending-operations byte budget.
    ///
    /// The engine journal pool calls this with a per-client slice of the
    /// node's total budget (`WHIPPET_ENGINE_JOURNAL_PENDING_MIB`), so opening
    /// more clients cannot silently multiply the memory cost. All other
    /// connection settings are identical to [`IggyJournal::connect`].
    pub async fn connect_with_pending(
        address: &str,
        username: &str,
        password: &str,
        pending_bytes: usize,
    ) -> Result<Self> {
        let pending_bytes = NonZeroUsize::new(pending_bytes).ok_or_else(|| {
            JournalError::InvalidConfig("pending operations budget must be non-zero".to_string())
        })?;
        let client = IggyClient::builder()
            .with_tcp()
            .with_server_address(address.to_string())
            .with_pending_operations(PendingOperationsConfig {
                max_operations: NonZeroUsize::new(4096).expect("4096 is non-zero"),
                max_bytes: pending_bytes,
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
            consumers: Box::new(ConsumerCache::new()),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn ping(&self) -> bool {
        self.client.ping().await.is_ok()
    }

    /// Broker-reported size and message count for one topic.
    ///
    /// This is the storage-growth signal the disk guard and the retention
    /// planner need; it is a read and never touches retention.
    pub async fn topic_stats(&self, topic: Topic) -> Result<(u64, u64)> {
        let (stream_id, topic_id) = self.ids.get(topic)?;
        let Some(details) = self.client.get_topic(stream_id, topic_id).await? else {
            return Ok((0, 0));
        };
        Ok((details.size.as_bytes_u64(), details.messages_count))
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

    /// Append a borrowed batch. Payloads are handed to the Iggy frames with a
    /// [`Bytes`] refcount bump; use [`IggyJournal::append_owned`] when the
    /// caller can give up the messages.
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
            iggy_messages.push(self.iggy_message(message.payload.clone(), &message.headers)?);
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
    /// The payload buffers move into the Iggy frames instead of being cloned,
    /// which saves even the borrowed path's refcount bump per message.
    /// Chunking and error semantics match [`IggyJournal::append`].
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
            iggy_messages.push(self.iggy_message(payload, &headers)?);
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
    ///
    /// A named header set maps [`HeaderName`] straight onto the cached keys,
    /// so no `String` key is materialised. A pair set keeps the historical
    /// `&str`-lookup path.
    fn iggy_message(&self, payload: Bytes, headers: &JournalHeaders) -> Result<IggyMessage> {
        match headers {
            JournalHeaders::Named(named) => {
                build_iggy_message_named(&self.header_keys, payload, named)
            }
            JournalHeaders::Pairs(pairs) => build_iggy_message(&self.header_keys, payload, pairs),
        }
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
        let mut records = Vec::new();
        self.read_payloads_into(
            topic,
            partition,
            consumer,
            offset,
            count,
            max_wait_ms,
            &mut records,
        )
        .await?;
        Ok(records)
    }

    /// Poll records into a caller-owned buffer.
    ///
    /// `out` is cleared and then filled with the polled records; the number
    /// appended is returned. A caller that keeps its buffer across iterations
    /// pays no per-poll `Vec` allocation once the buffer has the capacity. The
    /// consumer identifier comes from the connection's cache, so a repeat poll
    /// for the same `(topic, partition, consumer)` allocates nothing for it.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_payloads_into(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
        out: &mut Vec<PayloadRecord>,
    ) -> Result<usize> {
        out.clear();
        let first = self
            .poll(topic, partition, consumer, offset, count, max_wait_ms)
            .await;
        let polled = self
            .retry_after(first, || {
                self.poll(topic, partition, consumer, offset, count, max_wait_ms)
            })
            .await?;
        out.reserve(polled.messages.len());
        for message in polled.messages {
            out.push(PayloadRecord {
                offset: message.header.offset,
                timestamp_ns: message.header.timestamp,
                payload: message.payload,
            });
        }
        Ok(out.len())
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
        let consumer = self.consumers.get(topic, partition, consumer)?;
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
        let consumer = self.consumers.get(topic, partition, consumer)?;
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
        let consumer = self.consumers.get(topic, partition, consumer)?;
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

/// The result of an empty append on the Iggy backend.
///
/// Iggy reports no send confirmation for a zero-message batch, so learning the
/// current head would cost an extra `get_topic` round trip on every empty
/// append. The Iggy path therefore reports `base_offset == 0`; the memory
/// backend reports the current length (`log.len()`, so a fresh partition also
/// reports 0). `count == 0` marks the result as empty on both backends, so no
/// caller may read `base_offset` without checking `count`. The contract test
/// `empty_append_reports_the_length_and_stores_nothing` in `tests/contract.rs`
/// pins the memory side and records this divergence.
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
///
/// # Offset contract
///
/// For every `(topic, partition)` the log is a contiguous sequence starting at
/// offset 0: record `i` has offset `i`, [`MemoryJournal::high_watermark`] is
/// `len` (the next offset to append, `0` when empty), and the head — the last
/// appended offset — is `high_watermark - 1`. Timestamps are non-decreasing
/// across appends. Reads return the contiguous slice from the requested
/// offset, capped by the requested count, and an empty slice at or past the
/// head. Offsets are `u64`, so a negative offset is unrepresentable.
///
/// # Commit contract
///
/// A consumer commit is the offset of the **last record the consumer fully
/// applied** (inclusive), never the next offset to read. Therefore
/// `0 <= committed <= head`, commits are monotonic per
/// `(topic, partition, consumer)`, and a partition with no records has no valid
/// commit. [`MemoryJournal::commit`] enforces this fail-closed; see its docs
/// for the debug and release behaviour. [`MemoryJournal::check_invariants`]
/// verifies the whole store.
///
/// These are the semantics the Iggy backend is measured against; both are
/// documented and pinned by the `contract` integration test.
pub struct MemoryJournal {
    state: Mutex<MemoryState>,
}

/// A consumer key: `(topic, partition, consumer name)`.
type CommitKey = (Topic, u32, String);

#[derive(Default)]
struct MemoryState {
    logs: BTreeMap<(Topic, u32), Vec<MemoryRecord>>,
    committed: BTreeMap<CommitKey, u64>,
}

#[derive(Clone)]
struct MemoryRecord {
    offset: u64,
    timestamp_ns: u64,
    payload: Vec<u8>,
    headers: Vec<(String, String)>,
}

/// Store a header set with unique keys, the last value for a key winning.
///
/// Iggy frames user headers in a `BTreeMap<HeaderKey, HeaderValue>`, so a
/// repeated key already overwrites the earlier value on the wire. Normalising
/// here gives the memory backend the same result and lets
/// [`MemoryJournal::check_invariants`] require unique keys. The position of a
/// key's first occurrence is kept, so the pair order still round-trips.
fn normalized_headers(pairs: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut unique: Vec<(String, String)> = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        match unique.iter_mut().find(|(existing, _)| existing == &key) {
            Some(slot) => slot.1 = value,
            None => unique.push((key, value)),
        }
    }
    unique
}

/// The offset of the last record in a log, if it has one.
fn head_of(log: &[MemoryRecord]) -> Option<u64> {
    log.last().map(|record| record.offset)
}

/// Why a commit request violates the contract, if it does.
///
/// Pure so both the debug assertion and the release warning report the same
/// reason, and so tests can construct each violation without depending on the
/// panic path.
fn commit_violation(state: &MemoryState, key: &CommitKey, offset: u64) -> Option<String> {
    let (topic, partition, consumer) = key;
    if consumer.is_empty() {
        return Some("the consumer name is empty".to_string());
    }
    let Some(log) = state.logs.get(&(*topic, *partition)) else {
        return Some(format!(
            "{topic} partition {partition} has no log; nothing has been appended"
        ));
    };
    let Some(head) = head_of(log) else {
        return Some(format!(
            "{topic} partition {partition} is empty; there is no offset for {consumer} to commit"
        ));
    };
    if offset > head {
        return Some(format!(
            "offset {offset} is ahead of the {topic} partition {partition} head {head} \
             for consumer {consumer}"
        ));
    }
    if let Some(previous) = state.committed.get(key)
        && offset < *previous
    {
        return Some(format!(
            "offset {offset} rewinds consumer {consumer} on {topic} partition {partition} \
             from {previous}"
        ));
    }
    None
}

/// Apply a commit once it has been validated, never storing an impossible
/// resume point.
///
/// - an empty consumer name, an unknown/empty partition, a backwards commit,
///   or an offset ahead of the head stores nothing and returns `None`.
///
/// An ahead-of-head offset is **ignored**, never clamped to the head. The
/// caller claimed it applied a record that does not exist, so its claim says
/// nothing reliable about any record that does; storing the head would mark an
/// unapplied record as done and skip it on the next restart. Ignoring replays
/// more, which the dedup table and MD sequence make idempotent.
fn apply_commit(state: &mut MemoryState, key: CommitKey, offset: u64) -> Option<u64> {
    if key.2.is_empty() {
        return None;
    }
    let head = state
        .logs
        .get(&(key.0, key.1))
        .and_then(|log| head_of(log))?;
    if offset > head {
        return None;
    }
    if let Some(previous) = state.committed.get(&key)
        && offset < *previous
    {
        return None;
    }
    state.committed.insert(key, offset);
    Some(offset)
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

    /// Append a borrowed batch. Offsets continue the partition's log, so the
    /// returned [`AppendResult::base_offset`] is the previous length and the
    /// batch occupies `base_offset..base_offset + count` with no holes.
    ///
    /// An empty batch stores nothing and reports `base_offset == len`,
    /// `count == 0`. Record timestamps are non-decreasing: a clock that steps
    /// backwards is clamped to the last stored timestamp.
    pub fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> AppendResult {
        let mut state = self.state.lock().expect("memory journal poisoned");
        let log = state.logs.entry((topic, partition)).or_default();
        let base_offset = log.len() as u64;
        let mut timestamp_ns = log.last().map_or(0, |record| record.timestamp_ns);
        for (index, message) in messages.iter().enumerate() {
            timestamp_ns = now_ns().max(timestamp_ns);
            log.push(MemoryRecord {
                offset: base_offset + index as u64,
                timestamp_ns,
                // Dev path: convert to the owned storage format at the
                // storage boundary; the Iggy path hands the `Bytes` on
                // without a copy.
                payload: message.payload.to_vec(),
                headers: normalized_headers(message.headers.to_pairs()),
            });
        }
        AppendResult {
            partition,
            base_offset,
            count: messages.len() as u32,
        }
    }

    /// Consume the messages instead of cloning their payloads.
    ///
    /// Offset, empty-append, and timestamp semantics match
    /// [`MemoryJournal::append`].
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
        let mut timestamp_ns = log.last().map_or(0, |record| record.timestamp_ns);
        for (index, message) in messages.into_iter().enumerate() {
            let JournalMessage { payload, headers } = message;
            timestamp_ns = now_ns().max(timestamp_ns);
            log.push(MemoryRecord {
                offset: base_offset + index as u64,
                timestamp_ns,
                // Dev path: convert to the owned storage format. Zero-copy
                // when the `Bytes` is uniquely held, which append_owned's
                // move guarantees.
                payload: payload.into(),
                headers: normalized_headers(headers.into_pairs()),
            });
        }
        AppendResult {
            partition,
            base_offset,
            count,
        }
    }

    /// Read records from `offset` in offset order, at most `count`.
    ///
    /// The result is exactly the contiguous slice that starts at `offset`; a
    /// read at or past the head, or `count == 0`, returns an empty vector.
    pub fn read(
        &self,
        topic: Topic,
        partition: u32,
        offset: u64,
        count: u32,
    ) -> Vec<JournalRecord> {
        let state = self.state.lock().expect("memory journal poisoned");
        let Some(log) = state.logs.get(&(topic, partition)) else {
            return Vec::new();
        };
        // Offsets are append-ordered and contiguous, so the slice bound is a
        // binary search and the slice itself cannot have a hole.
        let start = log.partition_point(|record| record.offset < offset);
        let take = log.len().saturating_sub(start).min(count as usize);
        log[start..start + take]
            .iter()
            .map(|record| JournalRecord {
                offset: record.offset,
                timestamp_ns: record.timestamp_ns,
                payload: record.payload.clone(),
                headers: record.headers.clone(),
            })
            .collect()
    }

    pub fn read_payloads(
        &self,
        topic: Topic,
        partition: u32,
        offset: u64,
        count: u32,
    ) -> Vec<PayloadRecord> {
        let mut records = Vec::new();
        self.read_payloads_into(topic, partition, offset, count, &mut records);
        records
    }

    /// Read payloads into a caller-owned buffer.
    ///
    /// `out` is cleared and then filled with exactly the contiguous slice from
    /// `offset`, capped by `count`; the number appended is returned. A reused
    /// buffer pays no per-read allocation. A read at or past the head, or
    /// `count == 0`, leaves `out` empty and returns `0`.
    pub fn read_payloads_into(
        &self,
        topic: Topic,
        partition: u32,
        offset: u64,
        count: u32,
        out: &mut Vec<PayloadRecord>,
    ) -> usize {
        out.clear();
        let state = self.state.lock().expect("memory journal poisoned");
        let Some(log) = state.logs.get(&(topic, partition)) else {
            return 0;
        };
        // Offsets are append-ordered, so the first candidate is a binary
        // search. The reserve is then exact: an empty poll allocates nothing,
        // and a full batch allocates once.
        let start = log.partition_point(|record| record.offset < offset);
        let take = log.len().saturating_sub(start).min(count as usize);
        out.reserve(take);
        for record in &log[start..start + take] {
            out.push(PayloadRecord {
                offset: record.offset,
                timestamp_ns: record.timestamp_ns,
                payload: Bytes::from(record.payload.clone()),
            });
        }
        out.len()
    }

    /// Record the offset of the last record the consumer fully applied.
    ///
    /// The contract for one `(topic, partition, consumer)`:
    ///
    /// - `offset` is inclusive: the last applied record, not the next offset to
    ///   read;
    /// - `0 <= offset <= head`, where `head` is the last appended offset, so a
    ///   partition with no records has no valid commit;
    /// - commits never rewind (a later commit is `>=` the stored one);
    /// - `committed()` is `None` until the first valid commit.
    ///
    /// Fail closed. A commit ahead of the head, a commit for an empty or
    /// unknown partition, an empty consumer name, or a backwards commit is a
    /// caller bug:
    ///
    /// - debug builds panic with the reason (`debug_assert!`);
    /// - release builds never store an impossible resume point. An
    ///   ahead-of-head commit is ignored, a backwards commit is ignored, and a
    ///   commit for an empty/unknown partition or an empty consumer name is
    ///   dropped. Every one of these replays more, never less.
    ///
    /// Dropping is the safe release behaviour for the last two cases: the
    /// consumer's stored offset stays `None` (or unchanged), so a restart
    /// replays from the start of a log that has no applied records yet. That is
    /// at-least-once and can duplicate, but it can never skip records the way a
    /// fabricated offset would.
    ///
    /// The choice of ignoring over storing a guessed head keeps a restart
    /// correct: the
    /// store is a resume hint, and the caller that lost the race with the
    /// appended log still resumes at a record that exists. A hard error would
    /// force the caller to distinguish "my offset is stale" from "the broker
    /// failed", which the commit signature (and the Iggy commit path) does not
    /// carry.
    pub fn commit(&self, topic: Topic, partition: u32, consumer: &str, offset: u64) {
        let mut state = self.state.lock().expect("memory journal poisoned");
        let key = (topic, partition, consumer.to_string());
        if let Some(violation) = commit_violation(&state, &key, offset) {
            debug_assert!(false, "invalid journal commit: {violation}");
            tracing::warn!(
                topic = %topic,
                partition,
                consumer,
                offset,
                violation,
                "dropping an invalid journal commit"
            );
        }
        apply_commit(&mut state, key, offset);
    }

    /// The stored commit for `(topic, partition, consumer)`.
    ///
    /// `None` before the first valid commit; otherwise the offset of the last
    /// record the consumer fully applied.
    pub fn committed(&self, topic: Topic, partition: u32, consumer: &str) -> Option<u64> {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .committed
            .get(&(topic, partition, consumer.to_string()))
            .copied()
    }

    /// Approximate in-memory size: payload plus header text, all partitions.
    pub fn topic_stats(&self, topic: Topic) -> (u64, u64) {
        let state = self.state.lock().expect("memory journal poisoned");
        let mut bytes = 0u64;
        let mut messages = 0u64;
        for ((queued, _), records) in &state.logs {
            if *queued != topic {
                continue;
            }
            messages = messages.saturating_add(records.len() as u64);
            for record in records {
                bytes = bytes.saturating_add(record.payload.len() as u64);
                for (key, value) in &record.headers {
                    bytes = bytes
                        .saturating_add(key.len() as u64)
                        .saturating_add(value.len() as u64);
                }
            }
        }
        (bytes, messages)
    }

    /// Number of records in `(topic, partition)`.
    ///
    /// This is the next offset to append: `0` for an unknown or empty
    /// partition, and `head + 1` otherwise. It is not the head; use
    /// [`MemoryJournal::read`] or `lag` for records.
    pub fn high_watermark(&self, topic: Topic, partition: u32) -> u64 {
        let state = self.state.lock().expect("memory journal poisoned");
        state
            .logs
            .get(&(topic, partition))
            .map_or(0, |log| log.len() as u64)
    }

    /// Verify the stored state obeys the journal contract.
    ///
    /// Checks every log and every commit, including invariants that the current
    /// code maintains implicitly and future changes could break:
    ///
    /// - each log's offsets are exactly `0..len`, contiguous and increasing;
    /// - each log's timestamps are non-decreasing;
    /// - each record's header keys are unique (the pair representation allows
    ///   duplicates; [`normalized_headers`] removes them on append);
    /// - every commit has a non-empty consumer name, a partition with at least
    ///   one record, and an offset at or below that partition's head.
    ///
    /// Payloads are allowed to be empty: audit and command records may carry no
    /// bytes, and the contract does not require one. BTreeMap ordering is
    /// inherent, so it is not re-checked.
    pub fn check_invariants(&self) -> std::result::Result<(), String> {
        let state = self.state.lock().expect("memory journal poisoned");
        for ((topic, partition), log) in &state.logs {
            let mut previous_timestamp = 0u64;
            for (index, record) in log.iter().enumerate() {
                if record.offset != index as u64 {
                    return Err(format!(
                        "{topic} partition {partition}: record {index} has offset {}; \
                         offsets must be contiguous from 0",
                        record.offset
                    ));
                }
                if record.timestamp_ns < previous_timestamp {
                    return Err(format!(
                        "{topic} partition {partition}: record {index} has timestamp {} \
                         after {previous_timestamp}; timestamps must be non-decreasing",
                        record.timestamp_ns
                    ));
                }
                previous_timestamp = record.timestamp_ns;
                let mut keys = HashSet::with_capacity(record.headers.len());
                for (key, _) in &record.headers {
                    if !keys.insert(key.as_str()) {
                        return Err(format!(
                            "{topic} partition {partition}: record {index} has header key \
                             {key:?} more than once"
                        ));
                    }
                }
            }
        }
        for ((topic, partition, consumer), offset) in &state.committed {
            if consumer.is_empty() {
                return Err(format!(
                    "commit on {topic} partition {partition} has an empty consumer name"
                ));
            }
            let Some(log) = state.logs.get(&(*topic, *partition)) else {
                return Err(format!(
                    "commit for {consumer} on {topic} partition {partition} has no log"
                ));
            };
            let Some(head) = head_of(log) else {
                return Err(format!(
                    "commit for {consumer} on {topic} partition {partition} exists but the \
                     partition has no records"
                ));
            };
            if *offset > head {
                return Err(format!(
                    "commit for {consumer} on {topic} partition {partition} is {offset}, \
                     ahead of the head {head}"
                ));
            }
        }
        Ok(())
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

    /// Read payloads into a caller-owned buffer.
    ///
    /// `out` is cleared and filled with the polled records; the number appended
    /// is returned. Keep the buffer across loop iterations to avoid a per-poll
    /// allocation. On the Iggy backend the consumer identifier also comes from
    /// the connection's bounded cache.
    #[allow(clippy::too_many_arguments)]
    pub async fn read_payloads_into(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
        out: &mut Vec<PayloadRecord>,
    ) -> Result<usize> {
        match self {
            Self::Iggy(journal) => {
                journal
                    .read_payloads_into(topic, partition, consumer, offset, count, max_wait_ms, out)
                    .await
            }
            Self::Memory(journal) => {
                Ok(journal.read_payloads_into(topic, partition, offset, count, out))
            }
        }
    }

    /// Record the offset of the last record the consumer fully applied.
    ///
    /// Same contract on both backends: the offset is inclusive (last applied,
    /// not next to read), `0 <= offset <= head`, commits are monotonic per
    /// `(topic, partition, consumer)`, and `committed()` is `None` until the
    /// first commit. The Iggy backend stores exactly what the broker accepts;
    /// the memory backend (the test backend those semantics are measured
    /// against) asserts on a violation in debug and ignores it in release,
    /// never storing an impossible resume point. See
    /// [`MemoryJournal::commit`].
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

    /// Bytes and messages for a topic, across all partitions.
    pub async fn topic_stats(&self, topic: Topic) -> Result<(u64, u64)> {
        match self {
            Self::Iggy(journal) => journal.topic_stats(topic).await,
            Self::Memory(journal) => Ok(journal.topic_stats(topic)),
        }
    }

    /// Records between the committed offset and the partition head.
    ///
    /// The committed offset is the last applied record (inclusive), so the
    /// result is `head - committed` with both ends saturating: a consumer
    /// committed through the head has zero lag. On both backends
    /// `committed + lag == head`, except that an empty or unknown partition
    /// reports `0` for both. `crates/whippet/src/cli.rs` relies on that
    /// identity to report the head.
    pub async fn lag(&self, topic: Topic, partition: u32, committed: u64) -> Result<u64> {
        match self {
            Self::Iggy(journal) => journal.lag(topic, partition, committed).await,
            // The memory log's length is `head + 1`, so subtract the inclusive
            // committed record to match Iggy's `current_offset - committed`.
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
        journal
            .check_invariants()
            .expect("offsets and the commit obey the contract");
    }

    /// The offsets a log exposes: contiguous from zero, read in order, with the
    /// advertised base offset and head.
    #[test]
    fn memory_offsets_are_contiguous_and_read_in_order() {
        let journal = MemoryJournal::new();
        let first = journal.append(
            Topic::Md,
            1,
            &[
                JournalMessage::new(vec![0], vec![("n".to_string(), "0".to_string())]),
                JournalMessage::new(vec![1], vec![("n".to_string(), "1".to_string())]),
                JournalMessage::new(vec![2], vec![]),
            ],
        );
        assert_eq!(first.base_offset, 0);
        assert_eq!(first.count, 3);
        let second = journal.append_owned(Topic::Md, 1, vec![JournalMessage::new(vec![3], vec![])]);
        assert_eq!(
            second.base_offset, 3,
            "the next append starts at the head + 1"
        );
        assert_eq!(journal.high_watermark(Topic::Md, 1), 4);

        let records = journal.read(Topic::Md, 1, 0, 100);
        assert_eq!(
            records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            records
                .iter()
                .map(|record| record.payload.clone())
                .collect::<Vec<_>>(),
            vec![vec![0], vec![1], vec![2], vec![3]]
        );
        assert_eq!(records[1].header("n"), Some("1"), "headers round-trip");
        assert!(
            records
                .windows(2)
                .all(|pair| pair[0].timestamp_ns <= pair[1].timestamp_ns),
            "timestamps are non-decreasing within a log"
        );
        journal.check_invariants().expect("contiguous log");
    }

    /// Reads are exactly the contiguous slice from the requested offset, capped
    /// by the count, and empty at or past the head even through the facade.
    #[test]
    fn memory_read_is_the_contiguous_slice_from_the_offset() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Orders,
            2,
            &[
                JournalMessage::new(vec![0], vec![]),
                JournalMessage::new(vec![1], vec![]),
                JournalMessage::new(vec![2], vec![]),
                JournalMessage::new(vec![3], vec![]),
            ],
        );

        let slice = journal.read(Topic::Orders, 2, 1, 2);
        assert_eq!(
            slice.iter().map(|record| record.offset).collect::<Vec<_>>(),
            vec![1, 2],
            "a mid-log read starts at the requested offset and stops at the count"
        );
        assert_eq!(
            journal
                .read(Topic::Orders, 2, 1, 2)
                .iter()
                .map(|record| record.payload.clone())
                .collect::<Vec<_>>(),
            vec![vec![1], vec![2]],
            "no hole between the requested offset and the slice end"
        );
        // The head itself is readable; one past it is not.
        assert_eq!(
            journal
                .read(Topic::Orders, 2, 3, 1)
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert!(journal.read(Topic::Orders, 2, 4, 10).is_empty());
        assert!(journal.read(Topic::Orders, 2, u64::MAX, 10).is_empty());
        assert!(
            journal.read(Topic::Orders, 2, 0, 0).is_empty(),
            "count zero reads nothing"
        );

        // The payload path has the identical slice bound and buffer contract.
        let payloads = journal.read_payloads(Topic::Orders, 2, 2, 5);
        assert_eq!(
            payloads
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        let mut out = vec![PayloadRecord {
            offset: 99,
            timestamp_ns: 99,
            payload: Bytes::from_static(b"stale"),
        }];
        assert_eq!(
            journal.read_payloads_into(Topic::Orders, 2, 2, 0, &mut out),
            0
        );
        assert!(out.is_empty(), "a zero count clears the buffer");
        assert_eq!(
            journal.read_payloads_into(Topic::Orders, 2, 2, 1, &mut out),
            1
        );
        assert_eq!(out[0].offset, 2, "the reused buffer holds only the slice");
    }

    /// An empty append is a no-op that reports the length as its base offset.
    #[test]
    fn memory_empty_append_reports_the_length_and_stores_nothing() {
        let journal = MemoryJournal::new();

        let empty = journal.append(Topic::Alerts, 0, &[]);
        assert_eq!(empty.partition, 0);
        assert_eq!(empty.base_offset, 0, "an unknown partition is empty");
        assert_eq!(empty.count, 0);

        journal.append(Topic::Alerts, 0, &[JournalMessage::new(vec![1], vec![])]);
        let empty = journal.append_owned(Topic::Alerts, 0, Vec::new());
        assert_eq!(empty.base_offset, 1, "an empty append reports the length");
        assert_eq!(empty.count, 0);
        assert_eq!(journal.high_watermark(Topic::Alerts, 0), 1);
        assert_eq!(journal.read(Topic::Alerts, 0, 0, 10).len(), 1);
        journal.check_invariants().expect("empty append is a no-op");
    }

    /// A commit is the last applied offset, and lag counts what is left.
    #[tokio::test]
    async fn memory_commit_is_the_last_applied_offset_and_lag_starts_after_it() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Orders,
            5,
            &[
                JournalMessage::new(vec![0], vec![]),
                JournalMessage::new(vec![1], vec![]),
                JournalMessage::new(vec![2], vec![]),
            ],
        );

        assert_eq!(
            journal.committed(Topic::Orders, 5, "engine-p5"),
            None,
            "no commit before the first one"
        );
        journal.commit(Topic::Orders, 5, "engine-p5", 1);
        assert_eq!(journal.committed(Topic::Orders, 5, "engine-p5"), Some(1));

        // `1` is the last applied record, so offset `2` is still unread.
        let next = journal.committed(Topic::Orders, 5, "engine-p5").unwrap() + 1;
        assert_eq!(
            journal
                .read(Topic::Orders, 5, next, 10)
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![2],
            "a resume after the commit replays no applied record"
        );
        journal
            .check_invariants()
            .expect("commit at the last applied offset");

        // Through the facade, lag is head - committed: one record remains.
        let journal = Journal::Memory(journal);
        let committed = journal
            .committed(Topic::Orders, 5, "engine-p5")
            .await
            .expect("committed through the facade")
            .expect("a commit exists");
        assert_eq!(committed, 1, "committed is last-applied, not next");
        assert_eq!(
            journal.lag(Topic::Orders, 5, committed).await.expect("lag"),
            1
        );
    }

    /// The commit normaliser drops or ignores instead of storing an impossible
    /// resume point. This is the release-mode path; the debug path asserts and
    /// is covered by `commit_contract_asserts_in_debug`.
    #[test]
    fn commit_normalisation_ignores_and_drops() {
        let mut state = MemoryState::default();
        state.logs.insert(
            (Topic::Orders, 0),
            (0..3)
                .map(|offset| MemoryRecord {
                    offset,
                    timestamp_ns: 0,
                    payload: vec![offset as u8],
                    headers: Vec::new(),
                })
                .collect(),
        );

        // An ahead-of-head claim says nothing reliable about the head, so it
        // stores nothing: clamping would mark an unapplied record as done.
        assert_eq!(
            apply_commit(&mut state, (Topic::Orders, 0, "c".to_string()), 99),
            None,
            "an ahead-of-head commit is ignored, never clamped"
        );
        assert_eq!(
            state.committed.get(&(Topic::Orders, 0, "c".to_string())),
            None
        );

        // A valid commit stores.
        assert_eq!(
            apply_commit(&mut state, (Topic::Orders, 0, "c".to_string()), 1),
            Some(1)
        );

        // A backwards commit is ignored, not stored.
        assert_eq!(
            apply_commit(&mut state, (Topic::Orders, 0, "c".to_string()), 0),
            None,
            "a backwards commit does not rewind"
        );
        assert_eq!(
            state.committed.get(&(Topic::Orders, 0, "c".to_string())),
            Some(&1)
        );

        // Unknown and empty partitions have no valid offset.
        assert_eq!(
            apply_commit(&mut state, (Topic::Md, 0, "c".to_string()), 0),
            None
        );
        state.logs.insert((Topic::Md, 0), Vec::new());
        assert_eq!(
            apply_commit(&mut state, (Topic::Md, 0, "c".to_string()), 0),
            None,
            "an empty partition has no offset to commit"
        );
        assert_eq!(
            apply_commit(&mut state, (Topic::Orders, 0, String::new()), 0),
            None,
            "an empty consumer name is dropped"
        );
        assert_eq!(state.committed.len(), 1, "only the valid commit was stored");
    }

    /// Every constructed violation is a debug assertion with a clear message.
    ///
    /// A fresh journal per case keeps the deliberate panic from poisoning the
    /// state mutex of the next case.
    #[cfg(debug_assertions)]
    #[test]
    fn commit_contract_asserts_in_debug() {
        fn one_record() -> MemoryJournal {
            let journal = MemoryJournal::new();
            journal.append(Topic::Orders, 0, &[JournalMessage::new(vec![1], vec![])]);
            journal
        }

        fn assert_panics(
            journal: &MemoryJournal,
            topic: Topic,
            partition: u32,
            consumer: &str,
            offset: u64,
            expected: &str,
        ) {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                journal.commit(topic, partition, consumer, offset);
            }));
            let payload = result.expect_err("an invalid commit must assert in debug");
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                .unwrap_or_default();
            assert!(
                message.contains(expected) && message.contains("invalid journal commit"),
                "unexpected assertion message: {message}"
            );
        }

        assert_panics(
            &one_record(),
            Topic::Orders,
            0,
            "engine-p0",
            9,
            "ahead of the",
        );
        assert_panics(
            &one_record(),
            Topic::Orders,
            9,
            "engine-p9",
            0,
            "has no log",
        );
        assert_panics(
            &MemoryJournal::new(),
            Topic::Md,
            0,
            "engine-p0",
            0,
            "has no log",
        );
        assert_panics(
            &one_record(),
            Topic::Orders,
            0,
            "",
            0,
            "consumer name is empty",
        );

        // An equal commit is forward progress, not a rewind; a smaller one is a
        // bug.
        let journal = one_record();
        journal.commit(Topic::Orders, 0, "engine-p0", 0);
        journal.commit(Topic::Orders, 0, "engine-p0", 0);
        journal.append(Topic::Orders, 0, &[JournalMessage::new(vec![2], vec![])]);
        journal.commit(Topic::Orders, 0, "engine-p0", 1);
        assert_panics(&journal, Topic::Orders, 0, "engine-p0", 0, "rewinds");
    }

    /// Release builds never store an impossible resume point; this is the
    /// end-to-end proof of the release half of the contract, run with
    /// `cargo test -p whippet-journal --release`.
    #[cfg(not(debug_assertions))]
    #[test]
    fn commit_ignores_ahead_and_drops_in_release() {
        let journal = MemoryJournal::new();
        journal.append(Topic::Orders, 0, &[JournalMessage::new(vec![1], vec![])]);

        journal.commit(Topic::Orders, 0, "engine-p0", 9);
        assert_eq!(
            journal.committed(Topic::Orders, 0, "engine-p0"),
            None,
            "an ahead-of-head commit stores nothing"
        );

        journal.commit(Topic::Orders, 0, "engine-p0", 0);
        assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(0));

        // A backwards commit does not rewind.
        journal.commit(Topic::Orders, 0, "engine-p0", 0);
        assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(0));

        // Partitions with no records and empty consumer names store nothing.
        journal.commit(Topic::Md, 0, "whippet-md-p0", 0);
        assert_eq!(journal.committed(Topic::Md, 0, "whippet-md-p0"), None);
        journal.commit(Topic::Orders, 0, "", 0);
        assert_eq!(journal.committed(Topic::Orders, 0, ""), None);

        journal
            .check_invariants()
            .expect("the release path keeps the store valid");
    }

    /// `check_invariants` accepts what the API produces.
    #[test]
    fn memory_check_invariants_accepts_a_valid_store() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Orders,
            0,
            &[
                JournalMessage::new(vec![1], vec![("n".to_string(), "0".to_string())]),
                JournalMessage::new(vec![], vec![]),
            ],
        );
        journal.append_owned(Topic::Orders, 0, vec![JournalMessage::new(vec![3], vec![])]);
        journal.commit(Topic::Orders, 0, "engine-p0", 0);
        journal.commit(Topic::Orders, 0, "engine-p0", 2);
        assert!(
            journal.check_invariants().is_ok(),
            "an empty payload is allowed and a full-length commit is in range"
        );
    }

    /// Each invariant reports the exact violation when the state is corrupted
    /// directly, so a future writer that skips the enforcing API is caught.
    #[test]
    fn memory_check_invariants_rejects_constructed_violations() {
        fn seeded() -> MemoryJournal {
            let journal = MemoryJournal::new();
            journal.append(
                Topic::Orders,
                0,
                &[JournalMessage::new(
                    vec![1],
                    vec![("n".to_string(), "0".to_string())],
                )],
            );
            journal
        }

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state
                .committed
                .insert((Topic::Orders, 0, "engine".to_string()), 9);
        }
        let error = journal
            .check_invariants()
            .expect_err("commit ahead of head");
        assert!(error.contains("ahead of the head 0"), "{error}");

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state
                .committed
                .insert((Topic::Md, 0, "engine".to_string()), 0);
        }
        let error = journal.check_invariants().expect_err("commit with no log");
        assert!(error.contains("has no log"), "{error}");

        let journal = seeded();
        journal.append(Topic::Md, 0, &[]);
        {
            let mut state = journal.state.lock().expect("memory journal");
            state
                .committed
                .insert((Topic::Md, 0, "engine".to_string()), 0);
        }
        let error = journal
            .check_invariants()
            .expect_err("commit on an empty partition");
        assert!(error.contains("has no records"), "{error}");

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state.committed.insert((Topic::Orders, 0, String::new()), 0);
        }
        let error = journal
            .check_invariants()
            .expect_err("commit with an empty consumer");
        assert!(error.contains("empty consumer"), "{error}");

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state
                .logs
                .get_mut(&(Topic::Orders, 0))
                .expect("log")
                .push(MemoryRecord {
                    offset: 5,
                    timestamp_ns: 0,
                    payload: vec![2],
                    headers: Vec::new(),
                });
        }
        let error = journal.check_invariants().expect_err("offset hole");
        assert!(error.contains("contiguous"), "{error}");

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state
                .logs
                .get_mut(&(Topic::Orders, 0))
                .expect("log")
                .push(MemoryRecord {
                    offset: 1,
                    timestamp_ns: 0,
                    payload: vec![2],
                    headers: Vec::new(),
                });
            state.logs.get_mut(&(Topic::Orders, 0)).expect("log")[0].timestamp_ns = 5;
        }
        let error = journal.check_invariants().expect_err("timestamp rewind");
        assert!(error.contains("non-decreasing"), "{error}");

        let journal = seeded();
        {
            let mut state = journal.state.lock().expect("memory journal");
            state.logs.get_mut(&(Topic::Orders, 0)).expect("log")[0]
                .headers
                .push(("n".to_string(), "1".to_string()));
        }
        let error = journal
            .check_invariants()
            .expect_err("duplicate header key");
        assert!(error.contains("more than once"), "{error}");
    }

    /// Duplicate header keys keep the last value, matching Iggy's header map.
    #[test]
    fn memory_duplicate_header_keys_keep_the_last_value() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Orders,
            0,
            &[JournalMessage::new(
                vec![1],
                vec![
                    ("n".to_string(), "0".to_string()),
                    ("n".to_string(), "1".to_string()),
                    ("other".to_string(), "x".to_string()),
                ],
            )],
        );
        let record = &journal.read(Topic::Orders, 0, 0, 1)[0];
        assert_eq!(record.headers.len(), 2, "the duplicate key is collapsed");
        assert_eq!(record.header("n"), Some("1"), "the last value wins");
        assert_eq!(record.header("other"), Some("x"));
        journal.check_invariants().expect("unique header keys");
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
        let mut payloads = Vec::new();
        assert_send(journal.read_payloads_into(
            Topic::Orders,
            0,
            "consumer",
            0,
            1,
            0,
            &mut payloads,
        ));
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
    fn default_pending_budget_is_the_demonstrated_256_mib() {
        assert_eq!(DEFAULT_PENDING_MAX_BYTES, 256 * 1024 * 1024);
    }

    #[tokio::test]
    async fn connect_with_pending_rejects_a_zero_budget_before_connecting() {
        // Validation happens before any I/O: a zero budget is a caller bug,
        // not a connection failure.
        let error = IggyJournal::connect_with_pending("127.0.0.1:1", "u", "p", 0)
            .await
            .err()
            .expect("a zero pending budget must be rejected");
        assert!(matches!(error, JournalError::InvalidConfig(_)));
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
                decoded.get(&key).map(IggyHeaderValue::to_string_value),
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
        assert_eq!(message.payload, Bytes::from_static(&[9u8]));
        assert!(!message.headers.is_named(), "new_static is the pair path");
        assert_eq!(
            message.headers.to_pairs(),
            vec![
                ("symbol".to_string(), "ETH-USD".to_string()),
                ("seq".to_string(), "3".to_string()),
            ]
        );
    }

    #[test]
    fn new_named_keeps_the_named_header_pairs() {
        let message = JournalMessage::new_named(
            vec![9],
            vec![
                (HeaderName::Symbol, "ETH-USD".into()),
                (HeaderName::Seq, "3".into()),
            ],
        );
        assert_eq!(message.payload, Bytes::from_static(&[9u8]));
        assert!(message.headers.is_named());
        assert_eq!(
            message
                .headers
                .to_pairs()
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![("symbol", "ETH-USD"), ("seq", "3")],
            "the named set renders to the same string pairs"
        );
        // Converting to pairs consumes and preserves values.
        assert_eq!(
            JournalMessage::new_named(vec![], vec![(HeaderName::Codec, "x".into())])
                .headers
                .into_pairs(),
            vec![("codec".to_string(), "x".to_string())]
        );
    }

    #[test]
    fn named_headers_round_trip_like_the_pair_path() {
        let keys = HeaderKeys::new().expect("routing header keys parse");
        let named: Vec<(HeaderName, MetaHeaderValue)> = vec![
            (HeaderName::Symbol, "BTC-USD".into()),
            (HeaderName::Seq, "7".into()),
            (HeaderName::Venue, "whippet".into()),
            (HeaderName::Type, "order".into()),
            (HeaderName::Schema, "1".into()),
            (
                HeaderName::TraceId,
                "0000000000000000000000000000000a".into(),
            ),
            (HeaderName::Codec, "fixed-le-v1".into()),
        ];
        let pairs = named
            .iter()
            .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
            .collect::<Vec<_>>();

        let from_named = build_iggy_message_named(&keys, Bytes::from_static(b"payload"), &named)
            .expect("named frame");
        let from_pairs =
            build_iggy_message(&keys, Bytes::from_static(b"payload"), &pairs).expect("pair frame");

        let named_map = from_named
            .user_headers_map()
            .expect("headers decode")
            .expect("headers present");
        let pair_map = from_pairs
            .user_headers_map()
            .expect("headers decode")
            .expect("headers present");

        // Every pair of the named path reads back identically to the pair path.
        assert_eq!(named_map.len(), pair_map.len());
        for (name, value) in &named {
            let key = keys.named(*name);
            assert_eq!(
                named_map.get(&key).map(IggyHeaderValue::to_string_value),
                Some(value.as_str().to_string()),
                "named header {name:?} round-trips"
            );
            assert_eq!(
                named_map.get(&key),
                pair_map.get(&key),
                "the named frame carries the same key/value as the pair frame"
            );
        }
        // And the key sets themselves are identical.
        assert_eq!(
            named_map.keys().collect::<Vec<_>>(),
            pair_map.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn named_and_pair_paths_agree_on_message_meta_sets() {
        use whippet_core::headers::{HeaderMode, MessageMeta, SCHEMA_MD_EVENT};
        use whippet_core::ids::{Seq, Symbol};
        let keys = HeaderKeys::new().expect("routing header keys parse");
        let meta = MessageMeta::new(
            Symbol::new("BTC-USD").unwrap(),
            Seq(42),
            "md_event",
            SCHEMA_MD_EVENT,
            7,
        );
        for mode in [HeaderMode::Full, HeaderMode::Routing] {
            let meta_named =
                build_iggy_message_named(&keys, Bytes::from_static(b"p"), &meta.named_pairs(mode))
                    .expect("named frame");
            let meta_pairs =
                build_iggy_message(&keys, Bytes::from_static(b"p"), &meta.pairs_with(mode))
                    .expect("pair frame");
            let named_map = meta_named
                .user_headers_map()
                .expect("headers decode")
                .expect("headers present");
            let pair_map = meta_pairs
                .user_headers_map()
                .expect("headers decode")
                .expect("headers present");
            assert_eq!(named_map, pair_map, "mode {mode:?} wires identical headers");
        }
    }

    #[test]
    fn header_name_from_str_matches_the_cached_keys() {
        let keys = HeaderKeys::new().expect("routing header keys parse");
        for name in HeaderName::ALL {
            assert_eq!(
                keys.named(name),
                keys.get(name.as_str()).expect("known key"),
                "the named lookup resolves to the same key as the string lookup for {name:?}"
            );
        }
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
        journal
            .check_invariants()
            .expect("the owned and borrowed appends form one contiguous log");
    }

    #[test]
    fn borrowed_and_owned_appends_produce_identical_payloads_and_offsets() {
        let journal = MemoryJournal::new();
        let payloads: Vec<Vec<u8>> = vec![vec![1, 2, 3], vec![4], vec![5, 6, 7, 8]];
        let messages_for = || {
            payloads
                .iter()
                .map(|payload| JournalMessage::new(payload.clone(), vec![]))
                .collect::<Vec<_>>()
        };

        // The same payload set goes through both APIs: the borrowed append
        // first, the owned append right after, continuing the same log.
        let borrowed = journal.append(Topic::Md, 7, &messages_for());
        let owned = journal.append_owned(Topic::Md, 7, messages_for());

        assert_eq!(borrowed.partition, owned.partition);
        assert_eq!(borrowed.base_offset, 0);
        assert_eq!(
            owned.base_offset,
            borrowed.base_offset + payloads.len() as u64,
            "the owned append continues the borrowed append's offsets"
        );
        assert_eq!(borrowed.count as usize, payloads.len());
        assert_eq!(owned.count as usize, payloads.len());

        let records = journal.read(Topic::Md, 7, 0, 10);
        assert_eq!(records.len(), 2 * payloads.len());
        for (index, expected) in payloads.iter().enumerate() {
            let from_borrowed = &records[index];
            let from_owned = &records[index + payloads.len()];
            assert_eq!(from_borrowed.offset, index as u64);
            assert_eq!(from_owned.offset, index as u64 + payloads.len() as u64);
            assert_eq!(from_borrowed.payload, *expected);
            assert_eq!(
                from_owned.payload, *expected,
                "the owned append stores the same payload bytes at its own offset"
            );
        }
        journal
            .check_invariants()
            .expect("the two append paths target one contiguous log");
    }

    #[test]
    fn borrowed_and_owned_payload_handoffs_build_identical_frames() {
        let keys = HeaderKeys::new().expect("routing header keys parse");
        let headers = vec![("seq".to_string(), "1".to_string())];

        // The borrowed append clones the shared payload into the frame; the
        // owned append moves it. Both route through the same frame builders,
        // so the wire result must be byte-identical.
        let borrowed_message = JournalMessage::new(vec![1, 2, 3], headers.clone());
        let borrowed_frame = build_iggy_message(&keys, borrowed_message.payload.clone(), &headers)
            .expect("borrowed frame");

        let owned_message = JournalMessage::new(vec![1, 2, 3], headers.clone());
        let JournalHeaders::Pairs(owned_pairs) = owned_message.headers.clone() else {
            unreachable!("new produces the pair header set");
        };
        let owned_frame =
            build_iggy_message(&keys, owned_message.payload, &owned_pairs).expect("owned frame");

        assert_eq!(
            borrowed_frame.payload, owned_frame.payload,
            "the cloned handoff and the moved handoff carry identical payload bytes"
        );
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
        journal
            .check_invariants()
            .expect("the read paths do not mutate the log");
    }

    #[test]
    fn consumer_identifiers_are_cached_per_topic_partition_and_name() {
        let cache = ConsumerCache::new();
        let first = cache
            .get(Topic::Orders, 0, "engine-p0")
            .expect("consumer identifier");
        let second = cache
            .get(Topic::Orders, 0, "engine-p0")
            .expect("consumer identifier");
        assert!(
            Arc::ptr_eq(&first, &second),
            "a repeat poll for the same consumer reuses the cached identifier"
        );
        assert_eq!(*first, *second, "the cached consumer equals a fresh one");
        assert_eq!(cache.entry_count(), 1);

        // The partition and the topic are part of the key: each is a distinct
        // entry, even though the name (and therefore the consumer) is equal.
        let other_partition = cache
            .get(Topic::Orders, 1, "engine-p0")
            .expect("consumer identifier");
        let other_topic = cache
            .get(Topic::Md, 0, "engine-p0")
            .expect("consumer identifier");
        assert!(!Arc::ptr_eq(&first, &other_partition));
        assert!(!Arc::ptr_eq(&first, &other_topic));
        assert_eq!(*first, *other_partition);
        assert_eq!(cache.entry_count(), 3);
    }

    #[test]
    fn consumer_cache_is_bounded_and_keeps_serving_misses() {
        let cache = ConsumerCache::new();
        let names: Vec<String> = (0..CONSUMER_CACHE_LIMIT * 3)
            .map(|index| format!("engine-p{index}"))
            .collect();

        for name in &names {
            let consumer = cache.get(Topic::Orders, 0, name).expect("consumer");
            assert_eq!(
                consumer.id.get_cow_str_value().expect("named id").as_ref(),
                name.as_str(),
                "a cache miss still builds the right consumer"
            );
        }
        assert_eq!(
            cache.entry_count(),
            CONSUMER_CACHE_LIMIT,
            "many distinct consumer names must not grow the cache without limit"
        );

        // The earliest names are the ones kept; they still hit the cache.
        let cached = cache
            .get(Topic::Orders, 0, &names[0])
            .expect("cached consumer");
        let cached_again = cache
            .get(Topic::Orders, 0, &names[0])
            .expect("cached consumer");
        assert!(Arc::ptr_eq(&cached, &cached_again));

        // Names past the limit are served without being stored.
        let late = names.last().expect("a name");
        let miss = cache.get(Topic::Orders, 0, late).expect("consumer");
        assert_eq!(
            miss.id.get_cow_str_value().expect("named id").as_ref(),
            late.as_str()
        );
        assert_eq!(
            cache.entry_count(),
            CONSUMER_CACHE_LIMIT,
            "a full cache is not grown by further misses"
        );
    }

    #[test]
    fn memory_read_payloads_into_clears_and_fills_the_buffer() {
        let journal = MemoryJournal::new();
        journal.append(
            Topic::Md,
            0,
            &[
                JournalMessage::new(vec![1], vec![]),
                JournalMessage::new(vec![2], vec![]),
            ],
        );

        // A pre-filled buffer is cleared, not appended to.
        let mut out = vec![PayloadRecord {
            offset: 99,
            timestamp_ns: 99,
            payload: Bytes::from_static(b"stale"),
        }];
        let appended = journal.read_payloads_into(Topic::Md, 0, 0, 10, &mut out);
        assert_eq!(appended, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].offset, 0, "the stale entry is gone");
        assert_eq!(out[0].payload.as_ref(), b"\x01");
        assert_eq!(
            out,
            journal.read_payloads(Topic::Md, 0, 0, 10),
            "the buffered read matches the allocating read"
        );

        // A reused buffer holds only the newest batch.
        let appended = journal.read_payloads_into(Topic::Md, 0, 1, 10, &mut out);
        assert_eq!(appended, 1);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].offset, 1);
        assert_eq!(out[0].payload.as_ref(), b"\x02");

        // A miss clears the buffer and appends nothing.
        let appended = journal.read_payloads_into(Topic::Md, 9, 0, 10, &mut out);
        assert_eq!(appended, 0);
        assert!(out.is_empty());
        journal
            .check_invariants()
            .expect("buffer reuse does not mutate the log");
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
