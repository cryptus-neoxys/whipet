// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The Iggy edge.
//!
//! Whippet treats Iggy as a partitioned log, not a queue. This crate owns the
//! only async Iggy client code in the system: topic bootstrap, appends, polls,
//! and manual consumer-offset commits. The matcher never sees an `await`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    AutoLogin, Client, CommitPolicy, Consumer, ConsumerOffsetClient, Credentials, HeaderKey,
    HeaderValue, Identifier, IggyByteSize, IggyClient, IggyDuration, IggyError, IggyExpiry,
    IggyMessage, MaxTopicSize, MessageClient, PollOptions, PollingStrategy, StreamClient,
    StreamCreateOptions, SystemClient, TopicClient, TopicCreateOptions,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub partition: u32,
    pub base_offset: u64,
    pub count: u32,
}

/// The Iggy-backed journal.
pub struct IggyJournal {
    client: Arc<IggyClient>,
    address: String,
}

impl IggyJournal {
    /// Connect with a username/password. The local demo uses the bootstrap
    /// account; production-style PATs and scoped users are Phase 2.
    pub async fn connect(address: &str, username: &str, password: &str) -> Result<Self> {
        let client = IggyClient::builder()
            .with_tcp()
            .with_server_address(address.to_string())
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
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn ping(&self) -> bool {
        self.client.ping().await.is_ok()
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
            let stream_id = Identifier::named(topic.stream())?;
            let topic_id = Identifier::named(topic.name())?;
            match self.client.get_topic(&stream_id, &topic_id).await? {
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
                        .create_topic(&stream_id, topic.name(), &options)
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

    pub async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> Result<AppendResult> {
        if messages.is_empty() {
            return Ok(AppendResult {
                partition,
                base_offset: 0,
                count: 0,
            });
        }
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let mut iggy_messages = Vec::with_capacity(messages.len());
        for message in messages {
            let mut headers = BTreeMap::new();
            for (key, value) in &message.headers {
                headers.insert(HeaderKey::from_str(key)?, HeaderValue::from_str(value)?);
            }
            let iggy_message = IggyMessage::builder()
                .payload(Bytes::from(message.payload.clone()))
                .user_headers(headers)
                .build()?;
            iggy_messages.push(iggy_message);
        }
        let response = self
            .client
            .send_messages(
                &stream_id,
                &topic_id,
                &iggy::prelude::Partitioning::partition_id(partition),
                &mut iggy_messages,
            )
            .await?;
        let confirmation = response
            .confirmations
            .first()
            .ok_or(JournalError::NoConfirmation)?;
        Ok(AppendResult {
            partition: confirmation.partition_id,
            base_offset: confirmation.base_offset,
            count: messages.len() as u32,
        })
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
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        let options = PollOptions {
            count,
            max_wait: IggyDuration::from(Duration::from_millis(max_wait_ms)),
            min_count: 1,
            max_bytes: 32 * 1024 * 1024,
            commit: CommitPolicy::Manual,
        };
        let polled = self
            .client
            .poll_messages(
                &stream_id,
                &topic_id,
                partition,
                &consumer,
                &PollingStrategy::offset(offset),
                options,
            )
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

    pub async fn commit(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> Result<()> {
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        self.client
            .store_consumer_offset(&consumer, &stream_id, &topic_id, Some(partition), offset)
            .await?;
        Ok(())
    }

    pub async fn committed(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> Result<Option<u64>> {
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let consumer = Consumer::new(Identifier::named(consumer)?);
        let info = self
            .client
            .get_consumer_offset(&consumer, &stream_id, &topic_id, Some(partition))
            .await?;
        Ok(info.map(|info| info.stored_offset))
    }

    /// Records between the committed consumer offset and the partition head.
    /// `committed` is the last processed offset (inclusive).
    pub async fn lag(&self, topic: Topic, partition: u32, committed: u64) -> Result<u64> {
        let stream_id = Identifier::named(topic.stream())?;
        let topic_id = Identifier::named(topic.name())?;
        let Some(details) = self.client.get_topic(&stream_id, &topic_id).await? else {
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
        for (index, message) in messages.iter().enumerate() {
            log.push(MemoryRecord {
                offset: base_offset + index as u64,
                timestamp_ns: now_ns(),
                payload: message.payload.clone(),
                headers: message.headers.clone(),
            });
        }
        AppendResult {
            partition,
            base_offset,
            count: messages.len() as u32,
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
