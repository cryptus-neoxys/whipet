// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Multi-partition coverage for the in-memory journal.
//!
//! The unit tests in `src/lib.rs` cover a single partition. These tests pin
//! the semantics the engine depends on: offsets are per partition, high
//! watermarks are per partition, reads start at an offset, and commits are
//! scoped to `(topic, partition, consumer)`.

use whippet_journal::{Journal, JournalMessage, MemoryJournal, Topic};

fn message(value: u8) -> JournalMessage {
    JournalMessage::new(vec![value], vec![("n".to_string(), value.to_string())])
}

#[test]
fn partitions_have_independent_offsets_and_watermarks() {
    let journal = MemoryJournal::new();

    let first = journal.append(Topic::Orders, 0, &[message(0), message(1)]);
    assert_eq!(first.partition, 0);
    assert_eq!(first.base_offset, 0);
    assert_eq!(first.count, 2);

    let second = journal.append(Topic::Orders, 1, &[message(10)]);
    assert_eq!(second.partition, 1);
    assert_eq!(second.base_offset, 0, "partition 1 starts at its own zero");

    let third = journal.append(Topic::Orders, 0, &[message(2)]);
    assert_eq!(
        third.base_offset, 2,
        "partition 0 continues where it left off"
    );

    assert_eq!(journal.high_watermark(Topic::Orders, 0), 3);
    assert_eq!(journal.high_watermark(Topic::Orders, 1), 1);
    assert_eq!(journal.high_watermark(Topic::Orders, 2), 0);

    let partition_one = journal.read(Topic::Orders, 1, 0, 10);
    assert_eq!(partition_one.len(), 1);
    assert_eq!(partition_one[0].offset, 0);
    assert_eq!(partition_one[0].payload, vec![10]);
    assert_eq!(partition_one[0].header("n"), Some("10"));

    let tail = journal.read(Topic::Orders, 0, 1, 10);
    assert_eq!(
        tail.iter().map(|record| record.offset).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(journal.read(Topic::Orders, 0, 99, 10).is_empty());
    assert!(journal.read(Topic::Orders, 7, 0, 10).is_empty());
    journal
        .check_invariants()
        .expect("per-partition offsets and watermarks");
}

#[test]
fn commits_are_scoped_to_consumer_and_partition() {
    let journal = MemoryJournal::new();
    journal.append(Topic::Orders, 0, &[message(1), message(2), message(3)]);
    journal.append(Topic::Orders, 1, &[message(2)]);

    journal.commit(Topic::Orders, 0, "engine-p0", 0);
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(0));
    assert_eq!(journal.committed(Topic::Orders, 1, "engine-p0"), None);
    assert_eq!(journal.committed(Topic::Orders, 0, "other"), None);

    // Manual commits are explicit writes; a forward commit replaces the stored
    // offset, and the partitions stay independent.
    journal.commit(Topic::Orders, 0, "engine-p0", 2);
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(2));
    journal.commit(Topic::Orders, 0, "other", 1);
    assert_eq!(journal.committed(Topic::Orders, 0, "other"), Some(1));
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(2));
    journal
        .check_invariants()
        .expect("commits stay within their partition heads");
}

#[test]
fn topics_are_separate_logs() {
    let journal = MemoryJournal::new();
    journal.append(Topic::Orders, 3, &[message(1)]);
    journal.append(Topic::Md, 3, &[message(2)]);

    assert_eq!(journal.read(Topic::Orders, 3, 0, 10).len(), 1);
    assert_eq!(journal.read(Topic::Md, 3, 0, 10).len(), 1);
    assert_eq!(journal.high_watermark(Topic::Orders, 3), 1);
    assert_eq!(journal.high_watermark(Topic::Md, 3), 1);

    journal.commit(Topic::Orders, 3, "consumer", 0);
    assert_eq!(journal.committed(Topic::Orders, 3, "consumer"), Some(0));
    assert_eq!(journal.committed(Topic::Md, 3, "consumer"), None);
    journal
        .check_invariants()
        .expect("a commit on one topic does not leak into another");
}

#[tokio::test]
async fn memory_journal_has_no_lanes_to_recover() {
    let journal = Journal::memory();
    assert!(!journal.recover_operation(7).await);
}

#[tokio::test]
async fn owned_append_and_payload_reads_match_the_borrowed_api() {
    let journal = Journal::memory();

    let borrowed = journal
        .append(Topic::Md, 1, &[message(1)])
        .await
        .expect("borrowed append");
    assert_eq!(borrowed.base_offset, 0);

    let owned = journal
        .append_owned(Topic::Md, 1, vec![message(2), message(3)])
        .await
        .expect("owned append");
    assert_eq!(owned.partition, 1);
    assert_eq!(owned.base_offset, 1, "owned append continues the log");
    assert_eq!(owned.count, 2);

    let empty = journal
        .append_owned(Topic::Md, 1, Vec::new())
        .await
        .expect("empty owned append");
    assert_eq!(empty.count, 0);

    let records = journal
        .read(Topic::Md, 1, "md-p1", 0, 10, 0)
        .await
        .expect("full read");
    let payloads = journal
        .read_payloads(Topic::Md, 1, "md-p1", 0, 10, 0)
        .await
        .expect("payload read");

    assert_eq!(payloads.len(), records.len());
    for (payload, record) in payloads.iter().zip(records.iter()) {
        assert_eq!(payload.offset, record.offset);
        assert_eq!(payload.timestamp_ns, record.timestamp_ns);
        assert_eq!(
            payload.payload.as_ref(),
            record.payload.as_slice(),
            "payload bytes match the full read path"
        );
    }
    assert_eq!(
        records
            .iter()
            .map(|record| record.payload.clone())
            .collect::<Vec<_>>(),
        vec![vec![1], vec![2], vec![3]]
    );
    assert_eq!(records[0].header("n"), Some("1"));

    // Offsets past the tail and other partitions stay empty.
    assert!(
        journal
            .read_payloads(Topic::Md, 1, "md-p1", 9, 10, 0)
            .await
            .expect("tail read")
            .is_empty()
    );
    assert!(
        journal
            .read_payloads(Topic::Md, 0, "md-p0", 0, 10, 0)
            .await
            .expect("empty partition read")
            .is_empty()
    );
}

#[tokio::test]
async fn payload_reads_into_a_reused_buffer_match_the_allocating_api() {
    let journal = Journal::memory();
    journal
        .append(Topic::Md, 1, &[message(1), message(2), message(3)])
        .await
        .expect("append");

    let expected = journal
        .read_payloads(Topic::Md, 1, "md-p1", 1, 10, 0)
        .await
        .expect("allocating read");
    let mut out = vec![whippet_journal::PayloadRecord {
        offset: 42,
        timestamp_ns: 0,
        payload: bytes::Bytes::from_static(b"stale"),
    }];
    let appended = journal
        .read_payloads_into(Topic::Md, 1, "md-p1", 1, 10, 0, &mut out)
        .await
        .expect("buffered read");
    assert_eq!(appended, expected.len());
    assert_eq!(
        out, expected,
        "offsets and payloads match the allocating API, and the stale entry is gone"
    );

    // The buffer is reused: capacity survives, contents are replaced.
    let capacity = out.capacity();
    let appended = journal
        .read_payloads_into(Topic::Md, 1, "md-p1", 2, 10, 0, &mut out)
        .await
        .expect("buffered read");
    assert_eq!(appended, 1);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].offset, 2);
    assert_eq!(out[0].payload.as_ref(), &[3]);
    assert!(
        out.capacity() >= capacity,
        "reusing the buffer keeps its capacity"
    );
}

#[tokio::test]
async fn journal_memory_wrapper_matches_the_sync_api() {
    let journal = Journal::memory();
    assert!(journal.ping().await);
    assert!(!journal.is_iggy());

    let appended = journal
        .append(Topic::Orders, 2, &[message(1), message(2)])
        .await
        .expect("memory append");
    assert_eq!(appended.partition, 2);
    assert_eq!(appended.base_offset, 0);
    assert_eq!(appended.count, 2);

    let records = journal
        .read(Topic::Orders, 2, "engine-p2", 0, 10, 0)
        .await
        .expect("memory read");
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].offset, 1);

    journal
        .commit(Topic::Orders, 2, "engine-p2", 1)
        .await
        .expect("memory commit");
    assert_eq!(
        journal
            .committed(Topic::Orders, 2, "engine-p2")
            .await
            .expect("memory committed offset"),
        Some(1)
    );

    // An empty append is a no-op with zero count that reports the length.
    let empty = journal
        .append(Topic::Orders, 2, &[])
        .await
        .expect("empty append");
    assert_eq!(empty.count, 0);
    assert_eq!(empty.base_offset, 2, "an empty append reports the length");
    assert_eq!(
        journal
            .read(Topic::Orders, 2, "engine-p2", 0, 10, 0)
            .await
            .unwrap()
            .len(),
        2
    );
}
