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
}

#[test]
fn commits_are_scoped_to_consumer_and_partition() {
    let journal = MemoryJournal::new();
    journal.append(Topic::Orders, 0, &[message(1)]);
    journal.append(Topic::Orders, 1, &[message(2)]);

    journal.commit(Topic::Orders, 0, "engine-p0", 0);
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(0));
    assert_eq!(journal.committed(Topic::Orders, 1, "engine-p0"), None);
    assert_eq!(journal.committed(Topic::Orders, 0, "other"), None);

    // Manual commits are explicit writes; the last one wins.
    journal.commit(Topic::Orders, 0, "engine-p0", 5);
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(5));
    journal.commit(Topic::Orders, 0, "other", 3);
    assert_eq!(journal.committed(Topic::Orders, 0, "other"), Some(3));
    assert_eq!(journal.committed(Topic::Orders, 0, "engine-p0"), Some(5));
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

    // An empty append is a no-op with zero count.
    let empty = journal
        .append(Topic::Orders, 2, &[])
        .await
        .expect("empty append");
    assert_eq!(empty.count, 0);
    assert_eq!(
        journal
            .read(Topic::Orders, 2, "engine-p2", 0, 10, 0)
            .await
            .unwrap()
            .len(),
        2
    );
}
