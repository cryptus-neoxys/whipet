// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The offset and consumer-commit contract, pinned through the `Journal`
//! facade.
//!
//! The memory backend is the executable reference for the semantics the Iggy
//! backend must provide; the Iggy half of each row is what the adapter code
//! documents and what `crates/whippet` relies on. Rows marked *(divergence)*
//! cannot be executed without a broker and are called out in the last test.
//!
//! | Contract row | Memory backend | Iggy backend |
//! | --- | --- | --- |
//! | Appended offsets | `0..len-1` per `(topic, partition)`, contiguous | per-partition `base_offset` from the send confirmation, contiguous within one 256-message chunk |
//! | `AppendResult.base_offset` | previous length | first chunk's confirmation `base_offset` |
//! | empty append | `base_offset == len`, `count == 0` *(divergence)* | `base_offset == 0`, `count == 0` |
//! | `timestamp_ns` | non-decreasing per log | broker-assigned, append order |
//! | `read` / `read_payloads_into` | contiguous slice from `offset`, capped by `count`, cleared buffer | poll from `offset`, broker returns the contiguous run |
//! | poll at/after head | empty | empty |
//! | commit meaning | last applied offset, inclusive | `store_consumer_offset` of the same value |
//! | `committed()` | stored value, `None` before the first commit | `get_consumer_offset`, `None` before the first commit |
//! | commit bounds | `0 <= committed <= head`; debug assert, release ignores an ahead-of-head claim | broker accepts the value; the engine only commits applied offsets |
//! | `lag(committed)` | `head - committed` | `partition.current_offset - committed`, i.e. the last offset minus `committed` |
//! | `committed + lag` | `head` (0 for an empty partition) | `head` (0 for an empty partition) |

use whippet_journal::{Journal, JournalMessage, Topic};

fn message(value: u8) -> JournalMessage {
    JournalMessage::new(vec![value], vec![("n".to_string(), value.to_string())])
}

#[tokio::test]
async fn appended_offsets_are_contiguous_per_partition() {
    let journal = Journal::memory();

    let first = journal
        .append(Topic::Orders, 0, &[message(0), message(1)])
        .await
        .expect("first append");
    assert_eq!(first.partition, 0);
    assert_eq!(first.base_offset, 0);
    assert_eq!(first.count, 2);

    let second = journal
        .append(Topic::Orders, 0, &[message(2)])
        .await
        .expect("second append");
    assert_eq!(
        second.base_offset, 2,
        "the next append starts at the previous length"
    );

    let other_partition = journal
        .append(Topic::Orders, 1, &[message(9)])
        .await
        .expect("other partition");
    assert_eq!(
        other_partition.base_offset, 0,
        "each partition has its own zero"
    );

    let records = journal
        .read(Topic::Orders, 0, "contract", 0, 100, 0)
        .await
        .expect("read");
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2],
        "offsets are exactly 0..len-1, in order"
    );
    assert!(
        records
            .windows(2)
            .all(|pair| pair[0].timestamp_ns <= pair[1].timestamp_ns),
        "timestamps are non-decreasing"
    );
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.payload, vec![index as u8]);
        let expected = index.to_string();
        assert_eq!(record.header("n"), Some(expected.as_str()));
    }
}

#[tokio::test]
async fn empty_append_reports_the_length_and_stores_nothing() {
    // Memory reports the current length. The Iggy adapter cannot: a zero-message
    // send has no confirmation, so `empty_append` reports `base_offset == 0` and
    // callers must gate on `count == 0` (see `empty_append` in `src/lib.rs`).
    let journal = Journal::memory();
    let empty = journal
        .append(Topic::Md, 2, &[])
        .await
        .expect("empty append");
    assert_eq!(empty.count, 0);
    assert_eq!(empty.base_offset, 0, "an unknown partition is empty");

    journal
        .append(Topic::Md, 2, &[message(1)])
        .await
        .expect("append");
    let empty = journal
        .append_owned(Topic::Md, 2, Vec::new())
        .await
        .expect("empty owned append");
    assert_eq!(empty.count, 0);
    assert_eq!(empty.base_offset, 1, "the empty append reports the length");

    assert_eq!(
        journal
            .read(Topic::Md, 2, "contract", 0, 10, 0)
            .await
            .expect("read")
            .len(),
        1,
        "an empty append does not change the log"
    );
}

#[tokio::test]
async fn polls_return_the_contiguous_slice_and_stop_at_the_head() {
    let journal = Journal::memory();
    journal
        .append(
            Topic::Md,
            0,
            &[message(0), message(1), message(2), message(3)],
        )
        .await
        .expect("append");

    let mut out = Vec::new();
    let count = journal
        .read_payloads_into(Topic::Md, 0, "contract", 1, 2, 0, &mut out)
        .await
        .expect("poll");
    assert_eq!(count, out.len(), "the return value is out.len()");
    assert_eq!(
        out.iter().map(|record| record.offset).collect::<Vec<_>>(),
        vec![1, 2],
        "the slice starts at the requested offset, capped by count"
    );

    // The buffer is cleared, not appended to, on every poll.
    let count = journal
        .read_payloads_into(Topic::Md, 0, "contract", 3, 10, 0, &mut out)
        .await
        .expect("poll");
    assert_eq!(count, 1);
    assert_eq!(out[0].offset, 3, "the head is readable");

    // At and past the head the poll is empty.
    for offset in [4, 5, 99] {
        let count = journal
            .read_payloads_into(Topic::Md, 0, "contract", offset, 10, 0, &mut out)
            .await
            .expect("poll past the head");
        assert_eq!(count, 0, "poll at {offset} must be empty");
        assert!(out.is_empty(), "the buffer is cleared on an empty poll");
    }
    assert!(
        journal
            .read(Topic::Md, 0, "contract", 0, 0, 0)
            .await
            .expect("zero count")
            .is_empty(),
        "count zero reads nothing"
    );
}

#[tokio::test]
async fn committed_is_the_last_applied_offset_and_lag_is_head_minus_it() {
    let journal = Journal::memory();
    journal
        .append(
            Topic::Orders,
            3,
            &[message(0), message(1), message(2), message(3), message(4)],
        )
        .await
        .expect("append five records");

    let consumer = "whippet-engine-p3";
    assert_eq!(
        journal
            .committed(Topic::Orders, 3, consumer)
            .await
            .expect("committed"),
        None,
        "no commit before the first one"
    );

    // Commit the first two records: offset 1 is the last applied record.
    journal
        .commit(Topic::Orders, 3, consumer, 1)
        .await
        .expect("commit");
    assert_eq!(
        journal
            .committed(Topic::Orders, 3, consumer)
            .await
            .expect("committed"),
        Some(1)
    );
    assert_eq!(
        journal.lag(Topic::Orders, 3, 1).await.expect("lag"),
        3,
        "head 4 minus committed 1 leaves records 2, 3, 4"
    );
    assert_eq!(
        1 + journal.lag(Topic::Orders, 3, 1).await.expect("lag"),
        4,
        "committed + lag is the head on both backends"
    );
    assert_eq!(
        journal
            .read(Topic::Orders, 3, consumer, 2, 10, 0)
            .await
            .expect("resume read")
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![2, 3, 4],
        "resuming at committed + 1 replays nothing already applied"
    );

    // Committing through the head drains the lag.
    journal
        .commit(Topic::Orders, 3, consumer, 4)
        .await
        .expect("commit through the head");
    assert_eq!(journal.lag(Topic::Orders, 3, 4).await.expect("lag"), 0);

    // A partition with no records reports zero lag, like Iggy's missing
    // partition arm.
    assert_eq!(journal.lag(Topic::Orders, 9, 0).await.expect("lag"), 0);
    assert_eq!(
        journal
            .committed(Topic::Orders, 9, consumer)
            .await
            .expect("committed"),
        None
    );
}

#[tokio::test]
async fn commits_are_monotonic_and_bounded_by_the_head() {
    let journal = Journal::memory();
    journal
        .append(Topic::Orders, 0, &[message(0), message(1), message(2)])
        .await
        .expect("append");

    journal
        .commit(Topic::Orders, 0, "engine-p0", 0)
        .await
        .expect("first commit");
    journal
        .commit(Topic::Orders, 0, "engine-p0", 2)
        .await
        .expect("forward commit");
    assert_eq!(
        journal
            .committed(Topic::Orders, 0, "engine-p0")
            .await
            .expect("committed"),
        Some(2),
        "the inclusive head is a valid commit"
    );

    // A commit for a different consumer is independent.
    journal
        .commit(Topic::Orders, 0, "other", 1)
        .await
        .expect("other consumer");
    assert_eq!(
        journal
            .committed(Topic::Orders, 0, "engine-p0")
            .await
            .expect("committed"),
        Some(2)
    );
}

/// In debug builds, an impossible resume point is a panic, not a stored value.
#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "invalid journal commit")]
async fn commit_ahead_of_the_head_asserts_in_debug() {
    let journal = Journal::memory();
    journal
        .append(Topic::Orders, 0, &[message(0)])
        .await
        .expect("append");
    journal
        .commit(Topic::Orders, 0, "engine-p0", 5)
        .await
        .expect("the commit is a caller bug");
}

/// In debug builds, committing on a partition with no records is also a bug.
#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "invalid journal commit")]
async fn commit_on_an_empty_partition_asserts_in_debug() {
    let journal = Journal::memory();
    journal
        .commit(Topic::Md, 0, "whippet-md-p0", 0)
        .await
        .expect("the commit is a caller bug");
}
