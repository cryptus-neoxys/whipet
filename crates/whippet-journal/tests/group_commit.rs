// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Group-commit behaviour over the in-memory journal.
//!
//! These pin the contract the engine relies on: concurrent submissions to one
//! partition coalesce into one append, each submission still gets its own
//! contiguous offsets, a batch is never interleaved with another submission,
//! and separate partitions have separate writer tasks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use whippet_journal::{
    GroupCommit, GroupCommitConfig, Journal, JournalError, JournalMessage, JournalRecord, Topic,
};

fn message(value: u64) -> JournalMessage {
    JournalMessage::new(
        value.to_le_bytes().to_vec(),
        vec![("n".to_string(), value.to_string())],
    )
}

fn payload(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

async fn records(
    journal: &Journal,
    topic: Topic,
    partition: u32,
    count: u32,
) -> Vec<JournalRecord> {
    journal
        .read(topic, partition, "group-commit-test", 0, count, 0)
        .await
        .expect("read back appended records")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_single_message_submissions_get_contiguous_offsets() {
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());

    let mut handles = Vec::new();
    for value in 0..64u64 {
        let group = group.clone();
        handles.push(tokio::spawn(async move {
            let result = group
                .append(Topic::Orders, 0, vec![message(value)])
                .await
                .expect("group append");
            (value, result)
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("submission task"));
    }

    let mut offsets: Vec<u64> = results
        .iter()
        .map(|(_, result)| result.base_offset)
        .collect();
    offsets.sort_unstable();
    assert_eq!(
        offsets,
        (0..64).collect::<Vec<_>>(),
        "64 concurrent submissions must produce exactly offsets 0..64"
    );

    let stored = records(&journal, Topic::Orders, 0, 64).await;
    assert_eq!(stored.len(), 64);
    for (value, result) in results {
        assert_eq!(result.partition, 0);
        assert_eq!(result.count, 1);
        assert_eq!(
            stored[result.base_offset as usize].payload,
            payload(value),
            "offset {} must hold the message submitted for value {value}",
            result.base_offset
        );
    }

    group.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linger_coalesces_concurrent_submissions() {
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        journal,
        GroupCommitConfig {
            max_batch: 256,
            linger: Duration::from_millis(2),
        },
    );

    let mut handles = Vec::new();
    for value in 0..32u64 {
        let group = group.clone();
        handles.push(tokio::spawn(async move {
            group
                .append(Topic::Orders, 0, vec![message(value)])
                .await
                .expect("group append")
                .base_offset
        }));
    }

    let mut offsets = Vec::new();
    for handle in handles {
        offsets.push(handle.await.expect("submission task"));
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..32).collect::<Vec<_>>());

    let stats = group.stats();
    assert_eq!(stats.messages, 32, "every message is appended exactly once");
    assert!(
        stats.batches < 32,
        "32 concurrent submissions with a 2ms linger should coalesce, saw {} batches",
        stats.batches
    );
    assert!(
        stats.max_batch_seen > 1,
        "at least one append carried more than one message"
    );

    group.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_batch_one_never_coalesces() {
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        journal,
        GroupCommitConfig {
            max_batch: 1,
            linger: Duration::from_millis(50),
        },
    );

    let mut handles = Vec::new();
    for value in 0..32u64 {
        let group = group.clone();
        handles.push(tokio::spawn(async move {
            group
                .append(Topic::Orders, 0, vec![message(value)])
                .await
                .expect("group append")
                .base_offset
        }));
    }

    let mut offsets = Vec::new();
    for handle in handles {
        offsets.push(handle.await.expect("submission task"));
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..32).collect::<Vec<_>>());

    let stats = group.stats();
    assert_eq!(stats.messages, 32);
    assert_eq!(
        stats.max_batch_seen, 1,
        "max_batch = 1 must flush one submission per append"
    );
    assert_eq!(
        stats.batches, 32,
        "latency mode flushes immediately instead of lingering"
    );

    group.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_submission_is_not_interleaved_with_another() {
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        Arc::clone(&journal),
        GroupCommitConfig {
            max_batch: 8,
            linger: Duration::from_millis(2),
        },
    );

    let first = {
        let group = group.clone();
        tokio::spawn(async move {
            group
                .append(Topic::Orders, 0, (0..5u64).map(message).collect())
                .await
                .expect("five-message submission")
        })
    };
    let second = {
        let group = group.clone();
        tokio::spawn(async move {
            group
                .append(Topic::Orders, 0, (100..105u64).map(message).collect())
                .await
                .expect("five-message submission")
        })
    };

    let first = first.await.expect("first submission task");
    let second = second.await.expect("second submission task");
    assert_eq!(first.count, 5);
    assert_eq!(second.count, 5);

    let first_range = first.base_offset..first.base_offset + 5;
    let second_range = second.base_offset..second.base_offset + 5;
    assert!(
        first_range.end <= second_range.start || second_range.end <= first_range.start,
        "a submission's five offsets must be contiguous and must not interleave: \
         {first_range:?} vs {second_range:?}"
    );

    let stored = records(&journal, Topic::Orders, 0, 10).await;
    assert_eq!(stored.len(), 10);
    for (result, values) in [(first, 0..5u64), (second, 100..105u64)] {
        for (index, value) in values.enumerate() {
            let offset = result.base_offset + index as u64;
            assert_eq!(
                stored[offset as usize].payload,
                payload(value),
                "offset {offset} must hold message {value} of its own submission"
            );
        }
    }

    group.shutdown().await;
}

#[tokio::test]
async fn writer_tasks_exit_when_the_group_shuts_down() {
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        journal,
        GroupCommitConfig {
            max_batch: 4,
            linger: Duration::from_millis(1),
        },
    );

    // Non-empty submissions, as group commit now rejects empty ones: a
    // one-message submission to each partition creates both writers and
    // answers each caller with its own offset.
    let orders = group
        .append(Topic::Orders, 0, vec![message(1)])
        .await
        .expect("orders submission");
    assert_eq!(orders.base_offset, 0);
    let md = group
        .append(Topic::Md, 0, vec![message(2)])
        .await
        .expect("md submission");
    assert_eq!(md.base_offset, 0);
    assert_eq!(group.writer_count(), 2);

    // `shutdown` awaits the join handles, so a timeout here proves the tasks
    // actually ended instead of being detached and leaked.
    tokio::time::timeout(Duration::from_secs(2), group.shutdown())
        .await
        .expect("writer tasks exit after their senders are dropped");
    assert_eq!(group.writer_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn separate_partitions_linger_in_parallel() {
    // The memory journal appends synchronously, so the observable signal for
    // "these are independent writer tasks" is that their linger windows
    // overlap: two partitions each linger for `linger`, and the wall clock
    // stays near one linger window instead of two.
    let journal = Arc::new(Journal::memory());
    let linger = Duration::from_millis(100);
    let group = GroupCommit::new(
        Arc::clone(&journal),
        GroupCommitConfig {
            max_batch: 8,
            linger,
        },
    );

    let orders = {
        let group = group.clone();
        tokio::spawn(async move {
            group
                .append(Topic::Orders, 0, vec![message(1)])
                .await
                .expect("orders submission")
        })
    };
    let md = {
        let group = group.clone();
        tokio::spawn(async move {
            group
                .append(Topic::Md, 0, vec![message(2)])
                .await
                .expect("md submission")
        })
    };

    let started = Instant::now();
    let orders = orders.await.expect("orders task");
    let md = md.await.expect("md task");
    let elapsed = started.elapsed();

    assert_eq!(orders.partition, 0);
    assert_eq!(md.partition, 0);
    assert_eq!(
        orders.base_offset, 0,
        "partition offsets are per (topic, partition)"
    );
    assert_eq!(md.base_offset, 0);
    assert_eq!(
        records(&journal, Topic::Orders, 0, 1).await[0].payload,
        payload(1)
    );
    assert_eq!(
        records(&journal, Topic::Md, 0, 1).await[0].payload,
        payload(2)
    );
    assert!(
        elapsed < linger * 2,
        "two writers must linger in parallel, took {elapsed:?} for a {linger:?} linger"
    );

    group.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_large_submissions_never_cross_offsets() {
    // 200 + 200 messages exceeds the 256-message cap, so the writer must split
    // them into two appends. Each submission's ack must still address its own
    // payloads: this is the silent-mis-ack case a job-count cap would miss.
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        Arc::clone(&journal),
        GroupCommitConfig {
            max_batch: 256,
            linger: Duration::from_millis(5),
        },
    );

    let first = tokio::spawn({
        let group = group.clone();
        async move {
            group
                .append(
                    Topic::Orders,
                    0,
                    (0..200u64).map(message).collect::<Vec<_>>(),
                )
                .await
                .expect("first append")
        }
    });
    let second = tokio::spawn({
        let group = group.clone();
        async move {
            group
                .append(
                    Topic::Orders,
                    0,
                    (200..400u64).map(message).collect::<Vec<_>>(),
                )
                .await
                .expect("second append")
        }
    });

    let first = first.await.expect("first task");
    let second = second.await.expect("second task");
    let stored = records(&journal, Topic::Orders, 0, 400).await;
    assert_eq!(stored.len(), 400);

    for index in 0..200u64 {
        assert_eq!(
            stored[first.base_offset as usize + index as usize].payload,
            payload(index),
            "first submission payload at its own offset"
        );
        assert_eq!(
            stored[second.base_offset as usize + index as usize].payload,
            payload(200 + index),
            "second submission payload at its own offset"
        );
    }

    let mut starts = [first.base_offset, second.base_offset];
    starts.sort_unstable();
    assert_eq!(starts, [0, 200], "the two submissions cover 0..400");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_racing_with_appends_leaves_no_writer_and_no_dropped_job() {
    // `shutdown` drains the writer map; without the shutdown flag an `append`
    // running at the same moment could spawn a fresh writer behind the drain
    // and leak it. Every append here must either succeed before shutdown or
    // come back `WriterStopped` — never drop, never fail any other way.
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());

    let mut handles = Vec::new();
    for value in 0..64u64 {
        let group = group.clone();
        handles.push(tokio::spawn(async move {
            (
                value,
                group.append(Topic::Orders, 0, vec![message(value)]).await,
            )
        }));
    }
    let shutdown = {
        let group = group.clone();
        tokio::spawn(async move { group.shutdown().await })
    };

    let mut successes = 0usize;
    for handle in handles {
        let (value, result) = handle.await.expect("submission task");
        match result {
            Ok(_) => successes += 1,
            // Refused after shutdown started: the submission was never
            // appended, which is the documented `WriterStopped` contract.
            Err(JournalError::WriterStopped) => {}
            Err(error) => panic!("submission {value} failed unexpectedly: {error}"),
        }
    }
    shutdown.await.expect("shutdown task");
    assert_eq!(
        group.writer_count(),
        0,
        "no writer may survive or be re-inserted after shutdown"
    );

    // No job was silently dropped: exactly the successful submissions were
    // appended, nothing more.
    assert_eq!(group.stats().messages, successes as u64);
    let stored = records(&journal, Topic::Orders, 0, 64).await;
    assert_eq!(stored.len(), successes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_job_is_never_reordered_or_dropped() {
    // 200 + 100 + 200 messages with a 256-message cap and a 5 ms linger.
    // The 100-message job does not fit after the first 200 and must be held;
    // the writer must not linger and accept the next 200 before it, and must
    // not overwrite the held job.
    let journal = Arc::new(Journal::memory());
    let group = GroupCommit::new(
        Arc::clone(&journal),
        GroupCommitConfig {
            max_batch: 256,
            linger: Duration::from_millis(5),
        },
    );

    let sender = tokio::spawn({
        let group = group.clone();
        async move {
            let first = group
                .append(
                    Topic::Orders,
                    0,
                    (0..200u64).map(message).collect::<Vec<_>>(),
                )
                .await
                .expect("first submission");
            let second = group
                .append(
                    Topic::Orders,
                    0,
                    (200..300u64).map(message).collect::<Vec<_>>(),
                )
                .await
                .expect("second submission");
            let third = group
                .append(
                    Topic::Orders,
                    0,
                    (300..500u64).map(message).collect::<Vec<_>>(),
                )
                .await
                .expect("third submission");
            (first, second, third)
        }
    });

    let (first, second, third) = sender.await.expect("sender task");
    let stored = records(&journal, Topic::Orders, 0, 500).await;
    assert_eq!(stored.len(), 500);

    for index in 0..200u64 {
        assert_eq!(
            stored[first.base_offset as usize + index as usize].payload,
            payload(index),
            "first submission payload at its own offset"
        );
    }
    for index in 0..100u64 {
        assert_eq!(
            stored[second.base_offset as usize + index as usize].payload,
            payload(200 + index),
            "held submission payload at its own offset"
        );
    }
    for index in 0..200u64 {
        assert_eq!(
            stored[third.base_offset as usize + index as usize].payload,
            payload(300 + index),
            "third submission payload at its own offset"
        );
    }
    assert_eq!(group.stats().messages, 500, "no message was dropped");
}
