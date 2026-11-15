// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Per-IP ingress token bucket shared by HTTP and TCP.
//!
//! One second of burst, refilled continuously. This is an ingress guard, not a
//! quota system. The table is sharded so a request does not take one global
//! lock, and the stale-entry sweep is rate-limited so a full table cannot turn
//! every new address into an O(n) scan under the lock.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Upper bound on tracked source addresses. The limiter fails closed (429)
/// when the table is full of live entries, so a spoofed-IP flood cannot grow
/// memory without bound.
const MAX_TRACKED_IPS: usize = 10_000;

/// Buckets idle longer than this are dropped when a shard is full.
const BUCKET_IDLE: Duration = Duration::from_secs(60);

/// At most one stale sweep per shard in this window.
const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

/// Power of two so the shard index is a mask.
const SHARDS: usize = 16;
const PER_SHARD: usize = MAX_TRACKED_IPS / SHARDS;

struct Bucket {
    tokens: f64,
    last_seen: Instant,
}

struct Shard {
    buckets: HashMap<IpAddr, Bucket>,
    last_sweep: Instant,
}

/// A per-IP token bucket. One instance is shared by every ingress transport so
/// a client cannot get a second budget by switching from HTTP to TCP.
pub struct RateLimiter {
    per_sec: u32,
    shards: [Mutex<Shard>; SHARDS],
}

impl RateLimiter {
    pub fn new(per_sec: u32) -> Self {
        let now = Instant::now();
        Self {
            per_sec,
            shards: std::array::from_fn(|_| {
                Mutex::new(Shard {
                    buckets: HashMap::new(),
                    last_sweep: now,
                })
            }),
        }
    }

    /// `true` when one token is available for this address.
    pub(crate) fn check(&self, ip: IpAddr) -> bool {
        self.charge(ip, 1)
    }

    /// Consume `cost` tokens for this address. A batch is charged for its
    /// orders, not once per request: `check` alone would let a 256-order batch
    /// through for one token and make the limit decorative.
    pub(crate) fn charge(&self, ip: IpAddr, cost: u32) -> bool {
        if self.per_sec == 0 {
            return true;
        }
        let ip = canonical(ip);
        let cost = f64::from(cost.max(1));
        let shard = &self.shards[shard_index(ip)];
        let mut shard = shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        let rate = f64::from(self.per_sec);

        if let Some(bucket) = shard.buckets.get_mut(&ip) {
            let elapsed = now.saturating_duration_since(bucket.last_seen);
            bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * rate).min(rate);
            bucket.last_seen = now;
            if bucket.tokens >= cost {
                bucket.tokens -= cost;
                return true;
            }
            return false;
        }

        // A miss at capacity: sweep at most once per interval, then fail
        // closed if the shard is still full of live buckets.
        if shard.buckets.len() >= PER_SHARD {
            if now.saturating_duration_since(shard.last_sweep) >= SWEEP_INTERVAL {
                shard.buckets.retain(|_, bucket| {
                    now.saturating_duration_since(bucket.last_seen) < BUCKET_IDLE
                });
                shard.last_sweep = now;
            }
            if shard.buckets.len() >= PER_SHARD {
                return false;
            }
        }

        if rate < cost {
            return false;
        }
        shard.buckets.insert(
            ip,
            Bucket {
                tokens: rate - cost,
                last_seen: now,
            },
        );
        true
    }
}

/// Treat an IPv4-mapped IPv6 address as the IPv4 address, so the two forms do
/// not get separate budgets. Also used for the WebSocket per-IP table, where
/// `::ffff:1.2.3.4` and `1.2.3.4` must be the same peer.
pub(crate) fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

fn shard_index(ip: IpAddr) -> usize {
    // FNV-1a over the address bytes; cheap and stable.
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    match ip {
        IpAddr::V4(v4) => {
            for byte in v4.octets() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        IpAddr::V6(v6) => {
            for byte in v6.octets() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
    }
    (hash as usize) & (SHARDS - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(value: &str) -> IpAddr {
        value.parse().expect("valid test address")
    }

    #[test]
    fn disabled_limiter_allows_everything() {
        let limiter = RateLimiter::new(0);
        for _ in 0..1_000 {
            assert!(limiter.check(ip("127.0.0.1")));
        }
    }

    #[test]
    fn a_bucket_refills_over_time() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.check(ip("10.0.0.1")));
        assert!(!limiter.check(ip("10.0.0.1")), "burst is spent");
        std::thread::sleep(Duration::from_millis(1_050));
        assert!(limiter.check(ip("10.0.0.1")), "one token refilled");
    }

    #[test]
    fn distinct_addresses_have_distinct_budgets() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.check(ip("10.0.0.1")));
        assert!(limiter.check(ip("10.0.0.2")));
        assert!(!limiter.check(ip("10.0.0.1")));
    }

    #[test]
    fn shard_index_is_stable_and_in_range() {
        for address in ["1.2.3.4", "255.255.255.255", "::1", "2001:db8::1"] {
            let index = shard_index(ip(address));
            assert!(index < SHARDS);
            assert_eq!(index, shard_index(ip(address)));
        }
    }
}
