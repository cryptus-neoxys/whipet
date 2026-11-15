// Determinism and client-order-id gate for the simulator.

mod common;

use std::collections::HashSet;
use std::thread::sleep;
use std::time::Duration;

use common::{
    NONCE, SCENARIOS, SEED, config, encode, expected_client_order_id, simulator, stream_hash, take,
};
use whippet_sim::{Scenario, SimAction, SimConfig, client_order_id};

/// Golden replay gate: one 1,000-action stream per scenario at a pinned
/// `(seed, nonce)`. If this fails, either the stream changed on purpose (update
/// every pin and say why) or a refactor silently perturbed the RNG.
///
/// The pinned values were produced by this test itself from the first
/// implementation that mixed `run_nonce` into the RNG seed and widened the
/// client id; they are a regression pin, not an external specification.
#[test]
fn action_stream_hashes_are_pinned() {
    let expected: [(Scenario, u64); 4] = [
        (Scenario::CalmOpen, 0x205d_596b_421e_6bba),
        (Scenario::VolatileNews, 0x3434_6c55_35ad_ba1d),
        (Scenario::FlashCrash, 0x1602_388e_c2af_0af7),
        (Scenario::LiquidityDrought, 0xf571_5c81_db3f_9726),
    ];
    for (scenario, hash) in expected {
        let mut sim = simulator(config(scenario));
        let actions = take(&mut sim, 1_000);
        assert_eq!(actions.len(), 1_000, "{} ran dry", scenario.as_str());
        let actual = stream_hash(&actions);
        assert_eq!(
            actual,
            hash,
            "pinned action stream drifted for {} (seed {SEED:#018x}, nonce {NONCE:#018x}): {actual:#018x}",
            scenario.as_str(),
        );
    }
}

#[test]
fn same_seed_and_nonce_produce_byte_identical_actions() {
    for scenario in SCENARIOS {
        let mut left = simulator(config(scenario));
        let mut right = simulator(config(scenario));
        for index in 0..2_000 {
            let a = left.next_action().expect("tracked symbols");
            let b = right.next_action().expect("tracked symbols");
            assert_eq!(
                encode(&a),
                encode(&b),
                "{} diverged at action {index}",
                scenario.as_str()
            );
            assert_eq!(left.open_orders(), right.open_orders());
            assert_eq!(left.sequence(), right.sequence());
            assert_eq!(left.emitted(), right.emitted());
        }
        assert_eq!(left.check_invariants(), Ok(()));
    }
}

#[test]
fn run_nonce_changes_the_stream_and_the_ids() {
    let mut base = config(Scenario::CalmOpen);
    base.seed = 77;
    base.run_nonce = 0;
    let mut other = base.clone();
    other.run_nonce = 1;

    let mut a = simulator(base.clone());
    let mut b = simulator(other.clone());
    let left = take(&mut a, 2_000);
    let right = take(&mut b, 2_000);
    assert_ne!(
        stream_hash(&left),
        stream_hash(&right),
        "a different nonce must select a different stream"
    );

    let ids = |actions: &[SimAction]| -> HashSet<String> {
        actions
            .iter()
            .filter_map(|action| match action {
                SimAction::New(order) => order.client_order_id.clone(),
                SimAction::Cancel(_) => None,
            })
            .collect()
    };
    let left_ids = ids(&left);
    let right_ids = ids(&right);
    assert!(!left_ids.is_empty());
    assert!(
        left_ids.is_disjoint(&right_ids),
        "the two nonces must not share a single client order id"
    );

    // The first id pins the exact shape and both knobs.
    let Some(SimAction::New(first)) = left.first() else {
        panic!("the stream must start with an order");
    };
    let expected_first = expected_client_order_id(77, 0, 1);
    assert_eq!(
        first.client_order_id.as_deref(),
        Some(expected_first.as_str())
    );
    assert_eq!(
        client_order_id(77, 1, 1),
        expected_client_order_id(77, 1, 1),
        "the public id derivation must match the pinned format"
    );
}

#[test]
fn client_order_ids_are_globally_unique() {
    let mut sim = simulator(config(Scenario::CalmOpen));
    let actions = take(&mut sim, 100_000);
    let mut ids = HashSet::with_capacity(100_000);
    let mut orders = 0u64;
    for action in &actions {
        if let SimAction::New(order) = action {
            let id = order
                .client_order_id
                .as_deref()
                .expect("sim always sets an id");
            assert_eq!(id.len(), 54, "id {id} is not 3 fixed-width hex fields");
            assert_eq!(
                id,
                expected_client_order_id(SEED, NONCE, orders + 1),
                "id is not the pinned derivation"
            );
            assert!(ids.insert(id.to_string()), "duplicate client id {id}");
            orders += 1;
        }
    }
    assert_eq!(orders, sim.sequence());
    assert!(orders > 50_000, "the run must emit a lot of orders");
}

#[test]
fn sequence_beyond_32_bits_does_not_alias() {
    // The old derivation formatted `sequence as u32`, so these pairs collided.
    let width_boundary = u64::from(u32::MAX) + 1;
    for (left, right) in [
        (0, width_boundary),
        (1, width_boundary + 1),
        (u64::from(u32::MAX), u64::MAX),
    ] {
        assert_ne!(
            client_order_id(SEED, NONCE, left),
            client_order_id(SEED, NONCE, right),
            "sequences {left} and {right} must not share an id"
        );
    }
    assert_eq!(
        client_order_id(SEED, NONCE, width_boundary),
        expected_client_order_id(SEED, NONCE, width_boundary)
    );
}

#[test]
fn wall_clock_gaps_do_not_change_actions() {
    let mut baseline = simulator(config(Scenario::FlashCrash));
    let fast = take(&mut baseline, 300);

    let mut delayed = simulator(config(Scenario::FlashCrash));
    let mut slow = take(&mut delayed, 50);
    sleep(Duration::from_millis(150));
    slow.extend(take(&mut delayed, 250));

    assert_eq!(stream_hash(&fast), stream_hash(&slow));
    for (index, (a, b)) in fast.iter().zip(slow.iter()).enumerate() {
        assert_eq!(encode(a), encode(b), "action {index} depends on wall time");
    }
    assert_eq!(delayed.check_invariants(), Ok(()));
}

#[test]
fn advisory_pacing_fields_do_not_change_the_stream() {
    // rate_per_sec and duration_secs pace the runner; they must not perturb the
    // stream, or two runs of the same length would disagree.
    let mut fast = simulator(SimConfig {
        rate_per_sec: 10,
        duration_secs: 1,
        ..config(Scenario::VolatileNews)
    });
    let mut slow = simulator(SimConfig {
        rate_per_sec: 1_000_000,
        duration_secs: 86_400,
        ..config(Scenario::VolatileNews)
    });
    assert_eq!(
        stream_hash(&take(&mut fast, 500)),
        stream_hash(&take(&mut slow, 500))
    );
}
