// Shared helpers for the whippet-sim integration tests.
#![allow(dead_code)]

use whippet_core::config::ExchangeConfig;
use whippet_sim::{Scenario, SimAction, SimConfig, Simulator};

/// Fixed seed for the pinned-stream gate.
pub const SEED: u64 = 0x5eed_0000_0000_0001;
/// Fixed nonce for the pinned-stream gate.
pub const NONCE: u64 = 0x0000_0000_0000_0001;

pub const SCENARIOS: [Scenario; 4] = [
    Scenario::CalmOpen,
    Scenario::VolatileNews,
    Scenario::FlashCrash,
    Scenario::LiquidityDrought,
];

pub fn config(scenario: Scenario) -> SimConfig {
    SimConfig {
        seed: SEED,
        run_nonce: NONCE,
        scenario,
        ..SimConfig::default()
    }
}

pub fn simulator(config: SimConfig) -> Simulator {
    let exchange = ExchangeConfig::default();
    Simulator::new(config, &exchange)
}

pub fn take(sim: &mut Simulator, count: usize) -> Vec<SimAction> {
    (0..count).filter_map(|_| sim.next_action()).collect()
}

/// Canonical byte encoding of one action. Field order and separators are part
/// of the golden-hash contract: changing either invalidates every pin.
pub fn encode(action: &SimAction) -> Vec<u8> {
    match action {
        SimAction::New(order) => {
            let price = match order.price_cents {
                Some(price) => price.to_string(),
                None => "none".to_string(),
            };
            format!(
                "new|{}|{}|{}|{}|{}|{}|{}",
                order.symbol,
                order.side.as_str(),
                order.order_type.as_str(),
                price,
                order.qty,
                order.client_order_id.as_deref().unwrap_or(""),
                order.participant.as_deref().unwrap_or(""),
            )
            .into_bytes()
        }
        SimAction::Cancel(cancel) => format!(
            "cancel|{}|{}|{}",
            cancel.symbol,
            cancel.order_id.to_hex(),
            cancel.participant.as_deref().unwrap_or(""),
        )
        .into_bytes(),
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// FNV-1a over the canonical action bytes, one record separator per action,
/// finalised with the SplitMix64 mixer so short streams still spread.
pub fn stream_hash(actions: &[SimAction]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for action in actions {
        for byte in encode(action) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    splitmix64(hash)
}

/// `(seed, nonce, sequence)` -> the id the simulator would send. Kept in the
/// tests so the format is pinned independently of the implementation.
pub fn expected_client_order_id(seed: u64, run_nonce: u64, sequence: u64) -> String {
    format!("sim-{seed:016x}-{run_nonce:016x}-{sequence:016x}")
}
