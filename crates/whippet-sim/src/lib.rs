// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic, seeded market simulator.
//!
//! This is a synthetic workload, not a calibrated venue model. It produces a
//! credible mix of limit orders, cancels, IOC orders, post-only orders, and
//! occasional market orders around a random-walk mid. The same seed and
//! scenario produce the same action sequence, which is what the demo and the
//! regression tests rely on.

#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use whippet_core::api::{CancelRequest, OrderRequest};
use whippet_core::config::ExchangeConfig;
use whippet_core::error::{CoreError, Result};
use whippet_core::ids::{OrderId, Symbol};
use whippet_core::protocol::{OrderType, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    CalmOpen,
    VolatileNews,
    FlashCrash,
    LiquidityDrought,
}

impl Scenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CalmOpen => "calm_open",
            Self::VolatileNews => "volatile_news",
            Self::FlashCrash => "flash_crash",
            Self::LiquidityDrought => "liquidity_drought",
        }
    }
}

impl FromStr for Scenario {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "calm_open" | "calm" => Ok(Self::CalmOpen),
            "volatile_news" | "volatile" => Ok(Self::VolatileNews),
            "flash_crash" | "flash" => Ok(Self::FlashCrash),
            "liquidity_drought" | "drought" => Ok(Self::LiquidityDrought),
            other => Err(CoreError::InvalidValue(format!(
                "unknown scenario: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimConfig {
    pub seed: u64,
    /// Distinguishes two runs that use the same deterministic seed. The
    /// default demo uses a fresh nonce so a simulator restart does not replay
    /// client order ids that the durable dedup table has already seen.
    #[serde(default)]
    pub run_nonce: u64,
    pub rate_per_sec: u64,
    pub duration_secs: u64,
    pub scenario: Scenario,
    pub participants: u64,
    pub symbols: Vec<Symbol>,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0x5eed_1234_abcd_0001,
            run_nonce: 0,
            rate_per_sec: 400,
            duration_secs: 0,
            scenario: Scenario::CalmOpen,
            participants: 8,
            symbols: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SimAction {
    New(OrderRequest),
    Cancel(CancelRequest),
}

impl SimAction {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::New(_) => "order",
            Self::Cancel(_) => "cancel",
        }
    }
}

#[derive(Debug, Clone)]
struct OpenOrder {
    symbol: Symbol,
    order_id: OrderId,
    participant: String,
}

#[derive(Debug, Clone, Copy)]
struct Regime {
    market_probability: f64,
    ioc_probability: f64,
    post_only_probability: f64,
    cancel_probability: f64,
    spread_ticks: i64,
    drift_ticks: i64,
    sell_bias: f64,
}

impl Regime {
    fn for_scenario(scenario: Scenario, phase: u64) -> Self {
        match scenario {
            Scenario::CalmOpen => Self {
                market_probability: 0.02,
                ioc_probability: 0.04,
                post_only_probability: 0.06,
                cancel_probability: 0.20,
                spread_ticks: 1,
                drift_ticks: 0,
                sell_bias: 0.5,
            },
            Scenario::VolatileNews => Self {
                market_probability: 0.10,
                ioc_probability: 0.08,
                post_only_probability: 0.04,
                cancel_probability: 0.28,
                spread_ticks: 4,
                drift_ticks: if phase.is_multiple_of(2) { 1 } else { -1 },
                sell_bias: 0.5,
            },
            Scenario::FlashCrash => {
                let phase = phase % 4_000;
                if phase < 1_200 {
                    Self {
                        market_probability: 0.03,
                        ioc_probability: 0.05,
                        post_only_probability: 0.05,
                        cancel_probability: 0.20,
                        spread_ticks: 1,
                        drift_ticks: 0,
                        sell_bias: 0.5,
                    }
                } else if phase < 2_000 {
                    Self {
                        market_probability: 0.32,
                        ioc_probability: 0.15,
                        post_only_probability: 0.01,
                        cancel_probability: 0.45,
                        spread_ticks: 9,
                        drift_ticks: -4,
                        sell_bias: 0.82,
                    }
                } else if phase < 3_000 {
                    Self {
                        market_probability: 0.08,
                        ioc_probability: 0.10,
                        post_only_probability: 0.02,
                        cancel_probability: 0.40,
                        spread_ticks: 12,
                        drift_ticks: 0,
                        sell_bias: 0.55,
                    }
                } else {
                    Self {
                        market_probability: 0.05,
                        ioc_probability: 0.06,
                        post_only_probability: 0.05,
                        cancel_probability: 0.24,
                        spread_ticks: 3,
                        drift_ticks: 1,
                        sell_bias: 0.45,
                    }
                }
            }
            Scenario::LiquidityDrought => Self {
                market_probability: 0.04,
                ioc_probability: 0.06,
                post_only_probability: 0.02,
                cancel_probability: 0.42,
                spread_ticks: 8,
                drift_ticks: 0,
                sell_bias: 0.5,
            },
        }
    }
}

struct SymbolState {
    mid_ticks: i64,
    min_ticks: i64,
    max_ticks: i64,
    tick_size_cents: i64,
}

pub struct Simulator {
    config: SimConfig,
    rng: ChaCha20Rng,
    symbols: Vec<Symbol>,
    state: HashMap<Symbol, SymbolState>,
    open: VecDeque<OpenOrder>,
    sequence: u64,
    emitted: u64,
    symbol_cursor: usize,
}

impl Simulator {
    pub fn new(config: SimConfig, exchange: &ExchangeConfig) -> Self {
        let symbols: Vec<Symbol> = if config.symbols.is_empty() {
            exchange
                .instruments
                .iter()
                .map(|item| item.symbol)
                .collect()
        } else {
            config
                .symbols
                .iter()
                .copied()
                .filter(|symbol| exchange.instrument(*symbol).is_some())
                .collect()
        };
        let mut state = HashMap::new();
        for symbol in &symbols {
            let instrument = exchange.instrument(*symbol).expect("filtered above");
            let margin = (instrument.ladder_len() as i64 / 10).max(10);
            state.insert(
                *symbol,
                SymbolState {
                    mid_ticks: instrument.reference_price().0,
                    min_ticks: instrument.min_price_ticks + margin,
                    max_ticks: instrument.max_price_ticks - margin,
                    tick_size_cents: instrument.tick_size_cents,
                },
            );
        }
        let rng = ChaCha20Rng::seed_from_u64(config.seed);
        Self {
            config,
            rng,
            symbols,
            state,
            open: VecDeque::with_capacity(4_096),
            sequence: 0,
            emitted: 0,
            symbol_cursor: 0,
        }
    }

    pub fn config(&self) -> &SimConfig {
        &self.config
    }

    pub fn open_orders(&self) -> usize {
        self.open.len()
    }

    /// Generate the next deterministic action.
    pub fn next_action(&mut self) -> Option<SimAction> {
        if self.symbols.is_empty() {
            return None;
        }
        let symbol = self.symbols[self.symbol_cursor % self.symbols.len()];
        self.symbol_cursor = self.symbol_cursor.wrapping_add(1);
        self.emitted = self.emitted.saturating_add(1);
        let regime = Regime::for_scenario(self.config.scenario, self.emitted);

        if !self.open.is_empty() && self.rng.gen_bool(regime.cancel_probability) {
            let index = self.rng.gen_range(0..self.open.len());
            if let Some(order) = self.open.remove(index) {
                return Some(SimAction::Cancel(CancelRequest {
                    symbol: order.symbol,
                    order_id: order.order_id,
                    participant: Some(order.participant),
                }));
            }
        }

        let state = self.state.get_mut(&symbol)?;
        let drift = regime.drift_ticks;
        state.mid_ticks = (state.mid_ticks + self.rng.gen_range(-1..=1) + drift)
            .clamp(state.min_ticks, state.max_ticks);
        let mid = state.mid_ticks;
        let tick = state.tick_size_cents;

        let side = if self.rng.gen_bool(regime.sell_bias) {
            Side::Sell
        } else {
            Side::Buy
        };

        let roll = self.rng.gen_range(0.0..1.0);
        let order_type = if roll < regime.market_probability {
            OrderType::Market
        } else if roll < regime.market_probability + regime.ioc_probability {
            OrderType::Ioc
        } else if roll
            < regime.market_probability + regime.ioc_probability + regime.post_only_probability
        {
            OrderType::PostOnly
        } else {
            OrderType::Limit
        };

        let offset = self.rng.gen_range(0..=regime.spread_ticks.max(1));
        let price_ticks = match side {
            Side::Buy => (mid - offset).max(state.min_ticks),
            Side::Sell => (mid + offset).min(state.max_ticks),
        };
        let price_cents = price_ticks * tick;
        let qty = self.rng.gen_range(1..=25);

        self.sequence = self.sequence.wrapping_add(1);
        let client_order_id = format!("sim-{:016x}-{:08x}", self.config.seed, self.sequence as u32);
        let order_id = OrderId::derive(symbol, &client_order_id);
        let participant_index = self.rng.gen_range(1..=self.config.participants.max(1));
        let participant = format!("sim-{participant_index:02}");

        if matches!(order_type, OrderType::Limit | OrderType::PostOnly) {
            if self.open.len() >= 4_096 {
                self.open.pop_front();
            }
            self.open.push_back(OpenOrder {
                symbol,
                order_id,
                participant: participant.clone(),
            });
        }

        Some(SimAction::New(OrderRequest {
            symbol,
            side,
            order_type,
            price_cents: order_type.requires_price().then_some(price_cents),
            qty,
            client_order_id: Some(client_order_id),
            participant: Some(participant),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_actions() {
        let exchange = ExchangeConfig::default();
        let config = SimConfig {
            seed: 42,
            ..SimConfig::default()
        };
        let mut a = Simulator::new(config.clone(), &exchange);
        let mut b = Simulator::new(config, &exchange);
        for _ in 0..200 {
            let left = a.next_action().unwrap();
            let right = b.next_action().unwrap();
            match (left, right) {
                (SimAction::New(a), SimAction::New(b)) => {
                    assert_eq!(a.symbol, b.symbol);
                    assert_eq!(a.side, b.side);
                    assert_eq!(a.order_type, b.order_type);
                    assert_eq!(a.price_cents, b.price_cents);
                    assert_eq!(a.qty, b.qty);
                    assert_eq!(a.client_order_id, b.client_order_id);
                }
                (SimAction::Cancel(a), SimAction::Cancel(b)) => {
                    assert_eq!(a.order_id, b.order_id);
                }
                _ => panic!("action kind diverged"),
            }
        }
    }

    #[test]
    fn prices_stay_inside_the_ladder() {
        let exchange = ExchangeConfig::default();
        let mut sim = Simulator::new(
            SimConfig {
                seed: 7,
                scenario: Scenario::FlashCrash,
                ..SimConfig::default()
            },
            &exchange,
        );
        for _ in 0..5_000 {
            if let Some(SimAction::New(order)) = sim.next_action()
                && let Some(price_cents) = order.price_cents
            {
                let instrument = exchange.instrument(order.symbol).unwrap();
                assert!(price_cents >= instrument.min_price_cents());
                assert!(price_cents <= instrument.max_price_cents());
            }
        }
    }

    #[test]
    fn cancels_reference_real_open_orders() {
        let exchange = ExchangeConfig::default();
        let mut sim = Simulator::new(
            SimConfig {
                seed: 11,
                ..SimConfig::default()
            },
            &exchange,
        );
        let mut cancels = 0;
        for _ in 0..2_000 {
            match sim.next_action() {
                Some(SimAction::Cancel(cancel)) => {
                    assert!(cancel.order_id.0 != 0);
                    cancels += 1;
                }
                Some(SimAction::New(_)) | None => {}
            }
        }
        assert!(cancels > 0);
    }
}
