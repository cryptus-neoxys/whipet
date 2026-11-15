// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic, seeded market simulator.
//!
//! This is a synthetic workload, not a calibrated venue model. It produces a
//! credible mix of limit orders, cancels, IOC orders, post-only orders, and
//! occasional market orders around a random-walk mid.
//!
//! # Reproducibility
//!
//! The action stream is a pure function of the [`SimConfig`] seen by
//! [`Simulator::new`]; nothing in the simulator reads the wall clock, the
//! environment, or the OS RNG. Reproducing a run requires pinning **both**
//! [`SimConfig::seed`] and [`SimConfig::run_nonce`]:
//!
//! - `(seed, run_nonce)` seeds the ChaCha20 stream, so both values decide the
//!   action sequence.
//! - the client order id is `sim-{seed:016x}-{nonce:016x}-{sequence:016x}`, so
//!   both values decide the id space and a different nonce never replays ids
//!   that a durable node's dedup table has already accepted.
//!
//! The action-stream hash in `tests/` pins one 1,000-action stream per
//! scenario, which is the regression gate for accidental stream changes.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet, VecDeque};
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
    /// Selects the action stream together with [`Self::run_nonce`]. Pinned
    /// seeds are only reproducible on the same scenario and instrument set.
    pub seed: u64,
    /// Extra entropy folded into the RNG seed and into every client order id.
    ///
    /// A restart against a durable node must change this: the dedup table
    /// remembers client order ids, so replaying a `(seed, nonce)` pair sends
    /// duplicates and every order is rejected. The default demo uses a fresh
    /// nonce; a reproducible run pins it (the CLI defaults it to `0` when
    /// `--seed` is given). Two runs are identical only when *both* values
    /// match.
    #[serde(default)]
    pub run_nonce: u64,
    /// Advisory pacing. The simulator does not rate-limit itself; the runner
    /// sleeps between calls and never reads this field.
    pub rate_per_sec: u64,
    /// Advisory run length, also unused by the simulator itself.
    pub duration_secs: u64,
    pub scenario: Scenario,
    /// Participant universe size. Names are `sim-01..=sim-N`; zero behaves as
    /// one, so at least one participant always exists.
    pub participants: u64,
    /// Empty means "every instrument in the exchange config". Unknown symbols
    /// are dropped, duplicates are collapsed, so the tracked set can be empty.
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

/// Hard cap on tracked open orders. When the cap is reached, the oldest open
/// order is dropped from the simulator's own book before the new one is
/// pushed. The dropped id is never reused, but no cancel is emitted for it, so
/// the venue may still hold it.
const OPEN_ORDER_CAPACITY: usize = 4_096;

/// Fold `(seed, run_nonce)` into one RNG seed.
///
/// This is the SplitMix64 finalizer, applied to a nonce-scaled XOR. It needs no
/// extra dependency and is a bijection on `u64`, so for a fixed nonce every
/// seed selects a distinct stream seed. Keeping the mix here (rather than
/// seeding only from `seed`) is what makes `run_nonce` affect the actions.
fn stream_seed(seed: u64, run_nonce: u64) -> u64 {
    let mut z = seed ^ run_nonce.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The simulator's client id space.
///
/// Every field is fixed-width lowercase hex, so `(seed, run_nonce, sequence)`
/// maps 1:1 onto the id: no field can absorb a neighbour's digits, and the
/// full `u64` sequence survives instead of being truncated to 32 bits.
pub fn client_order_id(seed: u64, run_nonce: u64, sequence: u64) -> String {
    format!("sim-{seed:016x}-{run_nonce:016x}-{sequence:016x}")
}

/// Clamp without the panic `i64::clamp` raises when `min > max`; a corrupt
/// ladder must surface through `check_invariants`, not abort the caller.
fn clamp_ticks(value: i64, min: i64, max: i64) -> i64 {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
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
        // A config can name a symbol twice; the state map would already collapse
        // the duplicate, so collapse the schedule too or `symbols` and `state`
        // silently disagree about how many symbols are tracked.
        let mut seen = HashSet::with_capacity(symbols.len());
        let symbols: Vec<Symbol> = symbols
            .into_iter()
            .filter(|symbol| seen.insert(*symbol))
            .collect();
        let mut state = HashMap::with_capacity(symbols.len());
        for symbol in &symbols {
            let instrument = exchange.instrument(*symbol).expect("filtered above");
            let ladder_len = instrument.ladder_len() as i64;
            // Keep the interior band inside the ladder and around the reference
            // price, so `min_ticks <= mid_ticks <= max_ticks` holds at birth even
            // on a two-tick ladder (where a fixed 10-tick margin would invert the
            // bounds and make `i64::clamp` panic). A malformed ladder (inverted
            // bounds) yields a zero margin and is caught by `check_invariants`.
            let margin = (ladder_len / 10)
                .max(10)
                .min(((ladder_len - 1) / 2).max(0))
                .max(0);
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
        let rng = ChaCha20Rng::seed_from_u64(stream_seed(config.seed, config.run_nonce));
        Self {
            config,
            rng,
            symbols,
            state,
            open: VecDeque::with_capacity(OPEN_ORDER_CAPACITY),
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

    /// Orders emitted so far. One per [`SimAction::New`], so this is also the
    /// newest sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Actions emitted so far, cancels included. `emitted - sequence` is the
    /// number of cancels produced.
    pub fn emitted(&self) -> u64 {
        self.emitted
    }

    /// Generate the next deterministic action.
    ///
    /// Returns `None` when no symbol is tracked, which means "the simulator has
    /// nothing to trade": the tracked set came back empty after filtering
    /// `config.symbols` against the exchange (or the exchange has no
    /// instruments). That is a configuration outcome, not exhaustion, so the
    /// caller must stop rather than retry, and repeated calls keep returning
    /// `None` without advancing any counter, the RNG, or the open book.
    pub fn next_action(&mut self) -> Option<SimAction> {
        if self.symbols.is_empty() {
            return None;
        }
        let symbol = self.symbols[self.symbol_cursor % self.symbols.len()];
        // Resolve the per-symbol state before touching a counter or the RNG, so
        // a missing entry can never leave a half-advanced simulator behind.
        let state = self.state.get_mut(&symbol)?;
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

        let step = self.rng.gen_range(-1..=1) + regime.drift_ticks;
        state.mid_ticks = clamp_ticks(
            state.mid_ticks.saturating_add(step),
            state.min_ticks,
            state.max_ticks,
        );
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
            Side::Buy => mid.saturating_sub(offset).max(state.min_ticks),
            Side::Sell => mid.saturating_add(offset).min(state.max_ticks),
        };
        let price_cents = price_ticks * tick;
        let qty = self.rng.gen_range(1..=25);

        self.sequence = self.sequence.saturating_add(1);
        let client_order_id =
            client_order_id(self.config.seed, self.config.run_nonce, self.sequence);
        let order_id = OrderId::derive(symbol, &client_order_id);
        let participant_index = self.rng.gen_range(1..=self.config.participants.max(1));
        let participant = format!("sim-{participant_index:02}");

        if matches!(order_type, OrderType::Limit | OrderType::PostOnly) {
            if self.open.len() >= OPEN_ORDER_CAPACITY {
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

    /// Executable definition of the simulator's invariants. `O(tracked + open)`,
    /// allocation-bounded by the open book, and never panics: a corrupted
    /// simulator is reported as `Err`, not an abort.
    ///
    /// Checked here:
    ///
    /// - `symbols` is deduped and every entry has exactly one `state` entry;
    ///   `state` holds no untracked key. An empty tracked set is legal (nothing
    ///   to trade, `next_action` returns `None` forever) but then must be fully
    ///   empty: no state, no open orders.
    /// - per symbol: `tick_size_cents > 0`, `min_ticks <= mid_ticks <=
    ///   max_ticks`. The exchange ladder is not stored, so only internal
    ///   consistency is checked; `new` derives the band from the ladder.
    /// - the open book: at most [`OPEN_ORDER_CAPACITY`] entries, every entry on
    ///   a tracked symbol, every id non-zero and unique.
    /// - counters: `sequence <= emitted`, neither at `u64::MAX` (saturation
    ///   would start repeating ids), and `emitted - sequence` cancels.
    /// - client ids: `sim-{seed:016x}-{nonce:016x}-{sequence:016x}` round-trips
    ///   for a spread of sequence values, so `(seed, nonce, sequence)` stays 1:1
    ///   and a full-width `u64` sequence cannot alias its low 32 bits.
    pub fn check_invariants(&self) -> std::result::Result<(), String> {
        let mut tracked: HashSet<Symbol> = HashSet::with_capacity(self.symbols.len());
        for symbol in &self.symbols {
            if !tracked.insert(*symbol) {
                return Err(format!("duplicate tracked symbol {symbol}"));
            }
        }
        if self.state.len() != tracked.len() {
            return Err(format!(
                "state holds {} symbols but {} are tracked",
                self.state.len(),
                tracked.len()
            ));
        }
        for symbol in self.state.keys() {
            if !tracked.contains(symbol) {
                return Err(format!("state key {symbol} is not a tracked symbol"));
            }
        }
        if tracked.is_empty() && (!self.open.is_empty() || self.sequence != 0 || self.emitted != 0)
        {
            return Err("no tracked symbol but the simulator has produced actions".into());
        }

        for (symbol, state) in &self.state {
            if state.tick_size_cents <= 0 {
                return Err(format!(
                    "{symbol} has non-positive tick size {}",
                    state.tick_size_cents
                ));
            }
            if state.min_ticks > state.max_ticks {
                return Err(format!(
                    "{symbol} has inverted band [{}, {}]",
                    state.min_ticks, state.max_ticks
                ));
            }
            if state.mid_ticks < state.min_ticks || state.mid_ticks > state.max_ticks {
                return Err(format!(
                    "{symbol} mid {} is outside [{}, {}]",
                    state.mid_ticks, state.min_ticks, state.max_ticks
                ));
            }
        }

        if self.open.len() > OPEN_ORDER_CAPACITY {
            return Err(format!(
                "open book holds {} orders, above the {OPEN_ORDER_CAPACITY} cap",
                self.open.len()
            ));
        }
        let mut ids = HashSet::with_capacity(self.open.len());
        for order in &self.open {
            if !tracked.contains(&order.symbol) {
                return Err(format!(
                    "open order {} sits on untracked symbol {}",
                    order.order_id.to_hex(),
                    order.symbol
                ));
            }
            if order.order_id.0 == 0 {
                return Err(format!("open order on {} has a zero id", order.symbol));
            }
            if !ids.insert(order.order_id) {
                return Err(format!(
                    "open order id {} appears twice",
                    order.order_id.to_hex()
                ));
            }
        }

        if self.sequence > self.emitted {
            return Err(format!(
                "sequence {} is ahead of emitted {}",
                self.sequence, self.emitted
            ));
        }
        if self.sequence == u64::MAX || self.emitted == u64::MAX {
            return Err("action counters are saturated".into());
        }

        // The id must survive a round trip for every field width, including the
        // 32-bit boundary where the old `sequence as u32` truncation wrapped.
        let mut samples = vec![
            0,
            1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            u64::MAX - 1,
        ];
        if !samples.contains(&self.sequence) {
            samples.push(self.sequence);
        }
        let mut seen = HashSet::with_capacity(samples.len());
        for sequence in samples {
            let id = client_order_id(self.config.seed, self.config.run_nonce, sequence);
            if !seen.insert(id.clone()) {
                return Err(format!("client id {id} derives twice"));
            }
            let fields = id.split('-').collect::<Vec<_>>();
            let parsed = match fields.as_slice() {
                ["sim", seed, nonce, sequence] => u64::from_str_radix(seed, 16)
                    .ok()
                    .zip(u64::from_str_radix(nonce, 16).ok())
                    .zip(u64::from_str_radix(sequence, 16).ok())
                    .map(|((seed, nonce), sequence)| (seed, nonce, sequence)),
                _ => None,
            };
            if parsed != Some((self.config.seed, self.config.run_nonce, sequence)) {
                return Err(format!(
                    "client id {id} does not round-trip to ({:#x}, {:#x}, {:#x})",
                    self.config.seed, self.config.run_nonce, sequence
                ));
            }
        }

        Ok(())
    }
}
