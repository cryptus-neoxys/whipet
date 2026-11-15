// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};
use crate::ids::{Price, Symbol};

/// Operating mode. The mode changes the sequencing and acknowledgement path,
/// never the matching semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Gateway -> Iggy `orders` -> matcher -> Iggy `executions`/`md` -> consumers.
    /// Acknowledged after the Iggy write. Replayable from the log alone.
    #[default]
    Durable,
    /// Local sequencer -> matcher -> direct fanout, with Iggy write-behind.
    /// Lower latency, bounded uncommitted window. Phase 2.
    Turbo,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Turbo => "turbo",
        }
    }

    pub fn guarantee(self) -> &'static str {
        match self {
            Self::Durable => "Acknowledged after the Iggy write. Replayable from the log.",
            Self::Turbo => {
                "Acknowledged after matching. Iggy is write-behind; the in-flight window can be lost."
            }
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Mode {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "durable" => Ok(Self::Durable),
            "turbo" => Ok(Self::Turbo),
            other => Err(CoreError::InvalidValue(format!("unknown mode: {other}"))),
        }
    }
}

/// Static description of one instrument and its price ladder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentConfig {
    pub symbol: Symbol,
    /// Smallest price increment in cents. Prices are integer multiples of this.
    pub tick_size_cents: i64,
    /// Lowest ladder index (inclusive), in ticks.
    pub min_price_ticks: i64,
    /// Highest ladder index (inclusive), in ticks.
    pub max_price_ticks: i64,
    /// Order arena capacity for this book.
    pub max_orders: usize,
    /// Levels returned in a market-data snapshot per side.
    pub snapshot_depth: usize,
    /// Market orders may not trade outside this distance from the reference
    /// price. Expressed in basis points (100 bps = 1%).
    pub market_collar_bps: u32,
}

impl InstrumentConfig {
    pub fn new(
        symbol: &str,
        tick_size_cents: i64,
        min_price_ticks: i64,
        max_price_ticks: i64,
    ) -> Result<Self> {
        if tick_size_cents <= 0 {
            return Err(CoreError::InvalidValue(
                "tick_size_cents must be positive".into(),
            ));
        }
        if min_price_ticks >= max_price_ticks {
            return Err(CoreError::InvalidValue(
                "min_price_ticks must be below max_price_ticks".into(),
            ));
        }
        let ladder_len = max_price_ticks
            .checked_sub(min_price_ticks)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| CoreError::InvalidValue("price ladder length overflow".into()))?;
        if ladder_len > 5_000_000 {
            return Err(CoreError::InvalidValue(
                "price ladder exceeds the 5,000,000-level safety limit".into(),
            ));
        }
        Ok(Self {
            symbol: Symbol::new(symbol)?,
            tick_size_cents,
            min_price_ticks,
            max_price_ticks,
            max_orders: 250_000,
            snapshot_depth: 20,
            market_collar_bps: 500,
        })
    }

    pub fn ladder_len(&self) -> usize {
        (self.max_price_ticks - self.min_price_ticks + 1) as usize
    }

    pub fn validate(&self) -> Result<()> {
        if self.tick_size_cents <= 0 {
            return Err(CoreError::InvalidValue(
                "tick_size_cents must be positive".into(),
            ));
        }
        if self.min_price_ticks >= self.max_price_ticks {
            return Err(CoreError::InvalidValue(
                "min_price_ticks must be below max_price_ticks".into(),
            ));
        }
        if self.ladder_len() > 5_000_000 {
            return Err(CoreError::InvalidValue(
                "price ladder exceeds the 5,000,000-level safety limit".into(),
            ));
        }
        if self.max_orders == 0 || self.max_orders > 5_000_000 {
            return Err(CoreError::InvalidValue(
                "max_orders must be between 1 and 5,000,000".into(),
            ));
        }
        if self.snapshot_depth == 0 || self.snapshot_depth > u16::MAX as usize {
            return Err(CoreError::InvalidValue(
                "snapshot_depth must be between 1 and 65535".into(),
            ));
        }
        if self.market_collar_bps > 10_000 {
            return Err(CoreError::InvalidValue(
                "market_collar_bps must be at most 10000".into(),
            ));
        }
        Ok(())
    }

    pub fn contains_ticks(&self, price: Price) -> bool {
        price.0 >= self.min_price_ticks && price.0 <= self.max_price_ticks
    }

    /// Convert a quote-currency price in cents into a tick index.
    pub fn ticks_from_cents(&self, cents: i64) -> Result<Price> {
        if cents < 0 || cents % self.tick_size_cents != 0 {
            return Err(CoreError::InvalidValue(format!(
                "price {cents} cents is not a multiple of tick size {} cents",
                self.tick_size_cents
            )));
        }
        let ticks = cents / self.tick_size_cents;
        if ticks < self.min_price_ticks || ticks > self.max_price_ticks {
            return Err(CoreError::InvalidValue(format!(
                "price {cents} cents is outside the ladder [{}, {}]",
                self.min_price_cents(),
                self.max_price_cents()
            )));
        }
        Ok(Price(ticks))
    }

    pub fn cents_from_ticks(&self, price: Price) -> i64 {
        price.0 * self.tick_size_cents
    }

    pub fn min_price_cents(&self) -> i64 {
        self.min_price_ticks * self.tick_size_cents
    }

    pub fn max_price_cents(&self) -> i64 {
        self.max_price_ticks * self.tick_size_cents
    }

    /// A display reference used by the simulator and dashboard, in ticks.
    pub fn reference_price(&self) -> Price {
        Price(self.min_price_ticks + (self.max_price_ticks - self.min_price_ticks) / 2)
    }
}

/// Exchange-wide static configuration shared by gateway, engine, and simulator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub venue: String,
    pub partitions: u32,
    pub mode: Mode,
    pub instruments: Vec<InstrumentConfig>,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            venue: crate::VENUE.to_string(),
            partitions: 16,
            mode: Mode::Durable,
            instruments: default_instruments(),
        }
    }
}

impl ExchangeConfig {
    pub fn instrument(&self, symbol: Symbol) -> Option<&InstrumentConfig> {
        self.instruments.iter().find(|item| item.symbol == symbol)
    }

    pub fn instrument_or_err(&self, symbol: Symbol) -> Result<&InstrumentConfig> {
        self.instrument(symbol)
            .ok_or_else(|| CoreError::InvalidValue(format!("unknown instrument: {symbol}")))
    }
}

/// The three-symbol demo universe. Prices are deliberately broad ladders so a
/// scenario never runs out of room; the dashboard shows real quote currency.
pub fn default_instruments() -> Vec<InstrumentConfig> {
    vec![
        InstrumentConfig::new("BTC-USD", 100, 40_000, 90_000).expect("static config"),
        InstrumentConfig::new("ETH-USD", 10, 15_000, 45_000).expect("static config"),
        InstrumentConfig::new("SOL-USD", 1, 5_000, 35_000).expect("static config"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_convert_exactly() {
        let btc = InstrumentConfig::new("BTC-USD", 100, 40_000, 90_000).unwrap();
        assert_eq!(btc.ticks_from_cents(6_500_000).unwrap(), Price(65_000));
        assert_eq!(btc.cents_from_ticks(Price(65_000)), 6_500_000);
        assert!(btc.ticks_from_cents(6_500_050).is_err());
        assert!(btc.ticks_from_cents(100).is_err());
    }

    #[test]
    fn default_config_has_three_instruments() {
        let config = ExchangeConfig::default();
        assert_eq!(config.instruments.len(), 3);
        assert!(config.instrument(Symbol::new("BTC-USD").unwrap()).is_some());
    }

    #[test]
    fn mode_round_trips() {
        assert_eq!(Mode::from_str("durable").unwrap(), Mode::Durable);
        assert_eq!(Mode::from_str("TURBO").unwrap(), Mode::Turbo);
        assert!(Mode::from_str("fast").is_err());
    }
}
