// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Args;
use whippet_core::config::{ExchangeConfig, Mode, default_instruments};
use whippet_core::error::CoreError;
use whippet_core::ids::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalKind {
    Iggy,
    Memory,
}

impl JournalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Iggy => "iggy",
            Self::Memory => "memory",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value.to_ascii_lowercase().as_str() {
            "iggy" => Ok(Self::Iggy),
            "memory" => Ok(Self::Memory),
            other => Err(ConfigError::Invalid(format!(
                "unknown journal backend: {other} (expected iggy or memory)"
            ))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}")]
    Invalid(String),
    #[error("core error: {0}")]
    Core(#[from] CoreError),
}

/// Node configuration. Every value has a demo-safe default; Compose sets the
/// container paths explicitly.
#[derive(Args, Debug, Clone)]
pub struct NodeArgs {
    /// HTTP/WebSocket listen address.
    #[arg(long, env = "WHIPPET_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: String,
    /// Iggy TCP address.
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    /// Iggy username. The local demo uses the bootstrap account; Phase 2 moves
    /// services to scoped PATs.
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// `iggy` for the real demo, `memory` for tests and brokerless development.
    #[arg(long, env = "WHIPPET_JOURNAL", default_value = "iggy")]
    pub journal: String,
    /// `durable` is the Phase 1 path. `turbo` is Phase 2.
    #[arg(long, env = "WHIPPET_MODE", default_value = "durable")]
    pub mode: String,
    /// Comma-separated symbols from the built-in instrument set.
    #[arg(
        long,
        env = "WHIPPET_SYMBOLS",
        default_value = "BTC-USD,ETH-USD,SOL-USD"
    )]
    pub symbols: String,
    #[arg(long, env = "WHIPPET_PARTITIONS", default_value_t = 16)]
    pub partitions: u32,
    #[arg(long, env = "WHIPPET_SNAPSHOT_DIR", default_value = "data/snapshots")]
    pub snapshot_dir: PathBuf,
    #[arg(long, env = "WHIPPET_SNAPSHOT_EVERY", default_value_t = 10_000)]
    pub snapshot_every: u64,
    #[arg(
        long,
        env = "WHIPPET_DASHBOARD_DIR",
        default_value = "apps/dashboard/dist"
    )]
    pub dashboard_dir: PathBuf,
    #[arg(long, env = "WHIPPET_VENUE", default_value = "whippet")]
    pub venue: String,
    /// Optional build label shown on the dashboard.
    #[arg(long, env = "WHIPPET_BUILD", default_value = "dev")]
    pub build: String,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub http_addr: SocketAddr,
    pub iggy_addr: String,
    pub iggy_user: String,
    pub iggy_password: String,
    pub journal: JournalKind,
    pub mode: Mode,
    pub exchange: ExchangeConfig,
    pub snapshot_dir: PathBuf,
    pub snapshot_every: u64,
    pub dashboard_dir: PathBuf,
    pub build: String,
}

impl NodeConfig {
    pub fn from_args(args: NodeArgs) -> Result<Self, ConfigError> {
        let http_addr: SocketAddr = args.http_addr.parse().map_err(|_| {
            ConfigError::Invalid(format!("invalid HTTP address: {}", args.http_addr))
        })?;
        let journal = JournalKind::parse(&args.journal)?;
        let mode: Mode = args.mode.parse()?;
        if mode == Mode::Turbo {
            return Err(ConfigError::Invalid(
                "turbo mode is Phase 2; the durable slice is the default and only enabled path in this release"
                    .into(),
            ));
        }
        if args.partitions == 0 || args.partitions > 256 {
            return Err(ConfigError::Invalid(
                "partitions must be between 1 and 256".into(),
            ));
        }

        let requested: Vec<Symbol> = args
            .symbols
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Symbol::new)
            .collect::<Result<_, _>>()?;
        if requested.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one symbol is required".into(),
            ));
        }
        let mut instruments = Vec::new();
        for instrument in default_instruments() {
            if requested.contains(&instrument.symbol) {
                instruments.push(instrument);
            }
        }
        if instruments.len() != requested.len() {
            let known: Vec<_> = default_instruments()
                .iter()
                .map(|instrument| instrument.symbol.to_string())
                .collect();
            return Err(ConfigError::Invalid(format!(
                "unknown symbol in {:?}; known symbols: {}",
                args.symbols,
                known.join(", ")
            )));
        }

        Ok(Self {
            http_addr,
            iggy_addr: args.iggy_addr,
            iggy_user: args.iggy_user,
            iggy_password: args.iggy_password,
            journal,
            mode,
            exchange: ExchangeConfig {
                venue: args.venue,
                partitions: args.partitions,
                mode,
                instruments,
            },
            snapshot_dir: args.snapshot_dir,
            snapshot_every: args.snapshot_every.max(1),
            dashboard_dir: args.dashboard_dir,
            build: args.build,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        node: NodeArgs,
    }

    #[test]
    fn defaults_are_demo_safe() {
        let cli = Cli::parse_from(["test"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.mode, Mode::Durable);
        assert_eq!(config.exchange.partitions, 16);
        assert_eq!(config.exchange.instruments.len(), 3);
    }

    #[test]
    fn turbo_is_rejected_with_a_clear_message() {
        let cli = Cli::parse_from(["test", "--mode", "turbo"]);
        let error = NodeConfig::from_args(cli.node).unwrap_err();
        assert!(error.to_string().contains("Phase 2"));
    }

    #[test]
    fn unknown_symbol_is_rejected() {
        let cli = Cli::parse_from(["test", "--symbols", "DOGE-USD"]);
        assert!(NodeConfig::from_args(cli.node).is_err());
    }
}
