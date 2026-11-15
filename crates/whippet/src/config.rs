// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Args;
use whippet_core::config::{ExchangeConfig, Mode, default_instruments};
use whippet_core::error::CoreError;
use whippet_core::ids::Symbol;

use crate::turbo::TurboConfig;

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
#[derive(Args, Clone)]
pub struct NodeArgs {
    /// HTTP/WebSocket listen address.
    #[arg(long, env = "WHIPPET_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: String,
    /// Binary TCP order-ingress listen address. `off`, `none`, or an empty
    /// value disables the listener; the HTTP path stays available either way.
    #[arg(long, env = "WHIPPET_TCP_ADDR", default_value = "0.0.0.0:8081")]
    pub tcp_addr: String,
    /// Iggy TCP address.
    #[arg(long, env = "WHIPPET_IGGY_ADDR", default_value = "127.0.0.1:8090")]
    pub iggy_addr: String,
    /// Iggy username. The local demo uses the bootstrap account; Phase 2 moves
    /// services to scoped PATs.
    #[arg(long, env = "WHIPPET_IGGY_USER", default_value = "iggy")]
    pub iggy_user: String,
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD", default_value = "iggy")]
    pub iggy_password: String,
    /// Read the Iggy password from a file instead of the environment. Compose
    /// mounts the password as a secret and points this at `/run/secrets/...`.
    #[arg(long, env = "WHIPPET_IGGY_PASSWORD_FILE")]
    pub iggy_password_file: Option<PathBuf>,
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
    /// Optional bearer token for order ingress and cancel. When unset, those
    /// endpoints accept anonymous requests: a demo convenience, not a
    /// deployment posture. Set this for anything reachable by another host.
    #[arg(long, env = "WHIPPET_API_TOKEN")]
    pub api_token: Option<String>,
    /// Participant name assigned to authenticated ingress. Requests may omit
    /// `participant`; supplying a different one is rejected with 403.
    #[arg(long, env = "WHIPPET_API_PARTICIPANT", default_value = "api")]
    pub api_participant: String,
    /// Per-IP ingress token bucket, in requests per second. `0` disables the
    /// limiter, which is the single-node demo default.
    #[arg(long, env = "WHIPPET_RATE_LIMIT_PER_SEC", default_value_t = 0)]
    pub rate_limit_per_sec: u32,
    /// Per-IP WebSocket subscriber sub-limit. `0` disables it; the global cap
    /// still applies. The fanout benchmark sets 0 because all subscribers come
    /// from one host.
    #[arg(long, env = "WHIPPET_WS_PER_IP", default_value_t = 0)]
    pub ws_per_ip: usize,
    /// Turbo only: commands allowed in one shard's uncommitted window.
    #[arg(
        long,
        env = "WHIPPET_TURBO_WINDOW_COMMANDS",
        default_value_t = crate::turbo::DEFAULT_WINDOW_COMMANDS
    )]
    pub turbo_window_commands: usize,
    /// Turbo only: maximum age of the oldest unjournaled command, in ms.
    #[arg(
        long,
        env = "WHIPPET_TURBO_WINDOW_MS",
        default_value_t = crate::turbo::DEFAULT_WINDOW_MS
    )]
    pub turbo_window_ms: u64,
}

#[derive(Clone)]
pub struct NodeConfig {
    pub http_addr: SocketAddr,
    /// Binary TCP ingress address. `None` disables the listener.
    pub tcp_addr: Option<SocketAddr>,
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
    /// Bearer token required by ingress when set. Never logged or serialized.
    pub api_token: Option<String>,
    /// Participant derived from a valid token.
    pub api_participant: String,
    /// 0 disables per-IP ingress limiting.
    pub rate_limit_per_sec: u32,
    /// 0 disables the per-IP WebSocket sub-limit.
    pub ws_per_ip: usize,
    /// Turbo window limits. Validated even in durable mode so a bad value
    /// fails at startup, not when an operator flips `WHIPPET_MODE`.
    pub turbo: TurboConfig,
}

impl NodeConfig {
    pub fn auth_required(&self) -> bool {
        self.api_token.is_some()
    }

    pub fn from_args(args: NodeArgs) -> Result<Self, ConfigError> {
        let http_addr: SocketAddr = args.http_addr.parse().map_err(|_| {
            ConfigError::Invalid(format!("invalid HTTP address: {}", args.http_addr))
        })?;
        let tcp_addr = parse_tcp_addr(&args.tcp_addr)?;
        let journal = JournalKind::parse(&args.journal)?;
        let iggy_password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
        let mode: Mode = args.mode.parse()?;
        let turbo = TurboConfig::new(args.turbo_window_commands, args.turbo_window_ms)
            .map_err(ConfigError::Invalid)?;
        if mode == Mode::Turbo && journal != JournalKind::Iggy {
            return Err(ConfigError::Invalid(
                "turbo mode requires the Iggy journal; the in-memory journal is a test backend, not a supported deployment"
                    .into(),
            ));
        }
        if args.partitions == 0 || args.partitions > 256 {
            return Err(ConfigError::Invalid(
                "partitions must be between 1 and 256".into(),
            ));
        }

        let api_participant = args.api_participant;
        if api_participant.is_empty() || api_participant.len() > 64 {
            return Err(ConfigError::Invalid(
                "api participant must be between 1 and 64 bytes".into(),
            ));
        }
        let api_token = args.api_token.filter(|value| !value.trim().is_empty());

        let exchange = exchange_config(&args.venue, args.partitions, mode, &args.symbols)?;

        Ok(Self {
            http_addr,
            tcp_addr,
            iggy_addr: args.iggy_addr,
            iggy_user: args.iggy_user,
            iggy_password,
            journal,
            mode,
            exchange,
            snapshot_dir: args.snapshot_dir,
            snapshot_every: args.snapshot_every.max(1),
            dashboard_dir: args.dashboard_dir,
            build: args.build,
            api_token,
            api_participant,
            rate_limit_per_sec: args.rate_limit_per_sec,
            ws_per_ip: args.ws_per_ip,
            turbo,
        })
    }
}

/// Parse the optional binary TCP listen address. `off`, `none`, and an empty
/// value all disable the listener so a deployment can run HTTP-only without
/// changing the binary.
fn parse_tcp_addr(value: &str) -> Result<Option<SocketAddr>, ConfigError> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("off") || value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    value
        .parse()
        .map(Some)
        .map_err(|_| ConfigError::Invalid(format!("invalid TCP address: {value}")))
}

/// Resolve the Iggy password the same way for the node and `whippet doctor`.
/// The file wins when both are present, matching Compose secret mounts.
pub fn read_password(inline: &str, file: Option<&Path>) -> Result<String, ConfigError> {
    match file {
        Some(path) => Ok(std::fs::read_to_string(path)
            .map_err(|error| {
                ConfigError::Invalid(format!(
                    "could not read Iggy password file {}: {error}",
                    path.display()
                ))
            })?
            .trim_end_matches(['\r', '\n'])
            .to_string()),
        None => Ok(inline.to_string()),
    }
}

/// Build the configured instrument set the same way for the node and
/// `whippet doctor`, so snapshot verification uses the right symbols.
pub fn exchange_config(
    venue: &str,
    partitions: u32,
    mode: Mode,
    symbols: &str,
) -> Result<ExchangeConfig, ConfigError> {
    let mut requested: Vec<Symbol> = symbols
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Symbol::new)
        .collect::<Result<_, _>>()?;
    requested.sort_unstable();
    requested.dedup();
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
    for instrument in &instruments {
        instrument.validate()?;
    }
    if instruments.len() != requested.len() {
        let known: Vec<_> = default_instruments()
            .iter()
            .map(|instrument| instrument.symbol.to_string())
            .collect();
        return Err(ConfigError::Invalid(format!(
            "unknown symbol in {symbols:?}; known symbols: {}",
            known.join(", ")
        )));
    }
    Ok(ExchangeConfig {
        venue: venue.to_string(),
        partitions,
        mode,
        instruments,
    })
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
    fn turbo_defaults_are_bounded() {
        let cli = Cli::parse_from(["test", "--mode", "turbo"]);
        let config = NodeConfig::from_args(cli.node).expect("turbo is accepted with Iggy");
        assert_eq!(config.mode, Mode::Turbo);
        assert_eq!(
            config.turbo,
            TurboConfig {
                window_commands: crate::turbo::DEFAULT_WINDOW_COMMANDS,
                window_ms: crate::turbo::DEFAULT_WINDOW_MS,
            }
        );
    }

    #[test]
    fn turbo_rejects_the_memory_journal() {
        let cli = Cli::parse_from(["test", "--mode", "turbo", "--journal", "memory"]);
        match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("turbo must require the Iggy journal"),
            Err(error) => assert!(error.to_string().contains("requires the Iggy journal")),
        }
    }

    #[test]
    fn turbo_window_knobs_are_validated_in_every_mode() {
        let cli = Cli::parse_from(["test", "--turbo-window-commands", "0"]);
        assert!(NodeConfig::from_args(cli.node).is_err());
        let cli = Cli::parse_from(["test", "--turbo-window-ms", "0"]);
        assert!(NodeConfig::from_args(cli.node).is_err());
    }

    #[test]
    fn password_file_overrides_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("password");
        std::fs::write(&path, "s3cret\n").unwrap();
        let cli = Cli::parse_from(["test", "--iggy-password-file", path.to_str().unwrap()]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.iggy_password, "s3cret");
    }

    #[test]
    fn unknown_symbol_is_rejected() {
        let cli = Cli::parse_from(["test", "--symbols", "DOGE-USD"]);
        assert!(NodeConfig::from_args(cli.node).is_err());
    }

    #[test]
    fn ingress_defaults_to_anonymous_without_a_rate_limit() {
        let cli = Cli::parse_from(["test"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert!(config.api_token.is_none());
        assert!(!config.auth_required());
        assert_eq!(config.api_participant, "api");
        assert_eq!(config.rate_limit_per_sec, 0);
    }

    #[test]
    fn api_token_and_participant_are_configurable() {
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "s3cret",
            "--api-participant",
            "desk-1",
            "--rate-limit-per-sec",
            "250",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.api_token.as_deref(), Some("s3cret"));
        assert!(config.auth_required());
        assert_eq!(config.api_participant, "desk-1");
        assert_eq!(config.rate_limit_per_sec, 250);
    }

    #[test]
    fn an_empty_api_token_keeps_anonymous_ingress() {
        let cli = Cli::parse_from(["test", "--api-token", "   "]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert!(config.api_token.is_none());
        assert!(!config.auth_required());
    }

    #[test]
    fn an_empty_api_participant_is_rejected() {
        let cli = Cli::parse_from(["test", "--api-participant", ""]);
        assert!(NodeConfig::from_args(cli.node).is_err());
    }

    #[test]
    fn tcp_ingress_defaults_to_the_binary_port() {
        let cli = Cli::parse_from(["test"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.tcp_addr, Some("0.0.0.0:8081".parse().unwrap()));
    }

    #[test]
    fn tcp_ingress_can_be_disabled() {
        for value in ["off", "none", ""] {
            let cli = Cli::parse_from(["test", "--tcp-addr", value]);
            let config = NodeConfig::from_args(cli.node).unwrap();
            assert_eq!(config.tcp_addr, None, "{value:?} disables the listener");
        }
    }

    #[test]
    fn tcp_ingress_address_is_parsed() {
        let cli = Cli::parse_from(["test", "--tcp-addr", "127.0.0.1:9001"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.tcp_addr, Some("127.0.0.1:9001".parse().unwrap()));
    }

    #[test]
    fn a_malformed_tcp_address_is_rejected() {
        let cli = Cli::parse_from(["test", "--tcp-addr", "not-an-address"]);
        match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("a malformed TCP address must be rejected"),
            Err(error) => assert!(error.to_string().contains("invalid TCP address")),
        }
    }
}
