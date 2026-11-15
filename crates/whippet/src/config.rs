// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::Args;
use whippet_core::config::{ExchangeConfig, Mode, default_instruments};
use whippet_core::error::CoreError;
use whippet_core::ids::Symbol;

use crate::auth::ApiToken;
use crate::storage::StorageConfig;
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

/// Ingress pre-trade risk limits.
///
/// These are enforced at the HTTP and binary-TCP admission boundary, before
/// any journal append. They are an **ingress guard, not engine-enforced
/// risk**: the matcher remains the authority for what actually executes, and
/// a direct producer that appends to the orders log (or a future engine-side
/// path) bypasses them entirely.
///
/// Every limit is `u64` and `0` disables its check. Defaults are `0` so an
/// unconfigured node behaves exactly as before.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RiskConfig {
    /// Maximum quantity per new order. `0` disables the check.
    pub max_order_qty: u64,
    /// Maximum notional per new order that carries a price, in cents
    /// (`price_cents * qty`). Market orders with no price are not
    /// notional-checked in this slice. `0` disables the check.
    pub max_notional_cents: u64,
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
    /// Root of the node's own data volume. The bounded-storage monitor samples
    /// free space on the filesystem that holds this path.
    #[arg(
        long,
        env = "WHIPPET_DATA_DIR",
        default_value = crate::storage::DEFAULT_DATA_DIR
    )]
    pub data_dir: PathBuf,
    /// Iggy's data volume as seen from this container, typically a read-only
    /// mount. When the path does not exist the monitor samples only the
    /// Whippet volume.
    #[arg(
        long,
        env = "WHIPPET_IGGY_DATA_DIR",
        default_value = crate::storage::DEFAULT_IGGY_DATA_DIR
    )]
    pub iggy_data_dir: PathBuf,
    /// Volume usage percentage that starts reporting a storage warning. Must be
    /// inside 1..=99 and below the critical threshold.
    #[arg(
        long,
        env = "WHIPPET_DISK_WARN_PCT",
        default_value_t = crate::storage::DEFAULT_WARN_PCT
    )]
    pub disk_warn_pct: u8,
    /// Volume usage percentage that reports critical storage pressure and is
    /// meant to fail readiness and durable admission. Same bounds as the
    /// warning, and above it.
    #[arg(
        long,
        env = "WHIPPET_DISK_CRIT_PCT",
        default_value_t = crate::storage::DEFAULT_CRIT_PCT
    )]
    pub disk_crit_pct: u8,
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
    /// Optional legacy bearer token for order ingress and cancel. When unset,
    /// those endpoints accept anonymous requests: a demo convenience, not a
    /// deployment posture. Set this for anything reachable by another host.
    #[arg(long, env = "WHIPPET_API_TOKEN")]
    pub api_token: Option<String>,
    /// Participant name assigned to the legacy single token. Requests may omit
    /// `participant`; supplying a different one is rejected with 403.
    #[arg(long, env = "WHIPPET_API_PARTICIPANT", default_value = "api")]
    pub api_participant: String,
    /// Scoped tokens as `token:participant` pairs, comma separated. Each token
    /// authorizes only its own participant, so self-trade prevention and order
    /// attribution see distinct identities. Merged with `WHIPPET_API_TOKEN`.
    #[arg(long, env = "WHIPPET_API_TOKENS")]
    pub api_tokens: Option<String>,
    /// Scoped operator tokens as `token:operator-name` pairs, comma separated.
    /// Each token authorizes the halt/resume admin endpoints as its own
    /// operator identity. Separate from `WHIPPET_API_TOKENS`: a trading token
    /// is never an operator credential.
    #[arg(long, env = "WHIPPET_OPERATOR_TOKENS")]
    pub operator_tokens: Option<String>,
    /// Optional bearer token for `GET /metrics`. When set, the endpoint
    /// requires it; health and readiness stay unauthenticated for probes.
    #[arg(long, env = "WHIPPET_METRICS_TOKEN")]
    pub metrics_token: Option<String>,
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
    /// Pre-trade risk limit: maximum quantity per new order. `0` disables the
    /// check. Enforced at ingress, before any journal append.
    #[arg(long, env = "WHIPPET_RISK_MAX_ORDER_QTY", default_value_t = 0)]
    pub risk_max_order_qty: u64,
    /// Pre-trade risk limit: maximum notional per priced new order, in cents
    /// (`price_cents * qty`). Market orders without a price are not
    /// notional-checked. `0` disables the check.
    #[arg(long, env = "WHIPPET_RISK_MAX_NOTIONAL_CENTS", default_value_t = 0)]
    pub risk_max_notional_cents: u64,
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
    /// Participant derived from the legacy single token.
    pub api_participant: String,
    /// Scoped tokens. Never logged or serialized.
    pub api_tokens: Vec<ApiToken>,
    /// Scoped operator tokens for the admin endpoints. Never logged or
    /// serialized.
    pub operator_tokens: Vec<ApiToken>,
    /// Optional bearer token for `/metrics`.
    pub metrics_token: Option<String>,
    /// 0 disables per-IP ingress limiting.
    pub rate_limit_per_sec: u32,
    /// 0 disables the per-IP WebSocket sub-limit.
    pub ws_per_ip: usize,
    /// Turbo window limits. Validated even in durable mode so a bad value
    /// fails at startup, not when an operator flips `WHIPPET_MODE`.
    pub turbo: TurboConfig,
    /// Disk thresholds and volume roots for the bounded-storage guard.
    pub storage: StorageConfig,
    /// Ingress pre-trade risk limits. `0` on every field disables the checks;
    /// see [`RiskConfig`] for the ingress-guard caveat.
    pub risk: RiskConfig,
}

impl NodeConfig {
    pub fn auth_required(&self) -> bool {
        self.api_token.is_some() || !self.api_tokens.is_empty()
    }

    /// Map a presented bearer token to its participant, or `None`.
    ///
    /// Every entry is compared without a length or byte shortcut, and the
    /// loop does not stop at the first match, so response timing does not
    /// reveal whether a token exists or which entry matched.
    pub fn participant_for(&self, presented: &str) -> Option<&str> {
        let presented = presented.as_bytes();
        let mut matched: Option<&str> = None;
        for entry in &self.api_tokens {
            if crate::auth::constant_time_eq(entry.token.as_bytes(), presented) && matched.is_none()
            {
                matched = Some(entry.participant.as_str());
            }
        }
        if let Some(legacy) = self.api_token.as_deref()
            && crate::auth::constant_time_eq(legacy.as_bytes(), presented)
            && matched.is_none()
        {
            matched = Some(self.api_participant.as_str());
        }
        matched
    }

    /// True when any operator token is configured. False means the admin
    /// endpoints refuse every caller with `operator_not_configured`.
    pub fn operators_configured(&self) -> bool {
        !self.operator_tokens.is_empty()
    }

    /// Map a presented bearer token to its operator name, or `None`.
    ///
    /// Same timing contract as [`Self::participant_for`]: a full constant-time
    /// scan that does not stop at the first match, so response timing does not
    /// reveal whether a token exists or which entry matched.
    pub fn operator_for(&self, presented: &str) -> Option<&str> {
        let presented = presented.as_bytes();
        let mut matched: Option<&str> = None;
        for entry in &self.operator_tokens {
            if crate::auth::constant_time_eq(entry.token.as_bytes(), presented) && matched.is_none()
            {
                matched = Some(entry.participant.as_str());
            }
        }
        matched
    }

    /// True when the presented token may read `/metrics`; always true when no
    /// metrics token is configured.
    pub fn metrics_authorized(&self, presented: Option<&str>) -> bool {
        match self.metrics_token.as_deref() {
            None => true,
            Some(expected) => presented.is_some_and(|token| {
                crate::auth::constant_time_eq(expected.as_bytes(), token.as_bytes())
            }),
        }
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
        // A missing Iggy mount is reported rather than fatal: the node can run
        // before the volume is attached, and the monitor then samples only its
        // own data volume.
        let iggy_dir = args.iggy_data_dir.is_dir().then_some(args.iggy_data_dir);
        let storage = StorageConfig {
            warn_pct: args.disk_warn_pct,
            crit_pct: args.disk_crit_pct,
            whippet_dir: args.data_dir,
            iggy_dir,
        };
        storage.validate().map_err(ConfigError::Invalid)?;

        let api_participant = args.api_participant;
        if api_participant.is_empty() || api_participant.len() > 64 {
            return Err(ConfigError::Invalid(
                "api participant must be between 1 and 64 bytes".into(),
            ));
        }
        let api_token = args.api_token.filter(|value| !value.trim().is_empty());
        let api_tokens = args
            .api_tokens
            .as_deref()
            .map(parse_api_tokens)
            .transpose()?
            .unwrap_or_default();
        let operator_tokens = args
            .operator_tokens
            .as_deref()
            .map(parse_operator_tokens)
            .transpose()?
            .unwrap_or_default();
        let metrics_token = args.metrics_token.filter(|value| !value.trim().is_empty());
        // A duplicate token would make `participant_for` order-dependent; a
        // duplicate participant is allowed (rotation, two tokens one desk).
        reject_duplicate_tokens(&api_tokens, "WHIPPET_API_TOKENS", "participant")?;
        reject_duplicate_tokens(&operator_tokens, "WHIPPET_OPERATOR_TOKENS", "operator")?;
        if let Some(legacy) = api_token.as_deref()
            && api_tokens.iter().any(|entry| entry.token == legacy)
        {
            return Err(ConfigError::Invalid(
                "WHIPPET_API_TOKEN duplicates an entry in WHIPPET_API_TOKENS; a token must have one trading identity"
                    .into(),
            ));
        }
        // A token that appears in two role namespaces would grant both
        // privileges (or make the metrics credential an admin credential).
        // Roles are separate by design; reject the overlap at startup rather
        // than silently choosing one.
        validate_distinct_roles(
            api_token.as_deref(),
            &api_tokens,
            &operator_tokens,
            metrics_token.as_deref(),
        )?;

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
            api_tokens,
            operator_tokens,
            metrics_token,
            rate_limit_per_sec: args.rate_limit_per_sec,
            ws_per_ip: args.ws_per_ip,
            turbo,
            storage,
            risk: RiskConfig {
                max_order_qty: args.risk_max_order_qty,
                max_notional_cents: args.risk_max_notional_cents,
            },
        })
    }
}

/// Parse `token:name` pairs from one of the scoped-token variables.
///
/// Tokens cannot contain `:`; the first colon separates the pair. Empty pairs
/// are ignored so a trailing comma is harmless. Both fields are trimmed and
/// bounded like the legacy token/participant. `env` names the variable for the
/// error text; `role` names the identity the token binds (`participant` for
/// trading, `operator` for the admin endpoints).
fn parse_token_pairs(raw: &str, env: &str, role: &str) -> Result<Vec<ApiToken>, ConfigError> {
    let mut tokens = Vec::new();
    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let Some((token, name)) = pair.split_once(':') else {
            return Err(ConfigError::Invalid(format!(
                "{env} entries must be token:{role}"
            )));
        };
        let token = token.trim();
        let name = name.trim();
        if token.is_empty() || token.len() > 256 {
            return Err(ConfigError::Invalid(
                "an API token must be between 1 and 256 bytes".into(),
            ));
        }
        if token.contains(':') {
            return Err(ConfigError::Invalid(format!(
                "API tokens cannot contain ':' (use token:{role})"
            )));
        }
        if name.is_empty() || name.len() > 64 {
            return Err(ConfigError::Invalid(format!(
                "a token {role} must be between 1 and 64 bytes"
            )));
        }
        tokens.push(ApiToken {
            token: token.to_string(),
            participant: name.to_string(),
        });
    }
    Ok(tokens)
}

fn parse_api_tokens(raw: &str) -> Result<Vec<ApiToken>, ConfigError> {
    parse_token_pairs(raw, "WHIPPET_API_TOKENS", "participant")
}

fn parse_operator_tokens(raw: &str) -> Result<Vec<ApiToken>, ConfigError> {
    parse_token_pairs(raw, "WHIPPET_OPERATOR_TOKENS", "operator")
}

/// Reject a duplicate token in a scoped-token list.
///
/// A duplicate token would make the lookup order-dependent; a duplicate name
/// is allowed (rotation, two tokens one identity).
/// Reject a token string that appears in more than one credential role.
fn validate_distinct_roles(
    legacy_trading: Option<&str>,
    trading: &[ApiToken],
    operators: &[ApiToken],
    metrics: Option<&str>,
) -> Result<(), ConfigError> {
    let mut entries: Vec<(&str, &'static str)> = Vec::new();
    if let Some(token) = legacy_trading {
        entries.push((token, "trading token (WHIPPET_API_TOKEN)"));
    }
    for entry in trading {
        entries.push((entry.token.as_str(), "trading token (WHIPPET_API_TOKENS)"));
    }
    for entry in operators {
        entries.push((
            entry.token.as_str(),
            "operator token (WHIPPET_OPERATOR_TOKENS)",
        ));
    }
    if let Some(token) = metrics {
        entries.push((token, "metrics token (WHIPPET_METRICS_TOKEN)"));
    }
    for (index, (token, role)) in entries.iter().enumerate() {
        if let Some((_, other)) = entries[..index]
            .iter()
            .find(|(other_token, _)| other_token == token)
        {
            return Err(ConfigError::Invalid(format!(
                "the same bearer token is configured as both {other} and {role}; use a distinct token per role"
            )));
        }
    }
    Ok(())
}

fn reject_duplicate_tokens(tokens: &[ApiToken], env: &str, role: &str) -> Result<(), ConfigError> {
    for (index, entry) in tokens.iter().enumerate() {
        if tokens[..index]
            .iter()
            .any(|other| other.token == entry.token)
        {
            return Err(ConfigError::Invalid(format!(
                "duplicate token for {role} {} in {env}",
                entry.participant
            )));
        }
    }
    Ok(())
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
    fn storage_thresholds_default_and_validate() {
        let cli = Cli::parse_from(["test"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.storage.warn_pct, 70);
        assert_eq!(config.storage.crit_pct, 85);
        assert!(config.storage.validate().is_ok());

        // warn >= crit, and either threshold outside 1..=99, are startup errors.
        for args in [
            ["--disk-warn-pct", "70", "--disk-crit-pct", "70"],
            ["--disk-warn-pct", "90", "--disk-crit-pct", "85"],
            ["--disk-warn-pct", "0", "--disk-crit-pct", "85"],
            ["--disk-warn-pct", "70", "--disk-crit-pct", "100"],
        ] {
            let cli = Cli::parse_from(["test", args[0], args[1], args[2], args[3]]);
            assert!(
                NodeConfig::from_args(cli.node).is_err(),
                "{args:?} must be rejected"
            );
        }
    }

    #[test]
    fn storage_volumes_are_configured_from_paths() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let iggy = dir.path().join("iggy");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&iggy).unwrap();
        let cli = Cli::parse_from([
            "test",
            "--data-dir",
            data.to_str().unwrap(),
            "--iggy-data-dir",
            iggy.to_str().unwrap(),
            "--disk-warn-pct",
            "60",
            "--disk-crit-pct",
            "80",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.storage.whippet_dir, data);
        assert_eq!(config.storage.iggy_dir.as_deref(), Some(iggy.as_path()));
        assert_eq!(config.storage.warn_pct, 60);
        assert_eq!(config.storage.crit_pct, 80);
    }

    #[test]
    fn a_missing_iggy_volume_is_reported_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let iggy = dir.path().join("not-mounted");
        let cli = Cli::parse_from(["test", "--iggy-data-dir", iggy.to_str().unwrap()]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.storage.iggy_dir, None);
    }

    #[test]
    fn scoped_tokens_bind_distinct_participants() {
        let cli = Cli::parse_from([
            "test",
            "--api-tokens",
            "tok-a:alice, tok-b:bob",
            "--metrics-token",
            "metrics-secret",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.participant_for("tok-a"), Some("alice"));
        assert_eq!(config.participant_for("tok-b"), Some("bob"));
        assert_eq!(config.participant_for("tok-c"), None);
        assert!(config.auth_required());
        assert!(config.metrics_authorized(Some("metrics-secret")));
        assert!(!config.metrics_authorized(Some("nope")));
        assert!(!config.metrics_authorized(None));
    }

    #[test]
    fn malformed_and_duplicate_scoped_tokens_are_rejected() {
        for raw in ["no-colon", "tok:", ":alice", "tok-a:alice,tok-a:bob"] {
            let cli = Cli::parse_from(["test", "--api-tokens", raw]);
            assert!(
                NodeConfig::from_args(cli.node).is_err(),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn operator_tokens_bind_distinct_operators() {
        let cli = Cli::parse_from(["test", "--operator-tokens", "op-a:lead, op-b:risk"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert!(config.operators_configured());
        assert_eq!(config.operator_for("op-a"), Some("lead"));
        assert_eq!(config.operator_for("op-b"), Some("risk"));
        assert_eq!(config.operator_for("op-c"), None);
    }

    #[test]
    fn a_trading_token_is_never_an_operator_token() {
        let cli = Cli::parse_from([
            "test",
            "--api-tokens",
            "tok-a:alice",
            "--operator-tokens",
            "op-a:lead",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.operator_for("tok-a"), None);
        assert_eq!(config.participant_for("op-a"), None);
    }

    #[test]
    fn without_operator_tokens_the_admin_lookup_is_closed() {
        let cli = Cli::parse_from(["test", "--api-token", "trader-s3cret"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert!(!config.operators_configured());
        assert_eq!(config.operator_for("trader-s3cret"), None);
    }

    #[test]
    fn malformed_and_duplicate_operator_tokens_are_rejected() {
        for raw in [
            "no-colon",
            "op:",
            ":lead",
            "op-a:lead,op-a:risk",
            "op-a:lead,op-b:",
        ] {
            let cli = Cli::parse_from(["test", "--operator-tokens", raw]);
            assert!(
                NodeConfig::from_args(cli.node).is_err(),
                "{raw} must be rejected"
            );
        }
    }

    #[test]
    fn the_legacy_token_still_authorizes_its_participant() {
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "legacy",
            "--api-participant",
            "desk-1",
            "--api-tokens",
            "tok-a:alice",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.participant_for("legacy"), Some("desk-1"));
        assert_eq!(config.participant_for("tok-a"), Some("alice"));
    }

    #[test]
    fn credential_roles_cannot_share_a_token() {
        // Trading + operator.
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "shared",
            "--operator-tokens",
            "shared:ops",
        ]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("shared token must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("both"), "{error}");

        // Scoped trading + operator.
        let cli = Cli::parse_from([
            "test",
            "--api-tokens",
            "shared:alice",
            "--operator-tokens",
            "shared:ops",
        ]);
        assert!(NodeConfig::from_args(cli.node).is_err());

        // Metrics + trading.
        let cli = Cli::parse_from(["test", "--api-token", "shared", "--metrics-token", "shared"]);
        assert!(NodeConfig::from_args(cli.node).is_err());

        // Distinct tokens across all three roles are accepted.
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "trade",
            "--operator-tokens",
            "ops:ops1",
            "--metrics-token",
            "metrics",
        ]);
        assert!(NodeConfig::from_args(cli.node).is_ok());
    }

    #[test]
    fn a_legacy_token_cannot_be_repeated_in_the_scoped_list() {
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "shared",
            "--api-tokens",
            "shared:alice",
        ]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("duplicate trading token must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("duplicates an entry"), "{error}");
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
    fn risk_defaults_are_zero_and_disable_every_check() {
        let cli = Cli::parse_from(["test"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.risk, RiskConfig::default());
        assert_eq!(config.risk.max_order_qty, 0);
        assert_eq!(config.risk.max_notional_cents, 0);
    }

    #[test]
    fn risk_limits_parse_from_their_flags() {
        let cli = Cli::parse_from([
            "test",
            "--risk-max-order-qty",
            "1000",
            "--risk-max-notional-cents",
            "5000000",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(
            config.risk,
            RiskConfig {
                max_order_qty: 1000,
                max_notional_cents: 5_000_000,
            }
        );
    }

    #[test]
    fn a_zero_risk_flag_disables_only_its_own_check() {
        let cli = Cli::parse_from(["test", "--risk-max-order-qty", "100"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.risk.max_order_qty, 100);
        assert_eq!(config.risk.max_notional_cents, 0);
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
