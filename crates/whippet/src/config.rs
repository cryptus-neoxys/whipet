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
        let http_addr = parse_bind_addr(&args.http_addr, "HTTP")?;
        let tcp_addr = parse_tcp_addr(&args.tcp_addr)?;
        let journal = JournalKind::parse(&args.journal)?;
        let iggy_password = read_password(&args.iggy_password, args.iggy_password_file.as_deref())?;
        // Resolve the Iggy address once at startup so a typo fails here, not
        // on the first journal append. The memory journal never dials Iggy,
        // so brokerless development does not depend on DNS for an address it
        // will never use.
        let iggy_addr = parse_iggy_addr(&args.iggy_addr, journal)?;
        if journal == JournalKind::Iggy && iggy_password.is_empty() {
            return Err(ConfigError::Invalid(
                "Iggy password must not be empty (set WHIPPET_IGGY_PASSWORD or WHIPPET_IGGY_PASSWORD_FILE)"
                    .into(),
            ));
        }
        if journal == JournalKind::Iggy && args.iggy_user.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "Iggy username must not be empty (set WHIPPET_IGGY_USER)".into(),
            ));
        }
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
        if args.snapshot_every == 0 {
            return Err(ConfigError::Invalid(
                "snapshot cadence (WHIPPET_SNAPSHOT_EVERY) must be at least 1".into(),
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

        let config = Self {
            http_addr,
            tcp_addr,
            iggy_addr,
            iggy_user: args.iggy_user,
            iggy_password,
            journal,
            mode,
            exchange,
            snapshot_dir: args.snapshot_dir,
            snapshot_every: args.snapshot_every,
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
        };
        // Every construction path through flags/env ends here, so a bad
        // combination fails before any network bind or journal write.
        config.check_invariants()?;
        Ok(config)
    }

    /// Validate the assembled node configuration without touching the network
    /// or the filesystem: no DNS, no binds, no journal writes, no directory
    /// probes. Hostname resolution and secret-file reads stay in
    /// [`Self::from_args`], which runs before this check.
    ///
    /// Invariants enforced:
    /// - mode/journal: turbo requires the Iggy journal; durable runs on either
    ///   (memory is the test backend).
    /// - credentials: every token string non-empty; scoped names non-empty;
    ///   token strings pairwise disjoint across all roles (legacy trading,
    ///   scoped trading, operator, metrics); duplicate tokens rejected within
    ///   each scoped list; operator names unique; two trading tokens may share
    ///   one participant (documented rotation: the old and new token overlap
    ///   while clients roll over, and attribution sees one desk either way).
    /// - symbols/exchange: partitions `1..=256`; instruments non-empty with
    ///   unique symbols; each instrument re-validated through
    ///   `InstrumentConfig::validate` (positive tick, ordered ladder, the
    ///   5,000,000-level safety cap); each symbol's partition pins inside
    ///   `partitions`.
    /// - storage: `0 < warn_pct < crit_pct <= 99` via `StorageConfig::validate`;
    ///   snapshot cadence `> 0`; rate limits are `u32`/`usize` where `0`
    ///   disables the limiter (no rejection). Missing/unreadable data
    ///   directories are NOT errors: a volume may simply not be mounted yet,
    ///   and the monitor reports the gap as `Warn` at runtime.
    /// - addresses: neither listener may be the degenerate `0.0.0.0:0` /
    ///   `[::]:0` (unspecified address with port 0 binds nothing); port 0 is
    ///   otherwise allowed so tests can take an ephemeral port; HTTP and TCP
    ///   must not be the same `SocketAddr` with a real port (they cannot both
    ///   bind it); a disabled TCP listener (`None`) is valid.
    /// - iggy: for the Iggy journal the address must be `host:port` shaped
    ///   with a non-zero port, and username/password non-empty. The memory
    ///   journal never dials Iggy, so these are skipped there.
    /// - risk: limits are `u64`, so non-negativity is by type; both zero
    ///   disables the guard. No overflow check is needed at the config layer:
    ///   ingress multiplies `price_cents * qty` in `i128` (see the TCP/HTTP
    ///   risk guards), which cannot wrap for any `u64` limit.
    pub fn check_invariants(&self) -> Result<(), ConfigError> {
        if self.mode == Mode::Turbo && self.journal != JournalKind::Iggy {
            return Err(ConfigError::Invalid(
                "turbo mode requires the Iggy journal; the in-memory journal is a test backend, not a supported deployment"
                    .into(),
            ));
        }
        if self.exchange.partitions == 0 || self.exchange.partitions > 256 {
            return Err(ConfigError::Invalid(
                "partitions must be between 1 and 256".into(),
            ));
        }
        self.storage.validate().map_err(ConfigError::Invalid)?;
        if self.snapshot_every == 0 {
            return Err(ConfigError::Invalid(
                "snapshot cadence must be at least 1".into(),
            ));
        }
        if self.api_participant.is_empty() || self.api_participant.len() > 64 {
            return Err(ConfigError::Invalid(
                "api participant must be between 1 and 64 bytes".into(),
            ));
        }
        if let Some(legacy) = self.api_token.as_deref()
            && legacy.is_empty()
        {
            return Err(ConfigError::Invalid(
                "legacy API token must not be empty".into(),
            ));
        }
        for entry in &self.api_tokens {
            check_token_pair(entry, "WHIPPET_API_TOKENS", "participant")?;
        }
        for entry in &self.operator_tokens {
            check_token_pair(entry, "WHIPPET_OPERATOR_TOKENS", "operator")?;
        }
        if let Some(metrics) = self.metrics_token.as_deref()
            && metrics.is_empty()
        {
            return Err(ConfigError::Invalid(
                "metrics token must not be empty".into(),
            ));
        }
        reject_duplicate_tokens(&self.api_tokens, "WHIPPET_API_TOKENS", "participant")?;
        reject_duplicate_tokens(&self.operator_tokens, "WHIPPET_OPERATOR_TOKENS", "operator")?;
        // Operator names are unique: each operator token authorizes the admin
        // endpoints as its own identity, so two tokens sharing one name would
        // make audit attribution ambiguous. (Trading participants are the
        // exception: duplicate names are allowed rotation, documented above.)
        reject_duplicate_names(&self.operator_tokens)?;
        if let Some(legacy) = self.api_token.as_deref()
            && self.api_tokens.iter().any(|entry| entry.token == legacy)
        {
            return Err(ConfigError::Invalid(
                "WHIPPET_API_TOKEN duplicates an entry in WHIPPET_API_TOKENS; a token must have one trading identity"
                    .into(),
            ));
        }
        validate_distinct_roles(
            self.api_token.as_deref(),
            &self.api_tokens,
            &self.operator_tokens,
            self.metrics_token.as_deref(),
        )?;
        check_bind_pair(self.http_addr, self.tcp_addr)?;
        if self.journal == JournalKind::Iggy {
            check_iggy_addr_shape(&self.iggy_addr)?;
            if self.iggy_user.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "Iggy username must not be empty".into(),
                ));
            }
            if self.iggy_password.is_empty() {
                return Err(ConfigError::Invalid(
                    "Iggy password must not be empty".into(),
                ));
            }
        }
        if self.exchange.instruments.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one symbol is required".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for instrument in &self.exchange.instruments {
            instrument.validate()?;
            if !seen.insert(instrument.symbol) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate symbol in exchange config: {}",
                    instrument.symbol
                )));
            }
            if instrument.symbol.partition(self.exchange.partitions) >= self.exchange.partitions {
                return Err(ConfigError::Invalid(format!(
                    "symbol {} partitions outside 0..{}",
                    instrument.symbol, self.exchange.partitions
                )));
            }
        }
        Ok(())
    }
}

/// One parsed `token:name` pair, re-checked on the assembled config so direct
/// struct construction gets the same bounds as flag parsing.
fn check_token_pair(entry: &ApiToken, env: &str, role: &str) -> Result<(), ConfigError> {
    if entry.token.is_empty() || entry.token.len() > 256 {
        return Err(ConfigError::Invalid(
            "an API token must be between 1 and 256 bytes".into(),
        ));
    }
    if entry.token.contains(':') {
        return Err(ConfigError::Invalid(format!(
            "API tokens cannot contain ':' (use token:{role})"
        )));
    }
    if entry.participant.is_empty() || entry.participant.len() > 64 {
        return Err(ConfigError::Invalid(format!(
            "a token {role} must be between 1 and 64 bytes ({env})"
        )));
    }
    Ok(())
}

/// Reject two operator tokens sharing one name. Trading participants may
/// share a name (rotation); operator identities must stay attributable.
fn reject_duplicate_names(operators: &[ApiToken]) -> Result<(), ConfigError> {
    for (index, entry) in operators.iter().enumerate() {
        if operators[..index]
            .iter()
            .any(|other| other.participant == entry.participant)
        {
            return Err(ConfigError::Invalid(format!(
                "duplicate operator name {} in WHIPPET_OPERATOR_TOKENS; each operator token needs its own identity",
                entry.participant
            )));
        }
    }
    Ok(())
}

/// Pure bind-pair check: no DNS, no sockets. Port 0 is an ephemeral-port
/// request for tests; the degenerate unspecified+0 pair binds nothing and is
/// rejected, as is an HTTP/TCP collision on a real port.
fn check_bind_pair(http_addr: SocketAddr, tcp_addr: Option<SocketAddr>) -> Result<(), ConfigError> {
    for (kind, addr) in [("HTTP", Some(http_addr)), ("TCP", tcp_addr)]
        .into_iter()
        .filter_map(|(kind, addr)| addr.map(|addr| (kind, addr)))
    {
        if addr.port() == 0 && addr.ip().is_unspecified() {
            return Err(ConfigError::Invalid(format!(
                "invalid {kind} address: {addr} binds nothing (unspecified address with port 0)"
            )));
        }
    }
    if let Some(tcp) = tcp_addr
        && tcp == http_addr
        && http_addr.port() != 0
    {
        return Err(ConfigError::Invalid(format!(
            "HTTP and TCP listeners cannot share {http_addr}; give each its own host:port"
        )));
    }
    Ok(())
}

/// No-I/O shape check for the Iggy address: `host:port` with a non-zero
/// port. Resolution happens once in [`NodeConfig::from_args`]; this only
/// re-checks the shape so directly constructed configs get the same bound.
fn check_iggy_addr_shape(value: &str) -> Result<(), ConfigError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ConfigError::Invalid(
            "Iggy address must not be empty (use host:port, for example 127.0.0.1:8090)".into(),
        ));
    }
    if let Ok(addr) = value.parse::<SocketAddr>() {
        if addr.port() == 0 {
            return Err(ConfigError::Invalid(format!(
                "invalid Iggy address: {value} (port must be non-zero)"
            )));
        }
        return Ok(());
    }
    match value.rsplit_once(':') {
        Some((host, port)) if !host.trim().is_empty() => match port.trim().parse::<u16>() {
            Ok(port) if port != 0 => Ok(()),
            _ => Err(ConfigError::Invalid(format!(
                "invalid Iggy address: {value} (use host:port, for example iggy:8090)"
            ))),
        },
        _ => Err(ConfigError::Invalid(format!(
            "invalid Iggy address: {value} (use host:port, for example iggy:8090)"
        ))),
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
    parse_bind_addr(value, "TCP").map(Some)
}

/// Resolve the Iggy address once at startup.
///
/// A numeric `IP:port` resolves without I/O; a hostname is resolved here, at
/// startup only, so an unresolvable value fails before any bind or journal
/// write. The memory journal never dials Iggy, so its address is passed
/// through unchecked and brokerless development never depends on DNS.
fn parse_iggy_addr(value: &str, journal: JournalKind) -> Result<String, ConfigError> {
    let value = value.trim();
    if journal == JournalKind::Memory {
        return Ok(value.to_string());
    }
    if value.is_empty() {
        return Err(ConfigError::Invalid(
            "invalid Iggy address: empty (use host:port, for example 127.0.0.1:8090)".into(),
        ));
    }
    use std::net::ToSocketAddrs;
    value
        .to_socket_addrs()
        .ok()
        .and_then(|mut addresses| addresses.next())
        .ok_or_else(|| {
            ConfigError::Invalid(format!(
                "invalid Iggy address: {value} (use host:port, for example iggy:8090)"
            ))
        })?;
    Ok(value.to_string())
}

/// Parse a bind address.
///
/// A numeric `IP:port` is the normal and recommended form (`0.0.0.0:8081`
/// for a container that publishes the port). A hostname is also accepted and
/// resolved once at startup, because Compose/Mac users naturally write
/// `node:8081` or `localhost:8081`; an unresolvable or malformed value fails
/// startup with the value named.
fn parse_bind_addr(value: &str, kind: &str) -> Result<SocketAddr, ConfigError> {
    let value = value.trim();
    if let Ok(address) = value.parse() {
        return Ok(address);
    }
    use std::net::ToSocketAddrs;
    value
        .to_socket_addrs()
        .ok()
        .and_then(|mut addresses| addresses.next())
        .ok_or_else(|| {
            ConfigError::Invalid(format!(
                "invalid {kind} address: {value} (use host:port, for example 0.0.0.0:8081)"
            ))
        })
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
///
/// Duplicate symbols are rejected rather than deduplicated: a repeated symbol
/// is almost always a copy-paste error in `WHIPPET_SYMBOLS`, and silently
/// dropping it would hide the mistake while changing nothing else.
pub fn exchange_config(
    venue: &str,
    partitions: u32,
    mode: Mode,
    symbols: &str,
) -> Result<ExchangeConfig, ConfigError> {
    let raws: Vec<&str> = symbols
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    if raws.is_empty() {
        return Err(ConfigError::Invalid(
            "at least one symbol is required".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for raw in &raws {
        if !seen.insert(*raw) {
            return Err(ConfigError::Invalid(format!(
                "duplicate symbol in {symbols:?}: {raw} appears more than once"
            )));
        }
    }
    let mut requested: Vec<Symbol> = raws
        .iter()
        .map(|raw| Symbol::new(raw))
        .collect::<Result<_, _>>()?;
    requested.sort_unstable();
    requested.dedup();
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
    fn bind_addresses_accept_numeric_and_resolved_forms() {
        let cli = Cli::parse_from([
            "test",
            "--http-addr",
            "localhost:0",
            "--tcp-addr",
            "localhost:0",
        ]);
        let config = NodeConfig::from_args(cli.node).expect("localhost resolves");
        assert_eq!(config.tcp_addr.expect("tcp is enabled").port(), 0);
        assert_eq!(config.http_addr.port(), 0);

        let cli = Cli::parse_from(["test", "--tcp-addr", "not a host:port"]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("a malformed bind address must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("invalid TCP address"), "{error}");

        let cli = Cli::parse_from(["test", "--tcp-addr", "off"]);
        assert!(NodeConfig::from_args(cli.node).unwrap().tcp_addr.is_none());
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

    /// A valid config for direct `check_invariants` construction. Mirrors the
    /// demo defaults with loopback binds.
    fn test_config() -> NodeConfig {
        NodeConfig {
            http_addr: "127.0.0.1:8080".parse().unwrap(),
            tcp_addr: Some("127.0.0.1:8081".parse().unwrap()),
            iggy_addr: "127.0.0.1:8090".into(),
            iggy_user: "iggy".into(),
            iggy_password: "iggy".into(),
            journal: JournalKind::Iggy,
            mode: Mode::Durable,
            exchange: ExchangeConfig::default(),
            snapshot_dir: PathBuf::from("data/snapshots"),
            snapshot_every: 10_000,
            dashboard_dir: PathBuf::from("apps/dashboard/dist"),
            build: "test".into(),
            api_token: None,
            api_participant: "api".into(),
            api_tokens: Vec::new(),
            operator_tokens: Vec::new(),
            metrics_token: None,
            rate_limit_per_sec: 0,
            ws_per_ip: 0,
            turbo: crate::turbo::TurboConfig::default(),
            storage: crate::storage::StorageConfig::default(),
            risk: RiskConfig::default(),
        }
    }

    fn token_pair(token: &str, participant: &str) -> ApiToken {
        ApiToken {
            token: token.into(),
            participant: participant.into(),
        }
    }

    #[test]
    fn check_invariants_accepts_a_valid_config() {
        test_config().check_invariants().expect("valid config");
    }

    #[test]
    fn duplicate_symbols_are_rejected_not_deduplicated() {
        for symbols in [
            "BTC-USD,BTC-USD",
            "BTC-USD, BTC-USD",
            "ETH-USD,ETH-USD,ETH-USD",
        ] {
            let cli = Cli::parse_from(["test", "--symbols", symbols]);
            let error = match NodeConfig::from_args(cli.node) {
                Ok(_) => panic!("{symbols:?} must be rejected"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("duplicate symbol"), "{error}");
        }
        // Distinct symbols still parse.
        let cli = Cli::parse_from(["test", "--symbols", "BTC-USD,ETH-USD"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.exchange.instruments.len(), 2);
    }

    #[test]
    fn empty_and_blank_symbol_lists_are_rejected() {
        for symbols in ["", " , "] {
            let cli = Cli::parse_from(["test", "--symbols", symbols]);
            assert!(
                NodeConfig::from_args(cli.node).is_err(),
                "{symbols:?} must be rejected"
            );
        }
    }

    #[test]
    fn partitions_stay_inside_1_to_256() {
        for partitions in ["0", "257"] {
            let cli = Cli::parse_from(["test", "--partitions", partitions]);
            assert!(
                NodeConfig::from_args(cli.node).is_err(),
                "partitions={partitions} must be rejected"
            );
        }
        for partitions in ["1", "256"] {
            let cli = Cli::parse_from(["test", "--partitions", partitions]);
            assert!(
                NodeConfig::from_args(cli.node).is_ok(),
                "partitions={partitions} must be accepted"
            );
        }
    }

    #[test]
    fn http_and_tcp_cannot_share_one_socket() {
        // Same host:port with a real port cannot bind twice.
        let cli = Cli::parse_from([
            "test",
            "--http-addr",
            "127.0.0.1:9999",
            "--tcp-addr",
            "127.0.0.1:9999",
        ]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("a shared HTTP/TCP socket must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("cannot share"), "{error}");

        // Port 0 is an ephemeral-port request for tests, not a collision.
        let mut config = test_config();
        config.http_addr = "127.0.0.1:0".parse().unwrap();
        config.tcp_addr = Some("127.0.0.1:0".parse().unwrap());
        config.check_invariants().expect("port 0 is ephemeral");

        // A disabled TCP listener never collides.
        let mut config = test_config();
        config.tcp_addr = None;
        config.check_invariants().expect("disabled TCP is valid");

        // The degenerate unspecified+0 pair binds nothing.
        let mut config = test_config();
        config.http_addr = "0.0.0.0:0".parse().unwrap();
        config.tcp_addr = None;
        assert!(config.check_invariants().is_err());
    }

    #[test]
    fn duplicate_operator_names_are_rejected() {
        let cli = Cli::parse_from(["test", "--operator-tokens", "a:lead,b:lead"]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("duplicate operator names must be rejected"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("duplicate operator name"),
            "{error}"
        );
    }

    #[test]
    fn duplicate_trading_participants_are_rotation_not_ambiguity() {
        // Two trading tokens may share one participant while clients roll from
        // the old token to the new one: attribution sees one desk either way,
        // and no lookup becomes order-dependent because the mapped identity
        // is identical.
        let cli = Cli::parse_from(["test", "--api-tokens", "old-token:desk,new-token:desk"]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.participant_for("old-token"), Some("desk"));
        assert_eq!(config.participant_for("new-token"), Some("desk"));

        // The same holds rolling from the legacy token to a scoped token.
        let cli = Cli::parse_from([
            "test",
            "--api-token",
            "old-token",
            "--api-participant",
            "desk",
            "--api-tokens",
            "new-token:desk",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.participant_for("old-token"), Some("desk"));
        assert_eq!(config.participant_for("new-token"), Some("desk"));
    }

    #[test]
    fn an_empty_iggy_password_fails_for_the_iggy_journal_only() {
        let cli = Cli::parse_from(["test", "--iggy-password", ""]);
        assert!(NodeConfig::from_args(cli.node).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("password");
        std::fs::write(&path, "\n").unwrap();
        let cli = Cli::parse_from(["test", "--iggy-password-file", path.to_str().unwrap()]);
        assert!(NodeConfig::from_args(cli.node).is_err());

        // The memory journal never dials Iggy, so it needs no password.
        let cli = Cli::parse_from(["test", "--journal", "memory", "--iggy-password", ""]);
        assert!(NodeConfig::from_args(cli.node).is_ok());
    }

    #[test]
    fn a_malformed_iggy_address_fails_before_any_bind() {
        let cli = Cli::parse_from(["test", "--iggy-addr", "not a host:port"]);
        let error = match NodeConfig::from_args(cli.node) {
            Ok(_) => panic!("a malformed Iggy address must be rejected"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("invalid Iggy address"),
            "{error}"
        );

        // The memory journal never dials Iggy, so it accepts any placeholder.
        let cli = Cli::parse_from([
            "test",
            "--journal",
            "memory",
            "--iggy-addr",
            "not a host:port",
        ]);
        assert!(NodeConfig::from_args(cli.node).is_ok());

        // Pure shape check without DNS: empty and port-0 fail.
        let mut config = test_config();
        config.iggy_addr = "   ".into();
        assert!(config.check_invariants().is_err());
        config.iggy_addr = "127.0.0.1:0".into();
        assert!(config.check_invariants().is_err());
        config.iggy_addr = "iggy:8090".into();
        config.check_invariants().expect("hostname shape is valid");
    }

    #[test]
    fn a_zero_snapshot_cadence_is_rejected() {
        let cli = Cli::parse_from(["test", "--snapshot-every", "0"]);
        assert!(NodeConfig::from_args(cli.node).is_err());
        let mut config = test_config();
        config.snapshot_every = 0;
        assert!(config.check_invariants().is_err());
    }

    #[test]
    fn risk_limits_accept_the_full_u64_range() {
        // Non-negativity is by type (`u64`); the ingress guard multiplies in
        // `i128`, so even `u64::MAX` cannot wrap the notional computation.
        let cli = Cli::parse_from([
            "test",
            "--risk-max-order-qty",
            "18446744073709551615",
            "--risk-max-notional-cents",
            "18446744073709551615",
        ]);
        let config = NodeConfig::from_args(cli.node).unwrap();
        assert_eq!(config.risk.max_order_qty, u64::MAX);
        assert_eq!(config.risk.max_notional_cents, u64::MAX);
        config.check_invariants().expect("u64 limits are valid");
    }

    #[test]
    fn check_invariants_reuses_the_exchange_validation() {
        // Zero tick.
        let mut config = test_config();
        config.exchange.instruments[0].tick_size_cents = 0;
        assert!(config.check_invariants().is_err());

        // Duplicate symbol inside the exchange config.
        let mut config = test_config();
        let first = config.exchange.instruments[0].clone();
        config.exchange.instruments.push(first);
        let error = match config.check_invariants() {
            Ok(_) => panic!("duplicate exchange symbols must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("duplicate symbol"), "{error}");

        // Empty instrument set.
        let mut config = test_config();
        config.exchange.instruments.clear();
        assert!(config.check_invariants().is_err());

        // Partitions outside the bound on a directly built config.
        let mut config = test_config();
        config.exchange.partitions = 512;
        assert!(config.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_table() {
        type Case = (&'static str, Box<dyn Fn(&mut NodeConfig)>, bool);
        let cases: Vec<Case> = vec![
            ("baseline", Box::new(|_| {}), true),
            ("turbo on iggy", Box::new(|c| c.mode = Mode::Turbo), true),
            (
                "turbo on memory",
                Box::new(|c| {
                    c.mode = Mode::Turbo;
                    c.journal = JournalKind::Memory;
                }),
                false,
            ),
            (
                "memory durable",
                Box::new(|c| c.journal = JournalKind::Memory),
                true,
            ),
            (
                "zero partitions",
                Box::new(|c| c.exchange.partitions = 0),
                false,
            ),
            (
                "warn equals crit",
                Box::new(|c| c.storage.warn_pct = c.storage.crit_pct),
                false,
            ),
            (
                "zero snapshot cadence",
                Box::new(|c| c.snapshot_every = 0),
                false,
            ),
            (
                "empty participant",
                Box::new(|c| c.api_participant.clear()),
                false,
            ),
            (
                "empty legacy token",
                Box::new(|c| c.api_token = Some(String::new())),
                false,
            ),
            (
                "duplicate scoped tokens",
                Box::new(|c| {
                    c.api_tokens = vec![token_pair("t", "a"), token_pair("t", "b")];
                }),
                false,
            ),
            (
                "duplicate operator tokens",
                Box::new(|c| {
                    c.operator_tokens = vec![token_pair("t", "a"), token_pair("t", "b")];
                }),
                false,
            ),
            (
                "duplicate operator names",
                Box::new(|c| {
                    c.operator_tokens = vec![token_pair("a", "lead"), token_pair("b", "lead")];
                }),
                false,
            ),
            (
                "trading rotation shares a name",
                Box::new(|c| {
                    c.api_tokens = vec![token_pair("old", "desk"), token_pair("new", "desk")];
                }),
                true,
            ),
            (
                "legacy duplicates scoped token",
                Box::new(|c| {
                    c.api_token = Some("t".into());
                    c.api_tokens = vec![token_pair("t", "a")];
                }),
                false,
            ),
            (
                "token spans trading and operator",
                Box::new(|c| {
                    c.api_tokens = vec![token_pair("t", "a")];
                    c.operator_tokens = vec![token_pair("t", "ops")];
                }),
                false,
            ),
            (
                "token spans trading and metrics",
                Box::new(|c| {
                    c.api_tokens = vec![token_pair("t", "a")];
                    c.metrics_token = Some("t".into());
                }),
                false,
            ),
            (
                "empty metrics token",
                Box::new(|c| c.metrics_token = Some(String::new())),
                false,
            ),
            (
                "http equals tcp",
                Box::new(|c| c.tcp_addr = Some(c.http_addr)),
                false,
            ),
            ("tcp disabled", Box::new(|c| c.tcp_addr = None), true),
            (
                "empty iggy password",
                Box::new(|c| c.iggy_password.clear()),
                false,
            ),
            ("empty iggy user", Box::new(|c| c.iggy_user.clear()), false),
            (
                "memory ignores iggy credentials",
                Box::new(|c| {
                    c.journal = JournalKind::Memory;
                    c.iggy_password.clear();
                    c.iggy_user.clear();
                    c.iggy_addr = "bogus".into();
                }),
                true,
            ),
        ];
        for (name, mutate, expect_ok) in cases {
            let mut config = test_config();
            mutate(&mut config);
            assert_eq!(
                config.check_invariants().is_ok(),
                expect_ok,
                "case {name:?} must agree"
            );
        }
    }
}
