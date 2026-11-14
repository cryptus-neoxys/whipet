# 005 — QUIC connection strings cannot set the TLS server name, so certificate validation fails for remote hosts

**Status:** coordinate with #4872 first; file after it merges
**Type:** Bug report
**Area:** Rust SDK / QUIC / TLS
**Priority:** P2
**Effort:** S–M
**Duplicate check:** searched `server_name`, `QUIC TLS`, `QUIC certificate`, `validate_certificate`, `hostname` in issues and PRs. No duplicate issue. A reviewer already raised this on PR #4872 (discussion `r4550250089`), and #4210/#4883 cover connection-string option parity, not this specific field.

---

## GitHub issue body

### Bug description

The QUIC client hardcodes `server_name: "localhost"` when it is constructed from a connection string, and `QuicConnectionStringOptions` has no field for the TLS server name. With `validate_certificate=true`, a certificate issued for `broker.example.com` fails hostname verification. Users must drop to the builder API and manually construct the client, or disable certificate validation, which is unsafe.

### Affected area/component

- Rust SDK, QUIC transport, TLS configuration
- Connection-string parsing

### Deployment

QUIC with TLS, remote server or a certificate whose SAN is not `localhost`.

### Versions

Verified against upstream `master` `c78da20` (2026-11-14); the QUIC client config and connection-string parser are unchanged from the fork checkout.

### Reproduction

1. Start a TLS-enabled Iggy server whose certificate SAN is `broker.example.com` (not `localhost`).
2. Connect with a QUIC connection string that enables certificate validation.
3. The connection fails hostname verification.
4. The workaround is to parse the connection string, convert it to `QuicClientConfig`, set the public `server_name` field, and create a `QuicClient` directly. `IggyClient::builder_from_connection_string` cannot express this: it builds the client immediately, and `.with_quic()` starts from `QuicClientConfigBuilder::default()`, discarding the URL's options.

### Expected behavior

A QUIC connection string can specify the TLS server name. The default should be sensible for remote hosts: use the connection-string hostname when it is a DNS name, keep `localhost` when the host is an IP or the option is absent, and allow an explicit override.

### Proposed solution

- Add a server-name key to `QuicConnectionStringOptions`, using `tls_domain` for parity with TCP (or `tls_server_name` if the maintainers prefer a QUIC-specific name), parse it, and propagate it into `QuicClientConfig`.
- Default to the connection-string hostname when it is a DNS name; keep `localhost` for IP literals or an absent host to preserve existing local examples.
- Document the option alongside the other QUIC TLS connection-string keys.
- Add unit tests for the default, explicit value, malformed value, and propagation; add an integration test with a non-localhost SAN.

### Alternatives considered

- **Use the builder API.** It works, but connection strings are the documented zero-config path, and the other transports expose hostnames. This is an ergonomics and parity bug.
- **Disable certificate validation.** Unsafe; not acceptable.
- **Hardcode the host from the connection string’s address.** The address may be an IP or a load balancer, and the certificate name can differ. An explicit option with a host-derived default is clearer.

### Additional context

- `QuicClientConfig` hardcodes `server_name: "localhost"` in both `Default` and `From<ConnectionString>` (`core/common/src/types/configuration/quic_config/quic_client_config.rs:67,90`).
- `QuicConnectionStringOptions` has no `server_name` field (`core/common/src/types/configuration/quic_config/quic_connection_string_options.rs:27-38`).
- The QUIC client chooses between the platform verifier and `SkipServerVerification` (`core/sdk/src/quic/quic_client.rs:153-161`). A public-CA certificate for `broker.example.com` will validate through the platform trust store once `server_name` is set; a private CA still needs the OS store or a new CA option.
- PR #4872 standardizes connection-string keys across TCP/QUIC/WebSocket, but its documentation recommends `QuicClientConfigBuilder::with_server_name`, which discards the connection string's address and options. That is the gap this issue addresses.

### Contribution

I can implement the option, the parser, and the tests after #4872 merges or in coordination with its author. I will wait for assignment.

---

## Internal notes (do not paste)

### Why coordinate first

PR #4872 is open with `CHANGES_REQUESTED` and already touches the QUIC connection-string parser and TLS propagation. Filing a separate issue now risks conflicting with the in-flight parser changes. The clean path is:

1. Comment on #4872 now: its doc recommendation (`QuicClientConfigBuilder::with_server_name`) silently drops the connection-string address/options, and the connection-string path has no server-name key.
2. Ask whether `tls_domain`/`tls_server_name` can be included in #4872 or should be a follow-up.
3. If it is a follow-up, file this issue after #4872 merges.
4. If the PR author prefers, open the issue now and reference #4872 as a dependency.

### Files to touch

- `core/common/src/types/configuration/quic_config/quic_connection_string_options.rs`
- `core/common/src/types/configuration/quic_config/quic_client_config.rs`
- `core/sdk/src/quic/quic_client.rs` (if the verifier/CA path needs adjustment)
- Connection-string docs in `core/sdk/README.md` or the QUIC client docs
- Unit tests in the QUIC config module; integration test in `core/integration/tests/sdk/` or the client lifecycle tests

### Test plan

1. Parser: default host-derived name for a DNS host, `localhost` for an IP, explicit override, malformed value rejection.
2. Builder: explicit `with_server_name` still wins.
3. Integration: TLS-enabled server with a certificate for a non-localhost SAN; connection succeeds with the option and fails without it.
4. Regression: existing localhost/self-signed QUIC examples still connect with validation disabled.

### Risk

- The QUIC client has no custom-CA option. A public CA works via the platform verifier once the name is correct; a private CA needs the OS trust store or a separate CA feature. If #4872 does not add CA support, state that this issue only fixes the name, not private-CA trust.
- Coordinate naming with #4872 to avoid two different keys (`tls_domain` vs `tls_server_name`).
- Do not describe the current behavior as a security vulnerability; it is a TLS correctness and ergonomics bug. If a real MITM bypass is found, use `SECURITY.md`, not a public issue.
