# syntax=docker/dockerfile:1.7

# Whippet demo image: one Rust binary plus the prebuilt dashboard.
#
# Build from the repository root:
#   docker build -f docker/Dockerfile -t whippet:0.1.0 .
#
# The default command is `whippet node`. Compose overrides the command for the
# simulator and bench services, which use the same image.

ARG RUST_VERSION=1.95
ARG NODE_VERSION=25
ARG PNPM_VERSION=10.23.0

# ---------------------------------------------------------------------------
# Rust builder
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS rust-builder

# aws-lc-sys (pulled in by rustls) builds with CMake. The official Rust image
# has gcc, perl and pkg-config but not cmake.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy manifests first so dependency compilation is cached independently of
# source edits.
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p whippet --bin whippet \
    && cp /src/target/release/whippet /usr/local/bin/whippet

# ---------------------------------------------------------------------------
# Dashboard builder
# ---------------------------------------------------------------------------
FROM node:${NODE_VERSION}-bookworm-slim AS dashboard-builder

ARG PNPM_VERSION
WORKDIR /dashboard

RUN npm install --global "pnpm@${PNPM_VERSION}"

COPY apps/dashboard/package.json apps/dashboard/pnpm-lock.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile

COPY apps/dashboard ./
RUN pnpm build

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 whippet \
    && useradd --system --uid 10001 --gid whippet \
        --home-dir /app --shell /usr/sbin/nologin whippet

WORKDIR /app

COPY --from=rust-builder /usr/local/bin/whippet /usr/local/bin/whippet
COPY --from=dashboard-builder /dashboard/dist /app/dashboard
COPY docker/entrypoint.sh /usr/local/bin/whippet-entrypoint.sh

RUN chmod 0755 /usr/local/bin/whippet /usr/local/bin/whippet-entrypoint.sh \
    && mkdir -p /data/snapshots \
    && chown -R whippet:whippet /app /data

USER whippet

ENV WHIPPET_HTTP_ADDR=0.0.0.0:8080 \
    WHIPPET_DASHBOARD_DIR=/app/dashboard \
    WHIPPET_SNAPSHOT_DIR=/data/snapshots \
    RUST_LOG=whippet=info,whippet_engine=info,whippet_journal=info,whippet_marketdata=info,whippet_bench=info

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=6 \
    CMD ["whippet", "healthcheck", "--gateway", "http://127.0.0.1:8080"]

ENTRYPOINT ["/usr/local/bin/whippet-entrypoint.sh"]
CMD ["node"]
