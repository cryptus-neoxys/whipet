// Licensed under the Apache License, Version 2.0. See LICENSE.

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "whippet",
    version,
    about = "Whippet: sub-millisecond matching, millisecond-durable streaming on Apache Iggy",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

// The CLI is parsed once at startup; the enum size is irrelevant, and boxing
// `NodeArgs` would only add an indirection to the one path that runs a node.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand)]
enum Commands {
    /// Run the exchange node: gateway, engine shards, journal, and dashboard API.
    Node(whippet::config::NodeArgs),
    /// Run the deterministic market simulator against a node.
    Sim(whippet::sim_runner::SimArgs),
    /// Run the open-loop benchmark harness.
    Bench(whippet::cli::BenchArgs),
    /// Check the node health endpoint; exits non-zero when degraded.
    Healthcheck(whippet::cli::HealthArgs),
    /// Verify snapshot state hashes on disk.
    Verify(whippet::cli::VerifyArgs),
    /// Replay the order log for one partition and print the deterministic state hash.
    Replay(whippet::cli::ReplayArgs),
    /// Read-only preflight: Iggy topics, snapshot hashes, and node health.
    Doctor(whippet::cli::DoctorArgs),
    /// Plan the oldest order-log offset that is safe to delete per partition. Read-only.
    RetentionPlan(whippet::cli::RetentionPlanArgs),
    /// List the control-plane audit records written by halt/resume. Read-only.
    Audit(whippet::cli::AuditArgs),
    /// Reconstruct one order's timeline from the orders and executions logs. Read-only.
    OrderTrace(whippet::cli::OrderTraceArgs),
    /// Print educational surveillance indicators per participant. Read-only.
    Surveillance(whippet::cli::SurveillanceArgs),
    /// Reconstruct per-participant positions and cash flow from the logs. Read-only.
    Positions(whippet::cli::PositionsArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new(
                    "whippet=info,whippet_engine=info,whippet_journal=info,whippet_marketdata=info,whippet_bench=info",
                )
            }),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Node(args) => whippet::node::run(args).await,
        Commands::Sim(args) => whippet::sim_runner::run(args).await,
        Commands::Bench(args) => whippet::cli::run_bench(args).await,
        Commands::Healthcheck(args) => whippet::cli::run_health(args).await,
        Commands::Verify(args) => whippet::cli::run_verify(args),
        Commands::Replay(args) => whippet::cli::run_replay(args).await,
        Commands::Doctor(args) => whippet::cli::run_doctor(args).await,
        Commands::RetentionPlan(args) => whippet::cli::run_retention_plan(args).await,
        Commands::Audit(args) => whippet::cli::run_audit(args).await,
        Commands::OrderTrace(args) => whippet::cli::run_order_trace(args).await,
        Commands::Surveillance(args) => whippet::cli::run_surveillance(args).await,
        Commands::Positions(args) => whippet::cli::run_positions(args).await,
    }
}
