// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Args;
use reqwest::Client;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use whippet_core::api::OrderRequest;
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::Symbol;
use whippet_sim::{Scenario, SimAction, SimConfig, Simulator};

#[derive(Args, Debug, Clone)]
pub struct SimArgs {
    #[arg(long, env = "WHIPPET_GATEWAY", default_value = "http://127.0.0.1:8080")]
    pub gateway: String,
    #[arg(long, default_value_t = 400)]
    pub rate: u64,
    /// Zero means run until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    pub duration: u64,
    #[arg(long, default_value = "calm_open")]
    pub scenario: String,
    /// Omit for a fresh demo run. Pass a value for a reproducible run.
    #[arg(long)]
    pub seed: Option<u64>,
    /// Extra nonce for a reproducible run; defaults to 0 when --seed is given.
    #[arg(long)]
    pub run_nonce: Option<u64>,
    #[arg(
        long,
        default_value = "BTC-USD,ETH-USD,SOL-USD",
        env = "WHIPPET_SYMBOLS"
    )]
    pub symbols: String,
    #[arg(long, default_value_t = 8)]
    pub participants: u64,
    #[arg(long, default_value_t = 64)]
    pub max_in_flight: usize,
}

pub async fn run(args: SimArgs) -> Result<()> {
    if args.rate == 0 {
        anyhow::bail!("--rate must be positive");
    }
    let exchange = ExchangeConfig::default();
    let symbols: Vec<Symbol> = args
        .symbols
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Symbol::new)
        .collect::<std::result::Result<_, _>>()
        .context("parsing --symbols")?;
    let scenario: Scenario = args.scenario.parse()?;
    let seed = args.seed.unwrap_or_else(now_ns);
    let run_nonce = args.run_nonce.unwrap_or_else(|| {
        if args.seed.is_some() {
            0
        } else {
            now_ns() ^ u64::from(std::process::id())
        }
    });
    let config = SimConfig {
        seed,
        run_nonce,
        rate_per_sec: args.rate,
        duration_secs: args.duration,
        scenario,
        participants: args.participants,
        symbols,
    };
    let mut simulator = Simulator::new(config, &exchange);
    let client = Client::builder()
        .pool_max_idle_per_host(128)
        .timeout(Duration::from_secs(5))
        .build()?;
    let semaphore = Arc::new(Semaphore::new(args.max_in_flight.max(1)));
    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let base = args.gateway.trim_end_matches('/').to_string();
    let period = Duration::from_secs_f64(1.0 / args.rate as f64);
    let start = Instant::now();
    let mut tick = 0u64;
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    let mut last_report = Instant::now();

    info!(
        rate = args.rate,
        scenario = scenario.as_str(),
        seed,
        run_nonce,
        gateway = %base,
        "simulator started"
    );

    loop {
        let deadline = start + period.saturating_mul(tick as u32);
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = tokio::time::sleep_until(deadline.into()) => {}
        }
        tick = tick.wrapping_add(1);

        let Some(action) = simulator.next_action() else {
            break;
        };
        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let client = client.clone();
        let base = base.clone();
        let sent_task = Arc::clone(&sent);
        let errors_task = Arc::clone(&errors);
        tokio::spawn(async move {
            // Serialize straight to bytes. The old path built a
            // `serde_json::Value` and then serialized that again, so every
            // simulated action paid two JSON passes and a tree of allocations.
            let (url, body) = match &action {
                SimAction::New(order) => {
                    (format!("{base}/api/orders"), serde_json::to_vec(order).ok())
                }
                SimAction::Cancel(cancel) => (
                    format!("{base}/api/cancel"),
                    serde_json::to_vec(cancel).ok(),
                ),
            };
            if let Some(body) = body {
                match client
                    .post(url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {
                        sent_task.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(response) => {
                        errors_task.fetch_add(1, Ordering::Relaxed);
                        if errors_task.load(Ordering::Relaxed) <= 5 {
                            warn!(status = %response.status(), "gateway rejected a simulated action");
                        }
                    }
                    Err(error) => {
                        errors_task.fetch_add(1, Ordering::Relaxed);
                        if errors_task.load(Ordering::Relaxed) <= 5 {
                            warn!(error = %error, "simulator request failed");
                        }
                    }
                }
            }
            drop(permit);
        });

        if last_report.elapsed() >= Duration::from_secs(5) {
            info!(
                sent = sent.load(Ordering::Relaxed),
                errors = errors.load(Ordering::Relaxed),
                dropped = dropped.load(Ordering::Relaxed),
                open_orders = simulator.open_orders(),
                elapsed_s = start.elapsed().as_secs(),
                "simulator progress"
            );
            last_report = Instant::now();
        }
        if args.duration > 0 && start.elapsed() >= Duration::from_secs(args.duration) {
            break;
        }
    }

    let _ = semaphore.acquire_many(args.max_in_flight as u32).await;
    info!(
        sent = sent.load(Ordering::Relaxed),
        errors = errors.load(Ordering::Relaxed),
        dropped = dropped.load(Ordering::Relaxed),
        "simulator stopped"
    );
    Ok(())
}

/// Shared by the demo script and tests that need a deterministic order body.
pub fn example_order() -> OrderRequest {
    OrderRequest {
        symbol: Symbol::new("BTC-USD").expect("static symbol"),
        side: whippet_core::protocol::Side::Buy,
        order_type: whippet_core::protocol::OrderType::Limit,
        price_cents: Some(6_500_000),
        qty: 10,
        client_order_id: Some("example-1".into()),
        participant: Some("demo".into()),
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}
