// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::warn;
use whippet_core::api::{
    ApiError, CancelAck, CancelRequest, HealthView, OrderAck, OrderRequest, StateView, TradeView,
    WsClientMessage, WsServerMessage,
};
use whippet_core::headers::{MessageMeta, SCHEMA_ORDER};
use whippet_core::ids::{OrderId, ParticipantId, Price, Seq};
use whippet_core::protocol::Command;
use whippet_journal::{JournalMessage, Topic};

use crate::context::NodeContext;

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

pub fn router(ctx: Arc<NodeContext>) -> Router {
    let api = Router::new()
        .route("/api/orders", post(submit_order))
        .route("/api/cancel", post(cancel_order))
        .route("/api/state", get(state))
        .route("/api/health", get(health))
        .route("/api/instruments", get(instruments))
        .route("/api/trades", get(trades))
        .route("/metrics", get(prometheus))
        .route("/ws", get(ws_upgrade));

    let dashboard_dir = ctx.config.dashboard_dir.clone();
    let index = dashboard_dir.join("index.html");
    let api = if index.exists() {
        api.fallback_service(ServeDir::new(&dashboard_dir).not_found_service(ServeFile::new(index)))
    } else {
        api.fallback(missing_dashboard)
    };

    api.layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(ctx)
}

async fn submit_order(
    State(ctx): State<Arc<NodeContext>>,
    Json(request): Json<OrderRequest>,
) -> ApiResult<OrderAck> {
    let started = Instant::now();
    let instrument = ctx.exchange().instrument(request.symbol).ok_or_else(|| {
        bad_request(
            "unknown_symbol",
            format!("unknown symbol {}", request.symbol),
        )
    })?;
    if request.qty == 0 {
        return Err(bad_request("invalid_qty", "qty must be greater than zero"));
    }
    if request.qty > 1_000_000_000 {
        return Err(bad_request("invalid_qty", "qty is unreasonably large"));
    }

    let price = if request.order_type.requires_price() {
        let cents = request.price_cents.ok_or_else(|| {
            bad_request(
                "missing_price",
                "price_cents is required for this order type",
            )
        })?;
        instrument
            .ticks_from_cents(cents)
            .map_err(|error| bad_request("invalid_price", error.to_string()))?
    } else {
        Price::ZERO
    };

    let client_order_id = request
        .client_order_id
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let order_id = OrderId::derive(request.symbol, &client_order_id);
    let participant_name = request
        .participant
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "anonymous".into());
    let participant = ParticipantId::derive(&participant_name);
    let timestamp_ns = now_ns();
    let command = Command::new_order(
        request.symbol,
        request.side,
        request.order_type,
        price,
        whippet_core::ids::Qty(request.qty),
        order_id,
        participant,
        timestamp_ns,
    );
    let partition = request.symbol.partition(ctx.exchange().partitions);
    let meta = MessageMeta::new(request.symbol, Seq(0), "order", SCHEMA_ORDER, order_id.0);
    let message = JournalMessage::new(command.encode(), meta.pairs());
    let append = ctx
        .journal
        .append(Topic::Orders, partition, &[message])
        .await
        .map_err(journal_unavailable)?;
    let elapsed = started.elapsed();
    ctx.metrics.record_ingress();
    ctx.metrics
        .record_ingress_latency_us(elapsed.as_micros() as u64);

    Ok(Json(OrderAck {
        order_id,
        symbol: request.symbol,
        partition: append.partition,
        offset: append.base_offset,
        mode: ctx.config.mode,
        status: "accepted_by_log".into(),
        accepted_at_ns: timestamp_ns,
        durable_ack_ms: elapsed.as_secs_f64() * 1_000.0,
    }))
}

async fn cancel_order(
    State(ctx): State<Arc<NodeContext>>,
    Json(request): Json<CancelRequest>,
) -> ApiResult<CancelAck> {
    ctx.exchange().instrument(request.symbol).ok_or_else(|| {
        bad_request(
            "unknown_symbol",
            format!("unknown symbol {}", request.symbol),
        )
    })?;
    let participant_name = request
        .participant
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "anonymous".into());
    let participant = ParticipantId::derive(&participant_name);
    let timestamp_ns = now_ns();
    let command = Command::cancel(request.symbol, request.order_id, participant, timestamp_ns);
    let partition = request.symbol.partition(ctx.exchange().partitions);
    let meta = MessageMeta::new(
        request.symbol,
        Seq(0),
        "cancel",
        SCHEMA_ORDER,
        request.order_id.0,
    );
    let message = JournalMessage::new(command.encode(), meta.pairs());
    let append = ctx
        .journal
        .append(Topic::Orders, partition, &[message])
        .await
        .map_err(journal_unavailable)?;
    Ok(Json(CancelAck {
        order_id: request.order_id,
        symbol: request.symbol,
        partition: append.partition,
        offset: append.base_offset,
        mode: ctx.config.mode,
        status: "cancel_accepted_by_log".into(),
    }))
}

async fn state(State(ctx): State<Arc<NodeContext>>) -> Json<StateView> {
    Json(ctx.hub.state())
}

async fn health(State(ctx): State<Arc<NodeContext>>) -> Json<HealthView> {
    let metrics = ctx.metrics.snapshot();
    Json(HealthView {
        status: if metrics.iggy_connected {
            "ok".into()
        } else {
            "degraded".into()
        },
        mode: ctx.config.mode,
        iggy_connected: metrics.iggy_connected,
        iggy_addr: ctx.config.iggy_addr.clone(),
        journal: ctx.config.journal.as_str().to_string(),
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_s: ctx.started.elapsed().as_secs(),
        partitions: ctx.exchange().partitions,
    })
}

async fn instruments(
    State(ctx): State<Arc<NodeContext>>,
) -> Json<whippet_core::config::ExchangeConfig> {
    Json(ctx.exchange().clone())
}

async fn prometheus(State(ctx): State<Arc<NodeContext>>) -> impl IntoResponse {
    let state = ctx.hub.state();
    let body = prometheus_text(&state.metrics, state.freshness_ms, state.sequence);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

/// Prometheus exposition. Counters are monotonic; percentiles are gauges
/// because they describe the recent process window, not a cumulative sum.
pub(crate) fn prometheus_text(
    metrics: &whippet_core::api::MetricsView,
    freshness_ms: u64,
    sequence: u64,
) -> String {
    let mut out = String::with_capacity(2_048);
    out.push_str(
        "# HELP whippet_orders_ingressed_total Orders accepted by the gateway and appended to Iggy.\n",
    );
    out.push_str("# TYPE whippet_orders_ingressed_total counter\n");
    out.push_str(&format!(
        "whippet_orders_ingressed_total {}\n",
        metrics.orders_ingressed
    ));
    out.push_str("# HELP whippet_orders_matched_total Orders accepted by the matcher.\n");
    out.push_str("# TYPE whippet_orders_matched_total counter\n");
    out.push_str(&format!(
        "whippet_orders_matched_total {}\n",
        metrics.orders_matched
    ));
    out.push_str(
        "# HELP whippet_orders_rejected_total Orders rejected by validation or matching.\n",
    );
    out.push_str("# TYPE whippet_orders_rejected_total counter\n");
    out.push_str(&format!(
        "whippet_orders_rejected_total {}\n",
        metrics.orders_rejected
    ));
    out.push_str("# HELP whippet_executions_total Execution reports produced.\n");
    out.push_str("# TYPE whippet_executions_total counter\n");
    out.push_str(&format!(
        "whippet_executions_total {}\n",
        metrics.executions
    ));
    out.push_str("# HELP whippet_trades_total Trades observed by the MD consumer.\n");
    out.push_str("# TYPE whippet_trades_total counter\n");
    out.push_str(&format!("whippet_trades_total {}\n", metrics.trades));
    out.push_str("# HELP whippet_md_events_total Market-data records applied by the hub.\n");
    out.push_str("# TYPE whippet_md_events_total counter\n");
    out.push_str(&format!("whippet_md_events_total {}\n", metrics.md_events));
    out.push_str("# HELP whippet_engine_commands_total Input commands applied by the matcher.\n");
    out.push_str("# TYPE whippet_engine_commands_total counter\n");
    out.push_str(&format!(
        "whippet_engine_commands_total {}\n",
        metrics.engine_commands
    ));
    out.push_str("# HELP whippet_consumer_lag Records waiting behind the engine consumer.\n");
    out.push_str("# TYPE whippet_consumer_lag gauge\n");
    out.push_str(&format!("whippet_consumer_lag {}\n", metrics.consumer_lag));
    out.push_str("# HELP whippet_ingress_latency_microseconds Gateway append latency.\n");
    out.push_str("# TYPE whippet_ingress_latency_microseconds gauge\n");
    out.push_str(&format!(
        "whippet_ingress_latency_microseconds{{quantile=\"0.5\"}} {}\n",
        metrics.ingress_p50_us
    ));
    out.push_str(&format!(
        "whippet_ingress_latency_microseconds{{quantile=\"0.99\"}} {}\n",
        metrics.ingress_p99_us
    ));
    out.push_str("# HELP whippet_match_latency_microseconds Engine dequeue to execution events.\n");
    out.push_str("# TYPE whippet_match_latency_microseconds gauge\n");
    out.push_str(&format!(
        "whippet_match_latency_microseconds{{quantile=\"0.5\"}} {}\n",
        metrics.match_p50_us
    ));
    out.push_str(&format!(
        "whippet_match_latency_microseconds{{quantile=\"0.99\"}} {}\n",
        metrics.match_p99_us
    ));
    out.push_str("# HELP whippet_throughput_orders_per_second Accepted orders per second.\n");
    out.push_str("# TYPE whippet_throughput_orders_per_second gauge\n");
    out.push_str(&format!(
        "whippet_throughput_orders_per_second {}\n",
        metrics.throughput_orders_s
    ));
    out.push_str("# HELP whippet_uptime_seconds Node uptime.\n");
    out.push_str("# TYPE whippet_uptime_seconds counter\n");
    out.push_str(&format!("whippet_uptime_seconds {}\n", metrics.uptime_s));
    out.push_str("# HELP whippet_iggy_connected Whether the last Iggy health check succeeded.\n");
    out.push_str("# TYPE whippet_iggy_connected gauge\n");
    out.push_str(&format!(
        "whippet_iggy_connected {}\n",
        u8::from(metrics.iggy_connected)
    ));
    out.push_str("# HELP whippet_partitions Configured trading partitions.\n");
    out.push_str("# TYPE whippet_partitions gauge\n");
    out.push_str(&format!("whippet_partitions {}\n", metrics.partition_count));
    out.push_str("# HELP whippet_mode Operating mode (one-hot).\n");
    out.push_str("# TYPE whippet_mode gauge\n");
    out.push_str(&format!(
        "whippet_mode{{mode=\"{}\"}} 1\n",
        metrics.mode.as_str()
    ));
    out.push_str("# HELP whippet_md_freshness_milliseconds Age of the newest MD event.\n");
    out.push_str("# TYPE whippet_md_freshness_milliseconds gauge\n");
    out.push_str(&format!(
        "whippet_md_freshness_milliseconds {freshness_ms}\n"
    ));
    out.push_str("# HELP whippet_md_sequence Newest per-symbol MD sequence observed.\n");
    out.push_str("# TYPE whippet_md_sequence gauge\n");
    out.push_str(&format!("whippet_md_sequence {sequence}\n"));
    out
}

#[derive(Debug, Deserialize)]
struct TradesQuery {
    #[serde(default = "default_trade_limit")]
    limit: usize,
}

fn default_trade_limit() -> usize {
    100
}

async fn trades(
    State(ctx): State<Arc<NodeContext>>,
    Query(query): Query<TradesQuery>,
) -> Json<Vec<TradeView>> {
    let state = ctx.hub.state();
    let limit = query.limit.min(state.trades.len());
    Json(state.trades.into_iter().take(limit).collect())
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(ctx): State<Arc<NodeContext>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_loop(socket, ctx))
}

async fn ws_loop(mut socket: WebSocket, ctx: Arc<NodeContext>) {
    let mut receiver = ctx.hub.subscribe();
    if send_json(
        &mut socket,
        &WsServerMessage::State {
            state: ctx.hub.state(),
        },
    )
    .await
    .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            changed = receiver.changed() => {
                if changed.is_err() {
                    break;
                }
                let state = receiver.borrow().clone();
                if send_json(&mut socket, &WsServerMessage::State { state: (*state).clone() }).await.is_err() {
                    break;
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(WsClientMessage::Ping { sent_at_ns }) = serde_json::from_str(&text) {
                            let message = WsServerMessage::Notice {
                                level: "pong".into(),
                                message: sent_at_ns.to_string(),
                            };
                            if send_json(&mut socket, &message).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(error = %error, "websocket receive error");
                        break;
                    }
                }
            }
        }
    }
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, value: &T) -> Result<(), ()> {
    let text = serde_json::to_string(value).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

async fn missing_dashboard() -> Html<&'static str> {
    Html(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>Whippet</title>
<style>body{font-family:ui-monospace,monospace;background:#0b0e13;color:#d8dee9;padding:3rem;line-height:1.6}
code{color:#7dd3fc}a{color:#a7f3d0}</style></head><body>
<h1>Whippet API is running</h1>
<p>The dashboard assets were not found. Build them with:</p>
<pre><code>cd apps/dashboard &amp;&amp; pnpm install &amp;&amp; pnpm build</code></pre>
<p>Then restart the node, or set <code>WHIPPET_DASHBOARD_DIR</code>.</p>
<p>API: <a href="/api/state">/api/state</a> · <a href="/api/health">/api/health</a></p>
</body></html>"#,
    )
}

fn bad_request(code: &str, message: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    (StatusCode::BAD_REQUEST, Json(ApiError::new(code, message)))
}

fn journal_unavailable(error: whippet_journal::JournalError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError::new("journal_unavailable", error.to_string())),
    )
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::api::MetricsView;
    use whippet_core::config::Mode;

    #[test]
    fn prometheus_text_contains_core_series() {
        let metrics = MetricsView::empty(Mode::Durable, 16);
        let text = prometheus_text(&metrics, 42, 7);
        assert!(text.contains("whippet_orders_ingressed_total 0"));
        assert!(text.contains("whippet_mode{mode=\"durable\"} 1"));
        assert!(text.contains("whippet_md_freshness_milliseconds 42"));
        assert!(text.contains("whippet_md_sequence 7"));
        assert!(text.contains("# TYPE whippet_ingress_latency_microseconds gauge"));
    }
}
