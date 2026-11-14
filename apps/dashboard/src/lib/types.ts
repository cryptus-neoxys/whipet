export type Mode = 'durable' | 'turbo';
export type Side = 'buy' | 'sell';
export type OrderType = 'limit' | 'market' | 'ioc' | 'fok' | 'post_only';

export interface LevelView {
  price_cents: number;
  qty: number;
  orders: number;
}

export interface TradeView {
  symbol: string;
  price_cents: number;
  qty: number;
  side: Side;
  order_id: string;
  ts_ns: number;
  seq: number;
}

export interface BookView {
  symbol: string;
  seq: number;
  bids: LevelView[];
  asks: LevelView[];
  best_bid_cents: number | null;
  best_ask_cents: number | null;
  spread_cents: number | null;
  mid_cents: number | null;
  last_trade_price_cents: number | null;
  last_trade_qty: number;
  last_trade_side: Side | null;
  depth_bid_qty: number;
  depth_ask_qty: number;
}

export interface MetricsView {
  mode: Mode;
  orders_ingressed: number;
  orders_matched: number;
  orders_rejected: number;
  executions: number;
  trades: number;
  md_events: number;
  engine_commands: number;
  consumer_lag: number;
  ingress_p50_us: number;
  ingress_p99_us: number;
  match_p50_us: number;
  match_p99_us: number;
  throughput_orders_s: number;
  uptime_s: number;
  iggy_connected: boolean;
  partition_count: number;
}

export interface StateView {
  mode: Mode;
  venue: string;
  symbols: BookView[];
  trades: TradeView[];
  metrics: MetricsView;
  freshness_ms: number;
  sequence: number;
  iggy_addr: string;
  journal: string;
  guarantee: string;
  dashboard_build: string | null;
}

export interface HealthView {
  status: string;
  mode: Mode;
  iggy_connected: boolean;
  iggy_addr: string;
  journal: string;
  shards_ok: boolean;
  auth_required: boolean;
  version: string;
  uptime_s: number;
  partitions: number;
}

export interface OrderRequest {
  symbol: string;
  side: Side;
  order_type: OrderType;
  price_cents?: number | null;
  qty: number;
  client_order_id?: string;
  participant?: string;
}

export interface BatchOrderRequest {
  orders: OrderRequest[];
}

export interface BatchOrderAck {
  partition: number;
  base_offset: number;
  count: number;
  mode: Mode;
  durable_ack_ms: number;
  orders: OrderAck[];
}

export interface OrderAck {
  order_id: string;
  symbol: string;
  partition: number;
  offset: number;
  mode: Mode;
  status: string;
  accepted_at_ns: number;
  durable_ack_ms: number;
}

export interface ApiError {
  code: string;
  error: string;
}

export type WsServerMessage =
  | { type: 'state'; state: StateView }
  | { type: 'trade'; trade: TradeView }
  | { type: 'metrics'; metrics: MetricsView }
  | { type: 'notice'; level: string; message: string };

export interface FeedSample {
  t: number;
  p50: number;
  p99: number;
  matchP99: number;
  throughput: number;
  trades: number;
}
