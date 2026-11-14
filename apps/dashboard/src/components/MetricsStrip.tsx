import { formatCount, formatLatency, formatRate, formatUptime } from '../lib/format';
import type { MetricsView } from '../lib/types';

function Metric({
  label,
  value,
  detail,
  tone = 'neutral',
}: {
  label: string;
  value: string;
  detail?: string;
  tone?: 'neutral' | 'good' | 'warn' | 'bad';
}) {
  return (
    <div className={`metric metric-${tone}`}>
      <span className="metric-label">{label}</span>
      <span className="metric-value">{value}</span>
      {detail ? <span className="metric-detail">{detail}</span> : null}
    </div>
  );
}

export function MetricsStrip({ metrics }: { metrics: MetricsView | null }) {
  if (!metrics) {
    return (
      <section className="metrics-strip" aria-label="Live metrics">
        {Array.from({ length: 6 }).map((_, index) => (
          <div className="metric metric-skeleton" key={index}>
            <span className="metric-label">waiting for state</span>
            <span className="metric-value">—</span>
          </div>
        ))}
      </section>
    );
  }

  return (
    <section className="metrics-strip" aria-label="Live metrics">
      <Metric
        label="Order throughput"
        value={`${formatRate(metrics.throughput_orders_s)}/s`}
        detail={`${formatCount(metrics.orders_ingressed)} orders accepted by the log`}
      />
      <Metric
        label="Ingress latency"
        value={formatLatency(metrics.ingress_p99_us)}
        detail={`p99 · p50 ${formatLatency(metrics.ingress_p50_us)} · Iggy append ack`}
        tone={metrics.ingress_p99_us > 5_000 ? 'warn' : 'good'}
      />
      <Metric
        label="Match latency"
        value={formatLatency(metrics.match_p99_us)}
        detail={`p99 · p50 ${formatLatency(metrics.match_p50_us)} · engine only`}
        tone={metrics.match_p99_us > 1_000 ? 'warn' : 'good'}
      />
      <Metric
        label="Executions"
        value={formatCount(metrics.executions)}
        detail={`${formatCount(metrics.trades)} trades · ${formatCount(metrics.orders_rejected)} rejected`}
      />
      <Metric
        label="MD fanout"
        value={formatCount(metrics.md_events)}
        detail={`${metrics.partition_count} partitions · consumer lag ${formatCount(metrics.consumer_lag)}`}
      />
      <Metric
        label="Node"
        value={formatUptime(metrics.uptime_s)}
        detail={metrics.iggy_connected ? 'Iggy connected' : 'Iggy unreachable'}
        tone={metrics.iggy_connected ? 'good' : 'bad'}
      />
    </section>
  );
}
