import { useMemo } from 'react';
import { Link } from '@tanstack/react-router';
import type uPlot from 'uplot';
import { formatLatency, formatRate } from '../lib/format';
import type { FeedSample } from '../lib/types';
import { UPlotChart } from './UPlotChart';
import type { ChartSeries } from './UPlotChart';

const latencySeries: ChartSeries[] = [
  { label: 'ingress p50', stroke: '#5eead4', width: 1.5 },
  { label: 'ingress p99', stroke: '#fbbf24', width: 1.8 },
  { label: 'match p99', stroke: '#a78bfa', width: 1.4, dash: [5, 4] },
];

const throughputSeries: ChartSeries[] = [
  { label: 'orders/s', stroke: '#7dd3fc', width: 1.6, fill: 'rgba(125, 211, 252, 0.12)' },
];

export function LatencyChart({ history }: { history: FeedSample[] }) {
  const data = useMemo(
    () =>
      [
        history.map((sample) => sample.t / 1000),
        history.map((sample) => sample.p50),
        history.map((sample) => sample.p99),
        history.map((sample) => sample.matchP99),
      ] as uPlot.AlignedData,
    [history],
  );
  return (
    <section className="panel chart-panel">
      <div className="panel-head">
        <span>Latency over time</span>
        <span className="panel-meta">
          <span className="dev-loop">dev-loop</span> · µs ·{' '}
          <Link to="/bench" className="inline-link">
            bench method
          </Link>
        </span>
      </div>
      <UPlotChart
        data={data}
        series={latencySeries}
        height={210}
        yFormat={formatLatency}
        ariaLabel="Ingress and matcher latency over time, in microseconds"
      />
      <p className="chart-note">
        Ingress is measured around the durable Iggy append. Match is engine dequeue → execution events.
        This is the dev-loop path; the five-repetition protocol and manifests are on the{' '}
        <Link to="/bench" className="inline-link">
          Bench page
        </Link>
        .
      </p>
    </section>
  );
}

export function ThroughputChart({ history }: { history: FeedSample[] }) {
  const data = useMemo(
    () =>
      [
        history.map((sample) => sample.t / 1000),
        history.map((sample) => sample.throughput),
      ] as uPlot.AlignedData,
    [history],
  );
  return (
    <section className="panel chart-panel">
      <div className="panel-head">
        <span>Order throughput</span>
        <span className="panel-meta">
          <span className="dev-loop">dev-loop</span> · accepted orders/s
        </span>
      </div>
      <UPlotChart
        data={data}
        series={throughputSeries}
        height={210}
        yFormat={(value) => `${formatRate(value)}/s`}
        ariaLabel="Accepted order throughput over time, in orders per second"
      />
      <p className="chart-note">
        Open-loop load from the simulator or bench harness. If the generator falls behind, the benchmark
        manifest records scheduler slip instead of hiding it.
      </p>
    </section>
  );
}
