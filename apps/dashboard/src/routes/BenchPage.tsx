import { useFeed } from '../lib/feed';
import { formatLatency, formatRate } from '../lib/format';

const matrix = [
  ['L1', '64 B orders, 1 symbol', '1k → 100k orders/s', '5 min × 5'],
  ['L2', '256 B, 10 symbols', '10k, 50k orders/s', '5 min × 5'],
  ['F1', 'Fanout', '1 / 10 / 100 / 1000 subscribers', '5 min × 5'],
  ['R1', 'Replay', '10M messages, 1/10/100 consumers', 'drain × 5'],
  ['C1', 'Failover', '3-node, random kill phases', '30 s × 20'],
  ['S1', 'Soak', '80% of the SLO knee', '30 min × 3'],
];

export function BenchPage() {
  const { state } = useFeed();
  const metrics = state?.metrics;

  return (
    <div className="doc-page">
      <section className="doc-hero">
        <p className="eyebrow">Benchmark protocol</p>
        <h1>Open-loop. Coordinated-omission corrected. Every run carries a manifest.</h1>
        <p className="lede">
          A latency number without a method is decoration. Whippet paces requests at absolute
          deadlines, measures from the deadline rather than the send, records scheduler slip, keeps
          HDR histograms, runs five repetitions, and writes a manifest with the host, commit,
          durability policy, payload, and seed.
        </p>
      </section>

      <div className="doc-grid">
        <article className="panel doc-card">
          <h2>Run it</h2>
          <pre className="code-block">
{`# from the repository root
whippet bench \\
  --gateway http://127.0.0.1:8080 \\
  --rate 10000 --duration 30 --warmup 5 \\
  --repetitions 5 --label L1

# raw manifest + HDR-compatible percentiles
ls artifacts/bench/*.json`}
          </pre>
          <p className="muted">
            The benchmark manifest is the artifact. A chart without it is not published.
          </p>
        </article>

        <article className="panel doc-card">
          <h2>Live dev-loop reading</h2>
          <dl className="pin-list">
            <div><dt>Ingress p50</dt><dd>{formatLatency(metrics?.ingress_p50_us ?? 0)}</dd></div>
            <div><dt>Ingress p99</dt><dd>{formatLatency(metrics?.ingress_p99_us ?? 0)}</dd></div>
            <div><dt>Match p99</dt><dd>{formatLatency(metrics?.match_p99_us ?? 0)}</dd></div>
            <div><dt>Throughput</dt><dd>{formatRate(metrics?.throughput_orders_s ?? 0)}/s</dd></div>
          </dl>
          <p className="muted">
            These are live, conflated, and labeled dev-loop. They are not the headline result and
            they are not a substitute for a five-repetition run.
          </p>
        </article>
      </div>

      <section className="panel doc-card">
        <h2>Method</h2>
        <ol className="method-list">
          <li><strong>Absolute-deadline pacer.</strong> Request <em>i</em> is due at <code>t0 + i × period</code>. Latency is <code>completion − deadline</code>.</li>
          <li><strong>Scheduler slip is first-class.</strong> A late generator invalidates the run instead of hiding in the latency distribution.</li>
          <li><strong>HDR histograms.</strong> p50 / p90 / p99 / p99.9 / p99.99 / max. No averaged percentiles.</li>
          <li><strong>Five repetitions.</strong> The headline is the median run by p99; min/max spread is published.</li>
          <li><strong>Warmup and steady state.</strong> 60 s warmup; a 10 s window p99 drift above 5% fails the run.</li>
          <li><strong>Scope labels.</strong> Linux native can be headline; M4 and Docker Desktop are dev-loop.</li>
        </ol>
      </section>

      <section className="panel doc-card">
        <h2>Run matrix</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Workload</th>
                <th>Vary</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {matrix.map((row) => (
                <tr key={row[0]}>
                  {row.map((cell) => (
                    <td key={cell}>{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel doc-card">
        <h2>What is deliberately not measured</h2>
        <p>
          No p99.99 from fewer than one million samples. No comparison across different batching,
          durability, or encryption settings without labeling. No Docker Desktop number presented as
          a Linux result. No claim of sub-millisecond durable end-to-end latency.
        </p>
      </section>
    </div>
  );
}
