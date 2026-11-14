export function AboutPage() {
  return (
    <div className="doc-page">
      <section className="doc-hero">
        <p className="eyebrow">About the showcase</p>
        <h1>A small exchange, built to make the latency/durability trade-off visible.</h1>
        <p className="lede">
          Whippet is an independent Apache-2.0 showcase for Apache Iggy. It is not a SaaS, not a
          production venue, and not affiliated with or endorsed by the Apache Software Foundation.
          The goal is a rigorous, reproducible artifact: a deterministic matcher, a durable log, and
          benchmarks that state their scope.
        </p>
      </section>

      <section className="doc-grid">
        <article className="panel doc-card">
          <h2>What works in this build</h2>
          <ul>
            <li>Symbol-sharded, deterministic price-time matching with integer ticks.</li>
            <li>Durable order ingress through Iggy `orders`, with manual consumer offsets.</li>
            <li>Executions and market-data snapshots/deltas through Iggy topics.</li>
            <li>A live ladder, depth chart, trade tape, latency and throughput charts.</li>
            <li>Seeded simulator and an open-loop benchmark harness with HDR histograms.</li>
            <li>Snapshot + dedup table + input offset + state hash for replay verification.</li>
          </ul>
        </article>

        <article className="panel doc-card">
          <h2>What it does not claim</h2>
          <ul>
            <li>Sub-millisecond <em>durable end-to-end</em> latency. It claims sub-millisecond matching.</li>
            <li>Exactly-once delivery. The pipeline is at-least-once; the engine deduplicates.</li>
            <li>Headline numbers from an M4 or Docker Desktop. Those are labeled dev-loop.</li>
            <li>Production cluster maturity. Single-node is v1; 3-node VSR is Phase 3.</li>
            <li>Cross-symbol atomicity, clearing, margin, options, or FIX conformance.</li>
          </ul>
        </article>
      </section>

      <section className="panel doc-card">
        <h2>Guarantee matrix</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Claim</th>
                <th>Scope</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Sub-millisecond matching</td>
                <td>Matcher dequeue → execution events, in-process</td>
                <td>Measured live; p99 target &lt; 1 ms</td>
              </tr>
              <tr>
                <td>Low-single-digit-ms durable path</td>
                <td>Gateway → Iggy append ack → execution, Linux</td>
                <td>Design target; M4/Docker is slower and labeled</td>
              </tr>
              <tr>
                <td>Per-partition order</td>
                <td>Orders keyed by symbol to a fixed partition</td>
                <td>Enforced</td>
              </tr>
              <tr>
                <td>No acknowledged loss across one node</td>
                <td>3 nodes, RF3, persisted, Phase 3</td>
                <td>Planned, not in this build</td>
              </tr>
              <tr>
                <td>Replay reproduces the state hash</td>
                <td>Snapshot + tail replay + dedup table</td>
                <td>Property-tested; `whippet verify` checks snapshots</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section className="doc-grid">
        <article className="panel doc-card">
          <h2>Architecture</h2>
          <p>
            The gateway validates orders and appends them to Iggy. Each active partition has one
            engine task, which is the single writer for its books. The engine is a synchronous state
            machine: array price ladder, occupancy bitmap, intrusive FIFO, preallocated arena. It
            reads no clock and performs no I/O. The journal edge is async; the match loop is not.
          </p>
          <p>
            Executions and market data are appended to Iggy. The MD consumer applies deltas to a
            conflated cache and publishes a state frame at 20 Hz. The dashboard never sees raw
            per-event work in the browser.
          </p>
        </article>
        <article className="panel doc-card">
          <h2>Pinned versions</h2>
          <dl className="pin-list">
            <div><dt>Iggy server</dt><dd>apache/iggy:0.10.0</dd></div>
            <div><dt>Rust SDK</dt><dd>iggy = 0.12.0</dd></div>
            <div><dt>Wire protocol</dt><dd>0.12.0</dd></div>
            <div><dt>Rust</dt><dd>1.95.0</dd></div>
            <div><dt>License</dt><dd>Apache-2.0</dd></div>
          </dl>
          <p className="muted">
            No `latest` or `edge` tags. The image digest is recorded in benchmark manifests.
          </p>
        </article>
      </section>

      <section className="panel doc-card">
        <h2>Apache and attribution</h2>
        <p>
          Apache Iggy is a project of the Apache Software Foundation and is licensed under the Apache
          License, Version 2.0. Whippet is an independent showcase. It is not affiliated with,
          sponsored by, or endorsed by the ASF. Apache, Iggy, and the Apache feather are trademarks
          of the Apache Software Foundation; no logo or trademark is used to imply endorsement.
        </p>
        <p>
          The matcher design follows the LMAX / exchange-core lineage. Benchmark methodology follows
          Gil Tene's HdrHistogram and coordinated-omission work. These are references, not vendored
          code.
        </p>
      </section>
    </div>
  );
}
