export function ModeTradeoff({ mode }: { mode: 'durable' | 'turbo' }) {
  return (
    <section className="panel mode-tradeoff" aria-labelledby="mode-tradeoff-title">
      <div className="panel-head">
        <span id="mode-tradeoff-title">Mode trade-off</span>
        <span className="panel-meta">
          this node: {mode} · turbo is Phase 2 — not enabled
        </span>
      </div>
      <div className="tradeoff-body">
        <div
          className="tradeoff-control"
          role="group"
          aria-label="Durability mode (turbo is not enabled on this node)"
          title="Turbo is Phase 2 — not enabled. This node runs the durable path only."
        >
          <button
            type="button"
            className="mode-option active"
            aria-pressed={mode === 'durable'}
            disabled
            title="Durable is the only mode this node implements."
          >
            Durable
          </button>
          <button
            type="button"
            className="mode-option"
            aria-pressed={false}
            disabled
            title="Phase 2 — turbo needs the local sequencer and Iggy write-behind. This node runs durable only, so the control is disabled rather than silently ignored."
          >
            Turbo
          </button>
          <span className="phase-tag">Phase 2 — not enabled</span>
        </div>

        <div className="tradeoff-grid">
          <article className={`tradeoff-card ${mode === 'durable' ? 'is-active' : ''}`}>
            <h3>
              Durable {mode === 'durable' ? <span className="tag">active</span> : null}
            </h3>
            <p>
              The gateway appends the order to Iggy and waits for the durable acknowledgement before
              replying. The matcher consumes from the log, and executions plus market data fan out
              through the log to independent consumers.
            </p>
            <ul>
              <li>Ingress includes the Iggy append round trip.</li>
              <li>No acknowledged loss inside one node; the log is the replay source.</li>
              <li>This is the path the dashboard is reading right now.</li>
            </ul>
          </article>

          <article className="tradeoff-card is-off">
            <h3>
              Turbo <span className="tag tag-off">Phase 2 — not enabled</span>
            </h3>
            <p>
              A local sequencer would acknowledge before the log write, with Iggy catching up behind
              the matcher. Ingress latency drops, at the cost of a bounded uncommitted window.
            </p>
            <ul>
              <li>Lower ingress latency; a bounded uncommitted window can be lost on failure.</li>
              <li>Not implemented on this node.</li>
              <li>The top-bar badge always reports the mode the node actually runs.</li>
            </ul>
          </article>
        </div>
        <p className="tradeoff-note">
          No toggle is wired to nothing. Turbo becomes selectable only when the Phase 2 write-behind
          path ships.
        </p>
      </div>
    </section>
  );
}
