export function DemoBeats({
  onFocusTicket,
  onBurst,
  burstDisabled,
}: {
  onFocusTicket: () => void;
  onBurst: () => void;
  burstDisabled: boolean;
}) {
  return (
    <section className="panel demo-beats" aria-labelledby="demo-beats-title">
      <div className="beats-head">
        <span id="demo-beats-title" className="beats-title">
          Demo beats
        </span>
        <span className="beats-note">three moves, one browser tab</span>
      </div>
      <ol className="beats">
        <li className="beat">
          <span className="beat-index" aria-hidden="true">
            01
          </span>
          <div className="beat-body">
            <strong>Place a crossing order</strong>
            <p>Set a limit buy above the best ask, then submit it from the ticket.</p>
          </div>
          <button type="button" className="beat-action" onClick={onFocusTicket}>
            Focus ticket
          </button>
        </li>
        <li className="beat">
          <span className="beat-index" aria-hidden="true">
            02
          </span>
          <div className="beat-body">
            <strong>Watch the tape and book</strong>
            <p>The fill appears in the tape; the ladder and depth redraw from the same frame.</p>
          </div>
          <a className="beat-action beat-action-quiet" href="#trade-tape">
            Jump to tape
          </a>
        </li>
        <li className="beat">
          <span className="beat-index" aria-hidden="true">
            03
          </span>
          <div className="beat-body">
            <strong>Read the durable ack offset</strong>
            <p>The ticket reports the partition and offset returned after the Iggy append.</p>
          </div>
          <button type="button" className="beat-action" onClick={onBurst} disabled={burstDisabled}>
            Run 12-order burst
          </button>
        </li>
      </ol>
    </section>
  );
}
