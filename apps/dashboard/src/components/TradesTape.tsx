import { memo } from 'react';
import type { ReactElement } from 'react';
import { formatPrice, formatQty, formatTimeNs } from '../lib/format';
import type { TradeView } from '../lib/types';

const VISIBLE_TRADES = 60;

const TapeRow = memo(
  function TapeRow({ trade, selected }: { trade: TradeView; selected: boolean }) {
    return (
      <div className={`tape-row ${selected ? 'selected' : ''}`}>
        <span className="tape-time">{formatTimeNs(trade.ts_ns)}</span>
        <span className="tape-symbol">{trade.symbol}</span>
        <span className="tape-price">{formatPrice(trade.price_cents, trade.symbol)}</span>
        <span className="tape-qty">{formatQty(trade.qty)}</span>
        <span className={`tape-side ${trade.side}`}>{trade.side}</span>
      </div>
    );
  },
  (prev, next) =>
    prev.selected === next.selected &&
    prev.trade.symbol === next.trade.symbol &&
    prev.trade.price_cents === next.trade.price_cents &&
    prev.trade.qty === next.trade.qty &&
    prev.trade.side === next.trade.side &&
    prev.trade.ts_ns === next.trade.ts_ns &&
    prev.trade.seq === next.trade.seq,
);

export function TradesTape({ trades, selected }: { trades: TradeView[]; selected: string }) {
  // Read the first 60 by index instead of allocating a slice every frame.
  const visibleCount = Math.min(trades.length, VISIBLE_TRADES);
  const rows: ReactElement[] = new Array(visibleCount);
  for (let index = 0; index < visibleCount; index += 1) {
    const trade = trades[index];
    rows[index] = <TapeRow key={trade.seq} trade={trade} selected={trade.symbol === selected} />;
  }

  return (
    <section
      className="panel tape-panel"
      id="trade-tape"
      tabIndex={-1}
      aria-label={`Trade tape, newest ${VISIBLE_TRADES} trades across all instruments`}
    >
      <div className="panel-head">
        <span>Trades</span>
        <span className="panel-meta">{trades.length} in window</span>
      </div>
      <div className="tape-columns" aria-hidden="true">
        <span>time</span>
        <span>symbol</span>
        <span>price</span>
        <span>qty</span>
        <span>side</span>
      </div>
      <div className="tape">
        {visibleCount === 0 ? (
          <div className="empty-state">No trades yet. Submit a crossing order.</div>
        ) : null}
        {rows}
      </div>
    </section>
  );
}
