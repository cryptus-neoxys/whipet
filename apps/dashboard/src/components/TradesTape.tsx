import { memo } from 'react';
import { formatPrice, formatQty, formatTimeNs } from '../lib/format';
import type { TradeView } from '../lib/types';

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
  const visible = trades.slice(0, 60);
  return (
    <section
      className="panel tape-panel"
      id="trade-tape"
      tabIndex={-1}
      aria-label="Trade tape, newest 60 trades across all instruments"
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
        {visible.length === 0 ? (
          <div className="empty-state">No trades yet. Submit a crossing order.</div>
        ) : null}
        {visible.map((trade) => (
          <TapeRow
            key={`${trade.order_id}-${trade.seq}`}
            trade={trade}
            selected={trade.symbol === selected}
          />
        ))}
      </div>
    </section>
  );
}
