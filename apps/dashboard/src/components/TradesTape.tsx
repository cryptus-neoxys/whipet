import { formatPrice, formatQty, formatTimeNs } from '../lib/format';
import type { TradeView } from '../lib/types';

export function TradesTape({ trades, selected }: { trades: TradeView[]; selected: string }) {
  const visible = trades.slice(0, 60);
  return (
    <section className="panel tape-panel">
      <div className="panel-head">
        <span>Trades</span>
        <span className="panel-meta">{trades.length} in window</span>
      </div>
      <div className="tape-columns">
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
          <div
            key={`${trade.order_id}-${trade.seq}`}
            className={`tape-row ${trade.symbol === selected ? 'selected' : ''}`}
          >
            <span className="tape-time">{formatTimeNs(trade.ts_ns)}</span>
            <span className="tape-symbol">{trade.symbol}</span>
            <span className="tape-price">{formatPrice(trade.price_cents, trade.symbol)}</span>
            <span className="tape-qty">{formatQty(trade.qty)}</span>
            <span className={`tape-side ${trade.side}`}>{trade.side}</span>
          </div>
        ))}
      </div>
    </section>
  );
}
