import { formatPrice, formatQty } from '../lib/format';
import type { BookView, LevelView } from '../lib/types';

function LadderRow({
  level,
  symbol,
  side,
  maxQty,
  best,
}: {
  level: LevelView;
  symbol: string;
  side: 'buy' | 'sell';
  maxQty: number;
  best: boolean;
}) {
  const width = Math.max(2, (level.qty / maxQty) * 100);
  return (
    <div className={`ladder-row ladder-${side} ${best ? 'best' : ''}`}>
      <span className="ladder-depth" style={{ width: `${width}%` }} />
      <span className="ladder-price">{formatPrice(level.price_cents, symbol)}</span>
      <span className="ladder-qty">{formatQty(level.qty)}</span>
      <span className="ladder-orders">{level.orders}</span>
    </div>
  );
}

export function OrderBook({ book }: { book: BookView | null }) {
  if (!book) {
    return (
      <section className="panel book-panel">
        <div className="panel-head">Order book</div>
        <div className="empty-state">Waiting for the first market-data snapshot…</div>
      </section>
    );
  }

  const maxQty = Math.max(
    1,
    ...book.bids.map((level) => level.qty),
    ...book.asks.map((level) => level.qty),
  );
  const asks = [...book.asks].reverse();

  return (
    <section className="panel book-panel">
      <div className="panel-head">
        <span>
          {book.symbol} <span className="panel-sub">book</span>
        </span>
        <span className="panel-meta">
          seq {book.seq} · {book.bids.length + book.asks.length} levels
        </span>
      </div>
      <div className="book-columns">
        <span>price</span>
        <span>qty</span>
        <span>orders</span>
      </div>
      <div className="ladder ladder-asks">
        {asks.length === 0 ? <div className="ladder-empty">no asks</div> : null}
        {asks.map((level, index) => (
          <LadderRow
            key={level.price_cents}
            level={level}
            symbol={book.symbol}
            side="sell"
            maxQty={maxQty}
            best={index === asks.length - 1}
          />
        ))}
      </div>
      <div className="spread-row">
        <span className="spread-label">spread</span>
        <span className="spread-value">
          {book.spread_cents === null ? '—' : formatPrice(book.spread_cents, book.symbol)}
        </span>
        <span className="spread-mid">
          mid {book.mid_cents === null ? '—' : formatPrice(book.mid_cents, book.symbol)}
        </span>
      </div>
      <div className="ladder ladder-bids">
        {book.bids.length === 0 ? <div className="ladder-empty">no bids</div> : null}
        {book.bids.map((level, index) => (
          <LadderRow
            key={level.price_cents}
            level={level}
            symbol={book.symbol}
            side="buy"
            maxQty={maxQty}
            best={index === 0}
          />
        ))}
      </div>
    </section>
  );
}
