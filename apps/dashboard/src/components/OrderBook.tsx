import { memo } from 'react';
import { formatPrice, formatQty } from '../lib/format';
import type { BookView, LevelView } from '../lib/types';

/**
 * Depth bars only need whole-percent resolution. Quantizing here means a level
 * whose share of the book did not visibly change keeps an identical `width`
 * prop, so the row memo can skip it even though a sibling level moved.
 */
function depthWidthPct(qty: number, maxQty: number): number {
  return Math.max(2, Math.round((qty / maxQty) * 100));
}

const LadderRow = memo(
  function LadderRow({
    level,
    symbol,
    side,
    width,
    best,
  }: {
    level: LevelView;
    symbol: string;
    side: 'buy' | 'sell';
    width: number;
    best: boolean;
  }) {
    return (
      <div className={`ladder-row ladder-${side} ${best ? 'best' : ''}`}>
        <span className="ladder-depth" style={{ width: `${width}%` }} />
        <span className="ladder-price">{formatPrice(level.price_cents, symbol)}</span>
        <span className="ladder-qty">{formatQty(level.qty)}</span>
        <span className="ladder-orders">{level.orders}</span>
      </div>
    );
  },
  (prev, next) =>
    prev.level.price_cents === next.level.price_cents &&
    prev.level.qty === next.level.qty &&
    prev.level.orders === next.level.orders &&
    prev.symbol === next.symbol &&
    prev.side === next.side &&
    prev.width === next.width &&
    prev.best === next.best,
);

export function OrderBook({ book }: { book: BookView | null }) {
  if (!book) {
    return (
      <section className="panel book-panel" aria-label="Order book">
        <div className="panel-head">Order book</div>
        <div className="empty-state">Waiting for the first market-data snapshot…</div>
      </section>
    );
  }

  let maxQty = 1;
  for (const level of book.bids) if (level.qty > maxQty) maxQty = level.qty;
  for (const level of book.asks) if (level.qty > maxQty) maxQty = level.qty;

  const askCount = book.asks.length;

  return (
    <section className="panel book-panel" aria-label={`Order book for ${book.symbol}`}>
      <div className="panel-head">
        <span>
          {book.symbol} <span className="panel-sub">book</span>{' '}
          {book.halted ? (
            <span className="halt-badge" role="status" aria-label={`${book.symbol} is halted`}>
              halted
            </span>
          ) : null}
        </span>
        <span className="panel-meta">
          seq {book.seq} · {book.bids.length + askCount} levels
        </span>
      </div>
      <div className="book-columns">
        <span>price</span>
        <span>qty</span>
        <span>orders</span>
      </div>
      <div className="ladder ladder-asks">
        <div className="ladder-side-label">asks</div>
        {askCount === 0 ? <div className="ladder-empty">no asks</div> : null}
        {book.asks.map((_level, index) => {
          // Walk the ascending asks backwards so the best ask sits next to the
          // spread, without allocating a reversed copy every frame.
          const sourceIndex = askCount - 1 - index;
          const level = book.asks[sourceIndex];
          return (
            <LadderRow
              key={level.price_cents}
              level={level}
              symbol={book.symbol}
              side="sell"
              width={depthWidthPct(level.qty, maxQty)}
              best={sourceIndex === 0}
            />
          );
        })}
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
        <div className="ladder-side-label">bids</div>
        {book.bids.length === 0 ? <div className="ladder-empty">no bids</div> : null}
        {book.bids.map((level, index) => (
          <LadderRow
            key={level.price_cents}
            level={level}
            symbol={book.symbol}
            side="buy"
            width={depthWidthPct(level.qty, maxQty)}
            best={index === 0}
          />
        ))}
      </div>
    </section>
  );
}
