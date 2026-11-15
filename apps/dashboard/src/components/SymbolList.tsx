import { formatPrice, formatQty, formatSession } from '../lib/format';
import type { BookView } from '../lib/types';

export function SymbolList({
  books,
  selected,
  onSelect,
}: {
  books: BookView[];
  selected: string;
  onSelect: (symbol: string) => void;
}) {
  return (
    <section className="panel symbol-panel">
      <div className="panel-head">
        <span>Instruments</span>
        <span className="panel-meta">{books.length} live</span>
      </div>
      <div className="symbol-list">
        {books.map((book) => {
          const active = book.symbol === selected;
          const last = book.last_trade_price_cents ?? book.mid_cents;
          const session = formatSession(book.session);
          return (
            <button
              key={book.symbol}
              type="button"
              className={`symbol-row ${active ? 'active' : ''}`}
              aria-pressed={active}
              onClick={() => onSelect(book.symbol)}
            >
              <span className="symbol-name">
                {book.symbol}
                {session ? (
                  <span
                    className={`session-tag session-${session.tone}`}
                    role="status"
                    aria-label={`${book.symbol} is in ${session.label}`}
                  >
                    {session.text}
                  </span>
                ) : null}
                {book.halted ? (
                  <span className="halt-tag" role="status" aria-label={`${book.symbol} is halted`}>
                    halted
                  </span>
                ) : null}
              </span>
              <span className="symbol-price">{formatPrice(last, book.symbol)}</span>
              <span className="symbol-meta">
                <span className={book.last_trade_side === 'sell' ? 'sell' : 'buy'}>
                  {book.last_trade_side ? book.last_trade_side.toUpperCase() : '—'}
                </span>
                <span>spread {book.spread_cents === null ? '—' : formatPrice(book.spread_cents, book.symbol)}</span>
              </span>
              <span className="symbol-depth">
                <span className="depth-buy">{formatQty(book.depth_bid_qty)}</span>
                <span className="depth-sep">/</span>
                <span className="depth-sell">{formatQty(book.depth_ask_qty)}</span>
              </span>
            </button>
          );
        })}
      </div>
    </section>
  );
}
