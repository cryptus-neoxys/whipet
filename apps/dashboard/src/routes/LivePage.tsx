import { useEffect, useState } from 'react';
import { useFeed } from '../lib/feed';
import { formatCount, formatPrice } from '../lib/format';
import { MetricsStrip } from '../components/MetricsStrip';
import { ModeNote } from '../components/ModeBadge';
import { OrderBook } from '../components/OrderBook';
import { DepthChart } from '../components/DepthChart';
import { OrderTicket } from '../components/OrderTicket';
import { SymbolList } from '../components/SymbolList';
import { TradesTape } from '../components/TradesTape';
import { LatencyChart, ThroughputChart } from '../components/Charts';

export function LivePage() {
  const { state, history, connection, error } = useFeed();
  const [selected, setSelected] = useState('BTC-USD');

  const books = state?.symbols ?? [];
  useEffect(() => {
    if (books.length > 0 && !books.some((book) => book.symbol === selected)) {
      setSelected(books[0].symbol);
    }
  }, [books, selected]);

  const book = books.find((item) => item.symbol === selected) ?? books[0] ?? null;
  const metrics = state?.metrics ?? null;
  const stale = state !== null && state.freshness_ms > 3_000;
  const mode = state?.mode ?? 'durable';

  return (
    <div className="live-page">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Apache Iggy showcase · single node · deterministic matcher</p>
          <h1>
            Sub-millisecond matching.
            <br />
            <span>Millisecond-durable streaming.</span>
          </h1>
          <p className="lede">
            Orders enter through the gateway, land in Iggy, and are matched by a symbol-sharded
            price-time engine. Executions and market data flow back through the log to independent
            consumers. Every number is measured, labeled, and reproducible.
          </p>
          <ModeNote mode={mode} />
          <div className="pin-row">
            <span className="pin">Iggy server 0.10.0</span>
            <span className="pin">Rust SDK 0.12.0</span>
            <span className="pin">16 partitions</span>
            <span className="pin">Apache-2.0</span>
          </div>
        </div>

        <div className="hero-side">
          <div className="path-card">
            <div className="path-title">
              <span>durable path</span>
              <span className="path-live">
                <span className="pulse" /> {connection === 'live' ? 'receiving' : connection}
              </span>
            </div>
            <div className="path">
              <div className="path-node">
                <strong>Gateway</strong>
                <span>validate · idempotency</span>
              </div>
              <span className="path-arrow">→</span>
              <div className="path-node accent">
                <strong>Iggy orders</strong>
                <span>persisted · quorum ack</span>
              </div>
              <span className="path-arrow">→</span>
              <div className="path-node">
                <strong>Matcher</strong>
                <span>one writer per book</span>
              </div>
              <span className="path-arrow">→</span>
              <div className="path-node accent">
                <strong>Iggy exec + MD</strong>
                <span>replicated fanout</span>
              </div>
            </div>
            <div className="path-foot">
              <span>{state?.guarantee ?? 'Acknowledged after the Iggy write. Replayable from the log.'}</span>
            </div>
          </div>
          <div className="hero-stat">
            <span className="hero-stat-label">log sequence</span>
            <span className="hero-stat-value">{formatCount(state?.sequence ?? 0)}</span>
            <span className="hero-stat-note">
              freshness {state?.freshness_ms ?? 0} ms · {state?.journal ?? 'connecting'} · {state?.iggy_addr ?? ''}
            </span>
          </div>
        </div>
      </section>

      <MetricsStrip metrics={metrics} />

      {error ? <div className="banner banner-error">{error}</div> : null}
      {stale ? (
        <div className="banner banner-warn">
          Market data is stale by {state?.freshness_ms} ms. The dashboard does not pretend to be live.
        </div>
      ) : null}

      <div className="live-grid">
        <aside className="live-left">
          <SymbolList books={books} selected={book?.symbol ?? selected} onSelect={setSelected} />
          <OrderTicket book={book} />
        </aside>
        <section className="live-center">
          <OrderBook book={book} />
          <section className="panel depth-panel">
            <div className="panel-head">
              <span>Cumulative depth</span>
              <span className="panel-meta">
                {book ? `${book.symbol} · best ${formatPrice(book.best_bid_cents, book.symbol)} / ${formatPrice(book.best_ask_cents, book.symbol)}` : 'waiting'}
              </span>
            </div>
            <DepthChart book={book} />
          </section>
        </section>
        <aside className="live-right">
          <TradesTape trades={state?.trades ?? []} selected={book?.symbol ?? selected} />
        </aside>
      </div>

      <div className="charts-grid">
        <LatencyChart history={history} />
        <ThroughputChart history={history} />
      </div>
    </div>
  );
}
