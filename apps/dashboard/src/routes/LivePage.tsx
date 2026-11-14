import { useEffect, useMemo, useState } from 'react';
import { useFeed } from '../lib/feed';
import { useNow } from '../hooks/useNow';
import { formatCount, formatPrice } from '../lib/format';
import { DemoBeats } from '../components/DemoBeats';
import { MetricsStrip } from '../components/MetricsStrip';
import { ModeNote } from '../components/ModeBadge';
import { ModeTradeoff } from '../components/ModeTradeoff';
import { OrderBook } from '../components/OrderBook';
import { DepthChart } from '../components/DepthChart';
import { OrderTicket } from '../components/OrderTicket';
import { SymbolList } from '../components/SymbolList';
import { TradesTape } from '../components/TradesTape';
import { LatencyChart, ThroughputChart } from '../components/Charts';

export function LivePage() {
  const { state, history, connection, lastMessageAt, error } = useFeed();
  const [selected, setSelected] = useState('BTC-USD');
  const [focusSignal, setFocusSignal] = useState(0);
  const [burstSignal, setBurstSignal] = useState(0);
  const now = useNow(1_000);

  const books = useMemo(() => state?.symbols ?? [], [state?.symbols]);
  useEffect(() => {
    if (books.length > 0 && !books.some((book) => book.symbol === selected)) {
      setSelected(books[0].symbol);
    }
  }, [books, selected]);

  const book = books.find((item) => item.symbol === selected) ?? books[0] ?? null;
  const metrics = state?.metrics ?? null;
  const mode = state?.mode ?? 'durable';

  // freshness_ms is the server's age at snapshot time; the local frame age
  // keeps growing when the socket goes quiet, so the label stays honest.
  const ageMs = lastMessageAt > 0 ? Math.max(0, now - lastMessageAt) : 0;
  const stale = state !== null && ageMs > 3_000;
  const staleBy = Math.max(ageMs, state?.freshness_ms ?? 0);

  const unreachable = connection === 'offline' && state === null;
  const feedDown = connection === 'offline' && state !== null;

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
            consumers. Every number is measured, labeled, and scoped to its environment.
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
              <span className="path-live" role="status" aria-live="polite">
                <span className="pulse" aria-hidden="true" />{' '}
                {connection === 'live' ? (stale ? 'stale' : 'receiving') : connection}
              </span>
            </div>
            <div className="path">
              <div className="path-node">
                <strong>Gateway</strong>
                <span>validate · idempotency</span>
              </div>
              <span className="path-arrow" aria-hidden="true">
                →
              </span>
              <div className="path-node accent">
                <strong>Iggy orders</strong>
                <span>persisted · quorum ack</span>
              </div>
              <span className="path-arrow" aria-hidden="true">
                →
              </span>
              <div className="path-node">
                <strong>Matcher</strong>
                <span>one writer per book</span>
              </div>
              <span className="path-arrow" aria-hidden="true">
                →
              </span>
              <div className="path-node accent">
                <strong>Iggy exec + MD</strong>
                <span>replicated fanout</span>
              </div>
            </div>
            <div className="path-foot">
              <span>{state?.guarantee ?? 'Acknowledged after the Iggy write.'}</span>
            </div>
          </div>
          <div className="hero-stat">
            <span className="hero-stat-label">log sequence</span>
            <span className="hero-stat-value">{formatCount(state?.sequence ?? 0)}</span>
            <span className="hero-stat-note">
              freshness {state?.freshness_ms ?? 0} ms · {state?.journal ?? 'connecting'} ·{' '}
              {state?.iggy_addr ?? ''}
            </span>
          </div>
        </div>
      </section>

      {unreachable ? (
        <div className="banner banner-error" role="alert">
          <strong>Node unreachable.</strong> <code>/api/state</code> did not answer, so there is no
          snapshot to show. {error ? `${error} ` : ''}The dashboard keeps retrying in the background.
        </div>
      ) : feedDown ? (
        <div className="banner banner-error" role="alert">
          <strong>Live feed unreachable.</strong> Showing the last known snapshot, frozen at the
          last frame — it is not live. {error ? `${error} ` : ''}Reconnecting in the background.
        </div>
      ) : error ? (
        <div className="banner banner-error" role="status">
          {error}
        </div>
      ) : null}

      {stale ? (
        <div className="banner banner-warn" role="status">
          Market data is stale by {formatCount(Math.round(staleBy))} ms. The dashboard does not
          pretend to be live.
        </div>
      ) : null}

      <DemoBeats
        onFocusTicket={() => setFocusSignal((value) => value + 1)}
        onBurst={() => setBurstSignal((value) => value + 1)}
        burstDisabled={!book}
      />

      <MetricsStrip metrics={metrics} />

      <div className="live-grid">
        <aside className="live-left">
          <SymbolList books={books} selected={book?.symbol ?? selected} onSelect={setSelected} />
          <OrderTicket book={book} focusSignal={focusSignal} burstSignal={burstSignal} />
        </aside>
        <section className="live-center">
          <OrderBook book={book} />
          <section className="panel depth-panel">
            <div className="panel-head">
              <span>Cumulative depth</span>
              <span className="panel-meta">
                {book
                  ? `${book.symbol} · best ${formatPrice(book.best_bid_cents, book.symbol)} / ${formatPrice(book.best_ask_cents, book.symbol)}`
                  : 'waiting'}
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

      <ModeTradeoff mode={mode} />
    </div>
  );
}
