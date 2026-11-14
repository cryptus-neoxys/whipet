import { useEffect, useState } from 'react';
import { submitOrder } from '../lib/api';
import { formatPrice } from '../lib/format';
import type { BookView, OrderType, Side } from '../lib/types';

const orderTypes: { value: OrderType; label: string }[] = [
  { value: 'limit', label: 'Limit GTC' },
  { value: 'market', label: 'Market' },
  { value: 'ioc', label: 'IOC' },
  { value: 'fok', label: 'FOK' },
  { value: 'post_only', label: 'Post-only' },
];

export function OrderTicket({ book }: { book: BookView | null }) {
  const [side, setSide] = useState<Side>('buy');
  const [orderType, setOrderType] = useState<OrderType>('limit');
  const [price, setPrice] = useState('');
  const [qty, setQty] = useState('10');
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useState<{ tone: 'ok' | 'error'; text: string } | null>(null);

  useEffect(() => {
    if (!book) return;
    const cents = book.mid_cents ?? book.best_ask_cents ?? book.best_bid_cents;
    if (cents !== null && cents !== undefined) {
      setPrice((cents / 100).toFixed(2));
    }
  }, [book?.symbol]); // eslint-disable-line react-hooks/exhaustive-deps

  const requiresPrice = orderType !== 'market';

  const submit = async () => {
    if (!book) return;
    const quantity = Number.parseInt(qty, 10);
    if (!Number.isFinite(quantity) || quantity <= 0) {
      setMessage({ tone: 'error', text: 'Quantity must be a positive integer.' });
      return;
    }
    let priceCents: number | null = null;
    if (requiresPrice) {
      const parsed = Number.parseFloat(price);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        setMessage({ tone: 'error', text: 'Enter a valid price.' });
        return;
      }
      priceCents = Math.round(parsed * 100);
    }
    setPending(true);
    setMessage(null);
    try {
      const ack = await submitOrder({
        symbol: book.symbol,
        side,
        order_type: orderType,
        price_cents: priceCents,
        qty: quantity,
        client_order_id: `web-${crypto.randomUUID()}`,
        participant: 'dashboard',
      });
      setMessage({
        tone: 'ok',
        text: `Accepted at partition ${ack.partition}, offset ${ack.offset} in ${ack.durable_ack_ms.toFixed(2)} ms.`,
      });
    } catch (cause) {
      setMessage({
        tone: 'error',
        text: cause instanceof Error ? cause.message : 'Order was not accepted.',
      });
    } finally {
      setPending(false);
    }
  };

  const burst = async () => {
    if (!book) return;
    const cents = book.mid_cents ?? book.best_ask_cents ?? 6_500_000;
    setPending(true);
    setMessage(null);
    const requests = Array.from({ length: 12 }, (_, index) => {
      const buy = index % 2 === 0;
      const offset = Math.floor(index / 2) + 1;
      return submitOrder({
        symbol: book.symbol,
        side: buy ? 'buy' : 'sell',
        order_type: 'limit',
        price_cents: Math.round(cents) + (buy ? -offset : offset) * (book.symbol === 'BTC-USD' ? 100 : 10),
        qty: 1 + (index % 7),
        client_order_id: `web-burst-${crypto.randomUUID()}`,
        participant: 'dashboard-burst',
      });
    });
    const results = await Promise.allSettled(requests);
    const accepted = results.filter((result) => result.status === 'fulfilled').length;
    setPending(false);
    setMessage({
      tone: accepted === requests.length ? 'ok' : 'error',
      text: `${accepted}/${requests.length} burst orders accepted by the log.`,
    });
  };

  return (
    <section className="panel ticket-panel">
      <div className="panel-head">
        <span>Order ticket</span>
        <span className="panel-meta">{book?.symbol ?? '—'}</span>
      </div>
      <div className="ticket-side">
        <button
          type="button"
          className={side === 'buy' ? 'active buy' : ''}
          onClick={() => setSide('buy')}
        >
          Buy
        </button>
        <button
          type="button"
          className={side === 'sell' ? 'active sell' : ''}
          onClick={() => setSide('sell')}
        >
          Sell
        </button>
      </div>
      <label className="field">
        <span>Type</span>
        <select value={orderType} onChange={(event) => setOrderType(event.target.value as OrderType)}>
          {orderTypes.map((item) => (
            <option key={item.value} value={item.value}>
              {item.label}
            </option>
          ))}
        </select>
      </label>
      <label className="field">
        <span>Price (USD)</span>
        <input
          value={price}
          onChange={(event) => setPrice(event.target.value)}
          disabled={!requiresPrice}
          inputMode="decimal"
          placeholder={requiresPrice ? '65000.00' : 'market'}
        />
      </label>
      <label className="field">
        <span>Quantity</span>
        <input value={qty} onChange={(event) => setQty(event.target.value)} inputMode="numeric" />
      </label>
      <div className="ticket-reference">
        {book?.best_bid_cents ? `bid ${formatPrice(book.best_bid_cents, book.symbol)}` : 'no bid'}
        {' · '}
        {book?.best_ask_cents ? `ask ${formatPrice(book.best_ask_cents, book.symbol)}` : 'no ask'}
      </div>
      <button type="button" className={`submit ${side}`} onClick={submit} disabled={pending || !book}>
        {pending ? 'Submitting…' : `${side === 'buy' ? 'Buy' : 'Sell'} ${book?.symbol ?? ''}`}
      </button>
      <button type="button" className="burst" onClick={burst} disabled={pending || !book}>
        Send 12-order burst
      </button>
      {message ? <div className={`ticket-message ${message.tone}`}>{message.text}</div> : null}
      <p className="ticket-note">
        The gateway acknowledges after the Iggy append. Matching and MD fanout follow through the log.
      </p>
    </section>
  );
}
