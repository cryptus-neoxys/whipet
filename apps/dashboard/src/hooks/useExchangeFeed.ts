import { useEffect, useRef, useState } from 'react';
import { fetchState } from '../lib/api';
import type { FeedSample, StateView, WsServerMessage } from '../lib/types';

export type ConnectionState = 'connecting' | 'live' | 'reconnecting' | 'offline';

export interface ExchangeFeed {
  state: StateView | null;
  connection: ConnectionState;
  lastMessageAt: number;
  history: FeedSample[];
  error: string | null;
}

export function useExchangeFeed(): ExchangeFeed {
  const [state, setState] = useState<StateView | null>(null);
  const [connection, setConnection] = useState<ConnectionState>('connecting');
  const [lastMessageAt, setLastMessageAt] = useState(0);
  const [history, setHistory] = useState<FeedSample[]>([]);
  const [error, setError] = useState<string | null>(null);
  const historyRef = useRef<FeedSample[]>([]);

  useEffect(() => {
    let closed = false;
    let socket: WebSocket | null = null;
    let retry = 0;
    let opened = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const record = (next: StateView) => {
      setState(next);
      setConnection('live');
      setError(null);
      setLastMessageAt(Date.now());
      const now = Date.now();
      const samples = historyRef.current;
      const last = samples[samples.length - 1];
      if (!last || now - last.t >= 500) {
        samples.push({
          t: now,
          p50: next.metrics.ingress_p50_us,
          p99: next.metrics.ingress_p99_us,
          matchP99: next.metrics.match_p99_us,
          throughput: next.metrics.throughput_orders_s,
          trades: next.metrics.trades,
        });
        if (samples.length > 240) samples.shift();
        setHistory([...samples]);
      }
    };

    const connect = () => {
      if (closed) return;
      setConnection(retry === 0 ? 'connecting' : opened ? 'reconnecting' : 'offline');
      const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
      // The tape only renders 60 rows. Ask the server for a 60-trade frame so the
      // 20 Hz payload is not 250 trades of unused JSON.
      socket = new WebSocket(`${protocol}://${window.location.host}/ws?trades=60`);
      socket.onopen = () => {
        opened = true;
        retry = 0;
        setConnection('live');
        setError(null);
      };
      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data as string) as WsServerMessage;
          if (message.type === 'state') {
            record(message.state);
          }
        } catch {
          setError('Received a malformed market-data frame');
        }
      };
      socket.onerror = () => {
        socket?.close();
      };
      socket.onclose = () => {
        if (closed) return;
        // Before the first successful open, report offline rather than a
        // reconnecting state that suggests a working feed.
        setConnection(opened ? 'reconnecting' : 'offline');
        const delay = Math.min(10_000, 1_000 * 2 ** retry);
        retry += 1;
        timer = setTimeout(connect, delay);
      };
    };

    fetchState()
      .then((initial) => {
        if (!closed) {
          setState(initial);
          setLastMessageAt(Date.now());
        }
      })
      .catch((cause: unknown) => {
        if (!closed) {
          setError(cause instanceof Error ? cause.message : 'Could not reach the Whippet node');
          setConnection('offline');
        }
      });

    connect();
    return () => {
      closed = true;
      if (timer) clearTimeout(timer);
      socket?.close();
    };
  }, []);

  return { state, connection, lastMessageAt, history, error };
}
