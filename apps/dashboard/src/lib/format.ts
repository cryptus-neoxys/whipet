import type { SessionState } from './types';

const decimalsBySymbol: Record<string, number> = {
  'BTC-USD': 2,
  'ETH-USD': 2,
  'SOL-USD': 3,
};

export function formatPrice(cents: number | null | undefined, symbol = 'BTC-USD'): string {
  if (cents === null || cents === undefined) return '—';
  const decimals = decimalsBySymbol[symbol] ?? 2;
  return (cents / 100).toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function formatQty(qty: number): string {
  return qty.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

export function formatLatency(micros: number): string {
  if (!Number.isFinite(micros) || micros <= 0) return '0 µs';
  if (micros < 1000) return `${micros.toFixed(0)} µs`;
  if (micros < 1_000_000) return `${(micros / 1000).toFixed(2)} ms`;
  return `${(micros / 1_000_000).toFixed(2)} s`;
}

export function formatRate(value: number): string {
  if (!Number.isFinite(value)) return '0';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toFixed(0);
}

export function formatCount(value: number): string {
  return value.toLocaleString('en-US');
}

export function formatTimeNs(tsNs: number): string {
  const date = new Date(tsNs / 1_000_000);
  return date.toLocaleTimeString('en-US', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  } as Intl.DateTimeFormatOptions);
}

export function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

export function formatSignedCents(cents: number | null | undefined, symbol: string): string {
  if (cents === null || cents === undefined) return '—';
  const sign = cents > 0 ? '+' : '';
  return `${sign}${formatPrice(cents, symbol)}`;
}

/**
 * Badge text for an instrument's trading session. `open` renders nothing: the
 * badge is only for the states an operator must notice. The text is lowercase
 * but the badge CSS uppercases it, so the state is accessible without color.
 */
export function formatSession(
  session: SessionState | undefined,
): { text: string; label: string; tone: string } | null {
  switch (session) {
    case 'pre_open':
      return { text: 'pre-open', label: 'pre-open', tone: 'pre-open' };
    case 'closed':
      return { text: 'closed', label: 'closed', tone: 'closed' };
    default:
      return null;
  }
}
