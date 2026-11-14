import type { ConnectionState } from '../hooks/useExchangeFeed';

const labels: Record<ConnectionState, string> = {
  connecting: 'CONNECTING',
  live: 'LIVE',
  reconnecting: 'RECONNECTING',
  offline: 'OFFLINE',
};

export function ConnectionPill({
  connection,
  stale,
  ageMs,
}: {
  connection: ConnectionState;
  stale: boolean;
  ageMs: number;
}) {
  const state = stale && connection === 'live' ? 'reconnecting' : connection;
  const visible = stale && connection === 'live' ? 'STALE' : labels[connection];
  return (
    <span
      className={`connection connection-${state}`}
      role="status"
      aria-live="polite"
      aria-label={`Connection ${visible.toLowerCase()}`}
      title={`Last state frame ${Math.max(0, Math.round(ageMs))} ms ago`}
    >
      <span className="connection-dot" aria-hidden="true" />
      {visible}
    </span>
  );
}
