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
  return (
    <span className={`connection connection-${state}`} title={`Last state frame ${Math.max(0, Math.round(ageMs))} ms ago`}>
      <span className="connection-dot" />
      {stale && connection === 'live' ? 'STALE' : labels[connection]}
    </span>
  );
}
