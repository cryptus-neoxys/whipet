import { Link } from '@tanstack/react-router';
import { useFeed } from '../lib/feed';
import { useNow } from '../hooks/useNow';
import { ConnectionPill } from './ConnectionPill';
import { ModeBadge } from './ModeBadge';

export function Header() {
  const { state, connection, lastMessageAt } = useFeed();
  const now = useNow(1_000);
  const age = lastMessageAt === 0 ? 0 : now - lastMessageAt;
  const stale = connection === 'live' && lastMessageAt > 0 && age > 3_000;
  const mode = state?.mode ?? 'durable';

  return (
    <header className="topbar">
      <Link to="/" className="brand" aria-label="Whippet live">
        <svg className="brand-mark" viewBox="0 0 40 32" aria-hidden="true">
          <path d="M3 25 L12 8 L20 21 L27 6 L37 25" />
          <path className="brand-speed" d="M2 28 H20" />
        </svg>
        <span className="brand-text">
          <span className="brand-name">WHIPPET</span>
          <span className="brand-sub">exchange · iggy showcase</span>
        </span>
      </Link>

      <nav className="nav" aria-label="Sections">
        <Link to="/" activeOptions={{ exact: true }} activeProps={{ className: 'nav-link active' }} className="nav-link">
          Live
        </Link>
        <Link to="/bench" activeProps={{ className: 'nav-link active' }} className="nav-link">
          Benchmarks
        </Link>
        <Link to="/about" activeProps={{ className: 'nav-link active' }} className="nav-link">
          About
        </Link>
      </nav>

      <div className="topbar-status">
        <ConnectionPill connection={connection} stale={stale} ageMs={age} />
        <ModeBadge mode={mode} />
        <span className="build" title="Dashboard build">
          {state?.dashboard_build ?? 'dev'}
        </span>
      </div>
    </header>
  );
}
