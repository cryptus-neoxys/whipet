import type { Mode } from '../lib/types';

const copy: Record<Mode, { label: string; detail: string }> = {
  durable: {
    label: 'DURABLE',
    detail:
      'Gateway → Iggy orders → matcher → Iggy executions + MD → consumers. Acknowledged after the log write.',
  },
  turbo: {
    label: 'TURBO',
    detail:
      'Local sequencer → matcher → direct fanout, Iggy write-behind. Lower latency, bounded uncommitted window.',
  },
};

export function ModeBadge({ mode, compact = false }: { mode: Mode; compact?: boolean }) {
  const meta = copy[mode] ?? copy.durable;
  return (
    <span className={`mode-badge mode-${mode}`} title={meta.detail}>
      <span className="mode-dot" />
      {compact ? meta.label : `${meta.label} PATH`}
    </span>
  );
}

export function ModeNote({ mode }: { mode: Mode }) {
  const meta = copy[mode] ?? copy.durable;
  return (
    <div className={`mode-note mode-${mode}`}>
      <ModeBadge mode={mode} />
      <p>{meta.detail}</p>
    </div>
  );
}
