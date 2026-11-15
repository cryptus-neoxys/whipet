# ADR 0018: Instrument session state

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0015](0015-audited-instrument-control-plane.md) (halt/resume),
  [ADR 0016](0016-sequenced-price-level-market-data-feed.md) (MD feed),
  [ROADMAP Phase C](../ROADMAP-AND-IMPROVEMENT-PLAN.md),
  [ARCHITECTURE.md](../ARCHITECTURE.md), [OPERATIONS.md](../OPERATIONS.md)

## Context

Halt/resume gave the venue an operator stop that is ordered in the orders log
and replayed. A halt is not a session: a market can be pre-open (accepting
cancels but not new risk), open, or closed, and the distinction must survive a
restart and be visible on the dashboard and the sequenced feed. The improvement
plan lists session state as part of the instrument control plane, and warns
that it must be defined together with command ownership, authorization,
ordering, and replay.

## Decision

`SessionState` is a per-instrument engine field with three values:

```text
pre_open = 0   open = 1 (default)   closed = 2
```

It is changed by `CommandKind::SetSession`, a fixed-layout `Command` appended
to the **orders** topic on the symbol's partition. The session code travels in
the command's `flags` low byte; all order fields are zero. The command is
processed in log order, so a replay or snapshot restore reproduces the
session and the state hash covers it. An undecodable flag byte changes
nothing and re-emits the current status rather than inventing a state.

### Admission semantics

- New orders are accepted only while `open`; `pre_open` and `closed` reject
  them with `RejectReason::SessionNotOpen`.
- The halt check runs before the session check: an explicit operator halt is
  the more informative reason, and a halted pre-open instrument says
  `halted`.
- Cancels remain allowed in every session, so participants can withdraw risk
  before an open and after a close. Halt, resume, and set-session are never
  blocked.
- The default is `open`, so an engine without a session command behaves
  exactly as before.

### Visibility

The engine's Status MD event and the MD snapshot carry the session in the same
trailing status byte as the halt flag (`halted` bit 0, session bits 1–2). The
MD hub exposes `BookView.session`; the dashboard shows `PRE-OPEN`/`CLOSED`
badges (open has no badge). The sequenced `/ws/md` feed sees the same Status
deltas and snapshot flags.

### Control plane and audit

`POST /api/admin/instruments/{symbol}/session` accepts
`{"state":"pre_open"|"open"|"closed","reason":"..."}`. It requires an operator
token and uses the same append-then-audit shape as halt/resume: the command is
appended to the orders log (durable group commit or turbo lane), then a JSON
audit record with `action:"set_session"`, the session, reason, and order
offset is appended to the audit topic. The ack carries `session`; its
`halted: false` field is not a halt statement, because a session change does
not touch the halt flag.

### Snapshot format v5

`SymbolSnapshot` gains `session`, and the state hash gains the session byte.
The snapshot file format is bumped to v5; v4 and older are rejected with
`BadVersion` and there is no compatibility shim. An upgrade therefore requires
a reset or a full replay from an empty log. The golden state hash moves
deliberately; the standard log runs in `open`, so matching and sequencing are
unchanged.

## Alternatives rejected

- **Wall-clock sessions (a schedule):** not replayable and not deterministic
  across a restart; an operator or an external scheduler must append the
  command.
- **Exchange config only:** a restart would silently reset the session, and
  the log would not record who changed it.
- **Reuse halt for pre-open/closed:** conflates "operator stopped risk" with
  "session is not open" and loses the dashboard/feed distinction.
- **Separate commands topic:** no shared order with the orders the session
  gates.

## Consequences

- Session changes consume an orders-log offset and are visible in the state
  hash; they are rare, but they are part of the log's history.
- v4 snapshots are rejected; upgrade means reset/full replay. The MD status
  byte is present in new snapshots and Status events; a v5 upgrade resets the
  log, so no old MD snapshot is consumed by a v5 node.
- Auctions, automatic session schedules, and price-band changes are still
  future work; this ADR covers the session field and its command only.
- The audit record for a session change does not claim a halt value; readers
  must treat `halted` as absent and `session` as present.
