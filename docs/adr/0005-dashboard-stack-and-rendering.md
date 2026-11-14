# ADR 0005: Dashboard stack and rendering

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md), [ADR 0006](0006-benchmark-protocol.md)

## Context

The dashboard has to show a live limit-order book, a trades tape, latency and
throughput charts, and a benchmark page without turning the browser into the
bottleneck. It also has to be honest: a stale book must look stale, a conflated
feed must say so, and a dev-loop number must not look like a headline result.

The research dossier recommended a pure-Rust Leptos/WASM client with custom
Canvas2D renderers. That would keep the UI in Rust, but it would also mean
building chart primitives, routing, and a component ecosystem before the first
demo. The actual Phase 1 constraint was time-to-working-demo, not language
purity.

The market-data hub already produces a complete `StateView` on a fixed cadence.
The browser does not need every delta; it needs the latest coherent state and a
lossless visible tape. That makes state conflation the natural fanout model.

## Decision

Use a TypeScript single-page app in `apps/dashboard`:

- **TanStack Router 1.170.46** with a code-based route tree
  (`src/router.tsx`) for `/` (Live), `/bench`, and `/about`.
- **React 19.3.1** with React DOM 19.3.1 for the component tree.
- **uPlot 1.6.32** for time-series charts: ingress p50/p99, match p99, and
  orders/s.
- **Canvas2D** for the cumulative depth chart (`src/components/DepthChart.tsx`),
  drawn imperatively with device-pixel-ratio scaling and a `ResizeObserver`.
- **Vite 8.3.8** and TypeScript 5.9.3 for the build. `pnpm build` runs
  `tsc --noEmit` and then the Vite production build.
- **One WebSocket per tab** at `/ws`, with an initial `GET /api/state` fallback,
  exponential backoff (1 s doubling to 10 s), and an explicit
  connecting/live/reconnecting/offline state.

### 20 Hz conflation

`MdHub::spawn_publisher` ticks every 50 ms (20 Hz) and publishes a complete
`StateView` through a Tokio `watch` channel. The gateway WebSocket task sends
the latest state to each client. This is **state conflation**, not event
conflation:

- The browser receives at most 20 full snapshots per second, regardless of how
  many market-data events the engine produced.
- A slow client skips intermediate states; it does not queue them. The `watch`
  channel keeps only the latest value.
- The visible trades tape retains the last 250 trades. Audit and replay use the
  Iggy log, not the UI tape.
- The dashboard keeps 240 chart samples, recorded at most once per 500 ms, so
  the charts are a lower-frequency view than the 20 Hz feed.
- `StateView` includes `freshness_ms`, `sequence`, `mode`, `guarantee`, and
  `journal` so the UI can show staleness and the durability contract instead of
  pretending a dead feed is live.

The WebSocket payload is JSON in Phase 1. Binary frames, per-client delta
encoding, and a server-side conflation counter are Phase 2 work.

### Rendering discipline

- The market-data path writes into a cache; React renders the page, not each
  market-data event.
- uPlot instances are created once per chart and updated with `setData`; the
  chart is not re-created on every snapshot.
- The depth canvas redraws on book change and on resize; it does not use SVG
  nodes per price level.
- The order book, depth chart, and tape read from the same conflated `StateView`
  so the page is internally consistent at each frame.

## Consequences

Positive:

- The UI shipped in Phase 1 without a WASM toolchain or a custom chart library.
- uPlot is small and fast for the two time-series panels; Canvas2D gives direct
  control over the depth chart and avoids a large charting dependency.
- 20 Hz state conflation keeps browser work bounded and makes the feed robust to
  a slow tab.
- The mode badge and guarantee text are driven by the same `Mode` type as the
  backend, so the UI cannot silently disagree with the node.

Negative:

- The dashboard is not pure Rust. The research dossier's "one language"
  preference is not satisfied.
- JSON snapshots are larger than binary frames and repeat unchanged book levels
  every 50 ms. On localhost this is fine; it is not a public-internet fanout
  design.
- The UI cannot show every market-data delta or every trade beyond the 250-item
  tape. That is acceptable for a ladder, but it means the dashboard is not a
  replay or audit tool.
- There is no replay page, timeline scrubber, or audit view in Phase 1. The
  routes are Live, Bench, and About only.
- A slow client is not disconnected or counted; it simply misses intermediate
  states. Phase 2 should expose conflation/drop counters and a resync path.

## Alternatives considered

- **Leptos/WASM with custom Canvas2D** (the research recommendation). Pure Rust
  and a smaller runtime, but slower to build and weaker chart ecosystem.
  Rejected for Phase 1; still a credible future direction if the UI grows.
- **Lightweight Charts for everything.** Excellent financial charts, but it is
  another large dependency and still would not draw the custom cumulative depth
  view as directly as Canvas2D. Rejected.
- **SVG/D3 for the depth chart.** Easier to inspect, but one node per level and
  a virtual DOM diff per frame is the wrong shape for a 20 Hz ladder. Rejected.
- **Per-event WebSocket fanout.** Lowest display latency, highest browser and
  network cost, and it makes conflation invisible. Rejected; the UI needs
  coherent state, not every delta.
- **Server-side rendering.** No benefit for a live, authenticated-in-the-future
  trading UI. Rejected.
- **Binary WebSocket frames in Phase 1.** Better bandwidth, but it adds a codec
  and a versioning surface before the UI has stabilized. Deferred to Phase 2.

## Status

Accepted for Phase 1. The stack is pinned in `apps/dashboard/package.json`.
Revisit when replay/audit pages, binary frames, or multi-client fanout land. A
change to the 20 Hz cadence or to state-vs-event conflation is a product change,
not a tuning tweak, and should be recorded in a new ADR.
