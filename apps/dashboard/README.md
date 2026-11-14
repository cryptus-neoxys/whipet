# Whippet dashboard

The read-only-plus-ticket front end for the Whippet exchange. React 19, TanStack Router,
uPlot, and a small Canvas2D depth chart. It is served by the Rust node at `/` in the
production image; in development Vite proxies `/api` and `/ws` to the node.

## Commands

```sh
cd apps/dashboard
pnpm install
pnpm dev        # http://127.0.0.1:5173
pnpm build      # tsc --noEmit + vite build -> dist/
pnpm preview    # serve dist/ locally
pnpm typecheck  # tsc --noEmit
```

## Proxy setup

`vite.config.ts` forwards the two endpoints the dashboard uses:

| Path   | Target                  | Notes                          |
| ------ | ----------------------- | ------------------------------ |
| `/api` | `http://127.0.0.1:8080` | `/api/state`, `/api/health`, `/api/orders` |
| `/ws`  | `ws://127.0.0.1:8080`   | state frames at 20 Hz          |

Start the node on port 8080 first:

```sh
# from the repository root
cargo run -p whippet -- node
```

Then `pnpm dev`. The WebSocket URL is derived from `window.location`, so the same bundle
works behind the node or behind the Vite proxy without a build-time API base URL.

## Layout

- `src/routes/LivePage.tsx` — hero, demo beats, metrics, ladder, depth, tape, ticket, charts.
- `src/routes/BenchPage.tsx` — benchmark protocol and run matrix.
- `src/routes/AboutPage.tsx` — scope, guarantee matrix, attribution.
- `src/hooks/useExchangeFeed.ts` — `/api/state` bootstrap plus `/ws` reconnect loop. The last
  known state is kept when the feed drops; the UI labels it stale instead of blanking.
- `src/lib/types.ts` — mirrors the Rust DTOs in `crates/whippet-core/src/api.rs`.

## Notes

- Prices are integer cents in the API and formatted in the browser.
- The dashboard is dev-loop by default. Latency charts are labeled `dev-loop`; headline
  numbers belong to the five-repetition benchmark protocol on `/bench`.
- Durable is the only mode this node implements. Turbo is shown as `Phase 2 — not enabled`
  and its control is disabled rather than silently ignored.
- No new runtime dependencies beyond React, TanStack Router, and uPlot.
