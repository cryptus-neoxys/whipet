# Benchmark runs

This directory holds published benchmark runs. A run is a directory with a
method card, the machine-written manifest, and any raw or chart artifacts. The
manifest is the source of truth; prose and charts are derived from it.

There are no published run directories yet. The single recorded smoke run is
described in [BENCHMARKS.md](../BENCHMARKS.md) and is not published here as a
result, because one three-second repetition on Docker Desktop is a smoke test.

## Directory layout

```text
docs/benchmarks/
├── README.md
└── <YYYY-MM-DD>-<host>-<scope>-<label>/
    ├── METHOD.md
    ├── manifest.json
    ├── raw/
    │   ├── <run-id>.json
    │   └── <run-id>.hdr        # when the harness exports one
    └── charts/
        └── latency-vs-rate.svg
```

- `<YYYY-MM-DD>` is the run date, not the publish date.
- `<host>` is short and recognizable, for example `m4`, `r7iz`, `c7i`.
- `<scope>` is `dev-loop` or `headline`. Use the manifest's `labels` value.
- `<label>` is the workload label, for example `l1` or `f1`.
- One directory per published run set. If you rerun the same workload on a
  different day, create a new directory; do not overwrite history.

Example: `2026-11-14-m4-dev-loop-smoke/`.

## Rules

1. **The manifest is machine-written.** Do not edit it. If a field is wrong,
   fix the harness and rerun.
2. **Keep the filename equal to `run_id`.** `manifest.json` is a copy of the
   headline manifest; `raw/<run-id>.json` keeps the original name.
3. **Write a method card.** No directory without `METHOD.md`.
4. **Label the scope.** If the manifest says `dev-loop`, the card says
   `dev-loop`. Do not present a Docker Desktop run as a headline result.
5. **Publish five repetitions** unless the card explains why fewer are
   meaningful. A smoke run is not a published result.
6. **Report the median-p99 repetition** as the headline, plus the min/max p99
   spread across repetitions and the sample count.
7. **Do not retype numbers** into Markdown without linking the manifest. If a
   summary table disagrees with the manifest, the manifest wins.
8. **Charts are generated, not drawn by hand.** Keep the generator script or
   the exact command next to the chart.
9. **Do not commit huge raw files.** If a raw histogram or log is over a few
   tens of MB, store it outside the repo and link it from the card.
10. **Commit message includes the run id** and the scope.

## Method card template

Copy this into `METHOD.md` and fill it in. Keep it short.

```markdown
# <label> on <host>, <date>

## Question

What was measured, and what decision does it inform?

## Scope

- Scope label: dev-loop | headline
- Host: CPU, cores, memory, OS, kernel
- Runtime: native | Docker Desktop | OrbStack | Linux VM; versions
- Commit: <sha>, dirty: yes/no
- Image digest: <digest or none>
- Iggy server / SDK / wire: 0.10.0 / 0.12.0 / 0.12.0

## Workload

- Harness: `whippet bench`
- Mode: durable
- Transport: http
- Symbols: ...
- Rate: ... orders/s
- Duration: ... s measured, ... s warmup
- Repetitions: ...
- Durability: persisted / persisted
- Batch size: ...
- Encryption: no
- Seed: ...

## Result

- Samples: ...
- Achieved rate: .../s
- Errors: ...
- p50 / p90 / p99 / p99.9: ... / ... / ... / ... µs
- Scheduler slip p50 / p99: ... / ... µs
- Headline repetition: index ..., chosen by median p99
- p99 spread across repetitions: ... to ... µs

## Caveats

- What the run does not measure.
- Any noise, thermal, or host conditions.
- Any harness gap that affects interpretation.

## Artifacts

- `manifest.json` (headline, median p99)
- `raw/<run-id>.json`
- `charts/...`
- Command line: ...
```

## Adding a run

1. Start from a clean stack and a quiet machine. Do not run the simulator or a
   second benchmark against the same node.
2. Run the harness with at least five repetitions and a warmup:

   ```sh
   cargo run --release -p whippet -- bench \
     --gateway http://127.0.0.1:8080 \
     --rate 1000 --duration 30 --warmup 5 \
     --repetitions 5 --label L1
   ```

3. Find the manifest in `artifacts/bench/<run-id>.json`.
4. Create the run directory and copy the manifest unchanged.
5. Copy it to `manifest.json` and keep the original under `raw/`.
6. Write `METHOD.md` from the template.
7. Generate charts from the manifest. Put the generator command in the card.
8. Add a row to the benchmark table in `BENCHMARKS.md` only if the run is
   published; link the directory and the manifest.
9. Check the result against the manifest one more time. If they disagree,
   change the prose, never the JSON.

## Raw artifacts

- `manifest.json` and `raw/<run-id>.json`: the primary raw artifact. The
  harness writes histogram summaries, not per-sample values.
- `.hdr` files: the harness does not export them yet. If you add an export,
  put the files under `raw/` and record the histogram bounds and significant
  digits in the method card.
- Harness stdout: useful for the exact command and the summary line. Put it in
  the card or a small `raw/stdout.txt`.
- Node logs: not required for every run. If a run depends on a specific
  failure or retry, include the relevant excerpt and say which lines matter.

Screenshots of a dashboard are not data. They may illustrate a run, but the
manifest and the method card carry the numbers.

## Review checklist

- [ ] Directory name has date, host, scope, and label.
- [ ] `METHOD.md` exists and states the question and scope.
- [ ] `manifest.json` is unchanged and parses.
- [ ] `run_id` matches the file name.
- [ ] Scope label matches the manifest.
- [ ] Five repetitions, or a stated reason for fewer.
- [ ] Sample count is reported.
- [ ] Median-p99 headline and min/max spread are reported.
- [ ] No p99.99 claim below one million samples.
- [ ] No comparison across different durability, batching, or encryption
      settings without labeling.
- [ ] Charts are generated from the manifest.
- [ ] No hand-edited numbers.

The field reference is
[BENCHMARK-MANIFEST.md](../BENCHMARK-MANIFEST.md). The method and run matrix
are in [BENCHMARKS.md](../BENCHMARKS.md).
