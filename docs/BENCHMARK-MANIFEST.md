# Benchmark manifest

Every benchmark run writes one JSON manifest. The manifest is the result: host,
commit, workload, durability, sample counts, histograms, repetitions, and
scope. A table of numbers without a manifest is not publishable.

The Rust definition is `whippet_core::manifest::BenchManifest` in
`crates/whippet-core/src/manifest.rs`. The JSON Schema is
[`schemas/bench-manifest.schema.json`](../schemas/bench-manifest.schema.json)
(draft 2020-12). The schema and the structs describe the same fields.

## Rules

- The harness writes the manifest. Do not hand-edit it.
- Keep the file name equal to `run_id.json`.
- Publish the manifest unchanged next to its method card.
- `schema_version` is `1`. A change to the field set requires a schema version
  bump and a migration note.
- `additionalProperties` is `false` in the schema. An unknown field means the
  manifest and the schema disagree; fix the harness, do not relax the schema.

## Top-level fields

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | integer, const 1 | Manifest schema version. |
| `run_id` | string | UUID for the run; also the filename. |
| `created_at` | string, RFC 3339 | When the manifest was written. |
| `git_sha` | string | `git rev-parse HEAD`, or `unknown`. |
| `git_dirty` | boolean | True when `git status --porcelain` was non-empty. |
| `image_digest` | string or null | From `WHIPPET_IMAGE_DIGEST`. |
| `host` | `HostInfo` | Machine and runtime. |
| `workload` | `WorkloadSpec` | What was run. |
| `throughput` | `ThroughputSummary` | Throughput of the headline repetition. |
| `latency` | `LatencySummary` | Latency of the headline repetition. |
| `stages` | array of `StageLatency` | Per-stage breakdown. Empty today. |
| `repetitions` | array of `RepetitionResult` | Every repetition, in order. |
| `notes` | array of string | Harness notes. |
| `labels` | array of string | Free-form labels; currently the scope label. |

The top-level `throughput` and `latency` are copies of the repetition with the
median p99, not the best repetition and not an average. The full set is in
`repetitions`.

## HostInfo

| Field | Type | Meaning |
|---|---|---|
| `hostname` | string | `hostname`, or `unknown`. |
| `os` | string | `uname -s` (Darwin, Linux). |
| `kernel` | string | `uname -r`. |
| `cpu_model` | string | CPU brand string. |
| `physical_cores` | integer | Physical cores. The current harness sets this equal to logical cores. |
| `logical_cores` | integer | Logical CPUs. |
| `memory_bytes` | integer | Total memory in bytes. |
| `virtualization` | string | `native`, `docker-desktop`, `orbstack`, `linux-vm`, or a value from `WHIPPET_VIRTUALIZATION`. |
| `docker_version` | string or null | `docker --version`, when available. |
| `governor` | string or null | Linux CPU governor, when readable. |
| `smt_enabled` | boolean or null | Always null in the current harness. |
| `isolated_cores` | string or null | From `WHIPPET_ISOLATED_CORES`. |

`scope_label()` is derived from `virtualization` and `os`:

- `native` and an OS containing `linux` -> `headline`.
- `docker-desktop`, `orbstack`, or `linux-vm` -> `dev-loop`.
- Anything else -> `dev-loop`.

The label is written into `labels`, but the rule lives in code. A manifest with
`virtualization: "native"` and `os: "Darwin"` is still `dev-loop`.

## WorkloadSpec

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Run label, for example `L1` or `smoke`. |
| `mode` | string | `durable` (or `turbo` when it exists). |
| `transport` | string | `http` in the current harness. |
| `payload_bytes` | integer | Nominal payload size. Currently a fixed 96, not a measured request size. |
| `symbols` | array of string | Symbols used by the generator. |
| `partitions` | integer | Configured trading partitions. |
| `rate_per_sec` | integer | Target scheduled rate. |
| `duration_secs` | integer | Measured duration. |
| `warmup_secs` | integer | Warmup before measurement. |
| `repetitions` | integer | Requested repetition count. |
| `subscribers` | integer | Fanout subscribers. Always 0 today. |
| `seed` | integer | Generator seed. |
| `durability` | string | Topic durability, for example `persisted`. |
| `consumer_offset_durability` | string | Consumer-offset durability. |
| `batch_size` | integer | Producer batch size. 1 today. |
| `encryption` | boolean | Stream encryption requested. False today. |

## ThroughputSummary

| Field | Type | Meaning |
|---|---|---|
| `target_per_sec` | integer | Requested rate. |
| `achieved_per_sec` | number | `completed_samples / measured_secs`. Includes failed requests. |
| `goodput_per_sec` | number | `(completed_samples - error_count) / measured_secs`. |
| `error_count` | integer | Failed requests plus samples skipped because the in-flight cap was full. |
| `scheduled_samples` | integer | Samples scheduled after warmup. |
| `completed_samples` | integer | Samples that reached the collector, including errors. |
| `scheduler_slip` | `LatencySummary` | Generator wake time minus deadline, in microseconds. |

`achieved_per_sec` can exceed the target when the pacer catches up. Read it
with `scheduler_slip` and `error_count`, not alone.

Current harness caveat: when the in-flight cap is full, the skipped sample is
recorded both as a failed sample and as a spawn error. `error_count` can
therefore be higher than the number of failed requests. Treat it as an upper
bound until the harness is fixed.

## LatencySummary

All values are microseconds.

| Field | Type | Meaning |
|---|---|---|
| `count` | integer | Samples in the histogram. |
| `min` | number | Minimum. |
| `mean` | number | Mean. |
| `p50` | number | 50th percentile. |
| `p90` | number | 90th percentile. |
| `p99` | number | 99th percentile. |
| `p999` | number | 99.9th percentile. |
| `p9999` | number | 99.99th percentile. Do not quote it below one million samples. |
| `max` | number | Maximum. |
| `p50_ci95` | array of 2 numbers, or null | 95% bootstrap interval for p50. Always null today. |
| `p99_ci95` | array of 2 numbers, or null | 95% bootstrap interval for p99. Always null today. |

An empty histogram serializes as all zeros with `count: 0`. That is a valid
manifest but not a valid result.

## StageLatency

| Field | Type | Meaning |
|---|---|---|
| `stage` | string | Stage name, for example `gateway`, `match`, `publish`, `fanout`. |
| `latency` | `LatencySummary` | Stage latency in microseconds. |

The current harness always writes an empty `stages` array. There are no T0/T1/T2
timestamps in the request path yet.

## RepetitionResult

| Field | Type | Meaning |
|---|---|---|
| `index` | integer | Zero-based repetition index. |
| `started_at` | string, RFC 3339 | When the repetition started. |
| `duration_secs` | number | Measured duration in seconds. |
| `throughput` | `ThroughputSummary` | This repetition's throughput. |
| `latency` | `LatencySummary` | This repetition's latency. |
| `stages` | array of `StageLatency` | Empty today. |

The manifest keeps every repetition so a reader can see the spread. The
headline is the median by p99; the min and max p99 across repetitions are the
spread to publish with it.

## Example

This is a complete manifest in the shape the harness writes. It is the smoke
run described in [BENCHMARKS.md](BENCHMARKS.md), not a benchmark result.

```json
{
  "schema_version": 1,
  "run_id": "e27889ad-fd31-421c-806a-b2e38699b27f",
  "created_at": "2026-11-14T13:32:16.682695+00:00",
  "git_sha": "8eca5ff3283688a2f2e8a8a9d076e023f7e57715",
  "git_dirty": true,
  "image_digest": null,
  "host": {
    "hostname": "devs-macbook-air.local",
    "os": "Darwin",
    "kernel": "27.2.0",
    "cpu_model": "Apple M4",
    "physical_cores": 10,
    "logical_cores": 10,
    "memory_bytes": 17179869184,
    "virtualization": "dev-loop",
    "docker_version": "Docker version 29.9.2, build a82e528",
    "governor": null,
    "smt_enabled": null,
    "isolated_cores": null
  },
  "workload": {
    "name": "smoke",
    "mode": "durable",
    "transport": "http",
    "payload_bytes": 96,
    "symbols": ["BTC-USD", "ETH-USD", "SOL-USD"],
    "partitions": 16,
    "rate_per_sec": 200,
    "duration_secs": 3,
    "warmup_secs": 1,
    "repetitions": 1,
    "subscribers": 0,
    "seed": 1592590337,
    "durability": "persisted",
    "consumer_offset_durability": "persisted",
    "batch_size": 1,
    "encryption": false
  },
  "throughput": {
    "target_per_sec": 200,
    "achieved_per_sec": 200.0,
    "goodput_per_sec": 200.0,
    "error_count": 0,
    "scheduled_samples": 600,
    "completed_samples": 600,
    "scheduler_slip": {
      "count": 600,
      "min": 124.0,
      "mean": 1417.679999999999,
      "p50": 1333.0,
      "p90": 1730.0,
      "p99": 6227.0,
      "p999": 20879.0,
      "p9999": 20879.0,
      "max": 20879.0,
      "p50_ci95": null,
      "p99_ci95": null
    }
  },
  "latency": {
    "count": 600,
    "min": 1886.0,
    "mean": 5262.1866666666665,
    "p50": 3257.0,
    "p90": 6143.0,
    "p99": 54495.0,
    "p999": 64159.0,
    "p9999": 64159.0,
    "max": 64159.0,
    "p50_ci95": null,
    "p99_ci95": null
  },
  "stages": [],
  "repetitions": [
    {
      "index": 0,
      "started_at": "2026-11-14T13:32:12.580944+00:00",
      "duration_secs": 3.0,
      "throughput": {
        "target_per_sec": 200,
        "achieved_per_sec": 200.0,
        "goodput_per_sec": 200.0,
        "error_count": 0,
        "scheduled_samples": 600,
        "completed_samples": 600,
        "scheduler_slip": {
          "count": 600,
          "min": 124.0,
          "mean": 1417.679999999999,
          "p50": 1333.0,
          "p90": 1730.0,
          "p99": 6227.0,
          "p999": 20879.0,
          "p9999": 20879.0,
          "max": 20879.0,
          "p50_ci95": null,
          "p99_ci95": null
        }
      },
      "latency": {
        "count": 600,
        "min": 1886.0,
        "mean": 5262.1866666666665,
        "p50": 3257.0,
        "p90": 6143.0,
        "p99": 54495.0,
        "p999": 64159.0,
        "p9999": 64159.0,
        "max": 64159.0,
        "p50_ci95": null,
        "p99_ci95": null
      },
      "stages": []
    }
  ],
  "notes": [
    "Open-loop absolute-deadline pacing; latency measured from the scheduled deadline.",
    "M4/Docker Desktop is a dev-loop measurement, not a headline claim.",
    "HTTP ingress is a convenience path; the Linux headline uses TCP on a native host."
  ],
  "labels": ["dev-loop"]
}
```

## Validation

At minimum, check that both files parse:

```sh
python3 -m json.tool schemas/bench-manifest.schema.json > /dev/null
python3 -m json.tool artifacts/bench/<run-id>.json > /dev/null
```

If the `jsonschema` Python package is available:

```sh
python3 - <<'PY'
import json
import jsonschema

schema = json.load(open("schemas/bench-manifest.schema.json"))
manifest = json.load(open("artifacts/bench/<run-id>.json"))
jsonschema.Draft202012Validator(schema).validate(manifest)
print("manifest validates")
PY
```

The schema uses `additionalProperties: false`, so a manifest with a field the
schema does not know is rejected. That is intentional.

## What is always empty or fixed today

| Field | Current value | Note |
|---|---|---|
| `stages` | `[]` | No stage timestamps. |
| `p50_ci95`, `p99_ci95` | `null` | No bootstrap intervals. |
| `payload_bytes` | `96` | Nominal, not measured. |
| `subscribers` | `0` | No fanout workload. |
| `encryption` | `false` | No stream encryption. |
| `batch_size` | `1` | One HTTP order per request. |
| `physical_cores` | equal to `logical_cores` | The harness does not read the physical topology. |
| `smt_enabled` | `null` | Not detected. |
| `labels` | one scope label | The array is free-form. |
| `notes` | three fixed lines | Harness notes. |

Fix the harness before publishing a manifest that claims otherwise.
