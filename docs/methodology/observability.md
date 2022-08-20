# Storage Observability

## Research question

Which low-cost metrics make Bitcask write, sync, recovery, and compaction behavior explainable during experiments?

## Hypothesis

A small process-local metrics snapshot can explain benchmark deltas without changing the storage format or requiring an external metrics system.

## Mechanism

`DB.Metrics()` returns a stable `MetricsSnapshot` backed by Go 1.16-compatible `sync/atomic` counters.

Tracked counters:

- commits
- entries written
- bytes written
- sync count
- sync errors
- total sync latency in nanoseconds
- segment rotations
- merge runs
- merge bytes read
- merge bytes written
- recovery entries scanned
- recovery committed transaction ids
- successful group epochs and total waiters
- maximum observed group size
- last successful epoch ID
- durable append frontier
- durability retry attempts and successful retries

Lifecycle-aware KV placement also exposes `DB.KVPlacementMetrics()`:

- inline placement decisions
- value-log placement decisions
- insufficient-history threshold fallbacks

These advisory counters and their per-key observation history reset on reopen.

When autonomous recommendations are enabled, `DB.AutonomousObservation()` adds cumulative reads, writes, overwrites, large-value writes, and average sync latency. `DB.PolicyRecommendation()` is an immutable recommendation snapshot; it does not apply configuration changes.

`DB.CompactionObservation()` exposes measured logical/live/obsolete/physical bytes, merge bytes, approximate write/space amplification, and a copied per-segment read-hotness map. Read hotness is process-local and resets safely on reopen.

`DB.CommitLatency()` exposes an atomic fixed-bucket successful-commit histogram snapshot with count, p50, p95, p99, and exact observed maximum. Foreground observation adds no per-commit allocation; percentiles are conservative bucket upper bounds and reset on reopen.

Guarded autonomous application returns an `AutoTuneResult` describing recommendation, changed policies, resulting compaction/lifecycle state, and reason. `DB.PolicyAudit()` exposes a bounded copied ring of recommendation/application events, and autonomous observations include value-log total/live/stale pressure. Audit history is process-local and resets on reopen.

## Correctness invariant

Metrics are observational. They must not affect transaction visibility, recovery decisions, merge filtering, or index contents.

## Benchmark design

```bash
go test ./engine -run '^$' -bench 'BenchmarkMetricsSnapshot|BenchmarkTxCommitWithMetrics' -benchmem -count=3
```

Interpretation:

- `BenchmarkMetricsSnapshot` measures read-side observability overhead.
- `BenchmarkTxCommitWithMetrics` measures the write path with counters enabled.

## Expected trade-off

Atomic counters add small fixed overhead to the write path, but they make later experiments easier to explain: sync policy changes should move `Syncs`, compaction policy changes should move `MergeBytesRead` and `MergeBytesWritten`, and recovery experiments should move recovery counters.

## Observed results

Snapshot and write-path overhead (Apple M3 Max, `-benchmem -benchtime=100ms -count=3`, median):

| Workload | Commits | Entries | Bytes | Syncs | Rotations | Merge read/write | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `MetricsSnapshot` (read counters) | — | — | — | — | — | — | **9.25 ns/op, 0 B/op, 0 allocs/op** — snapshot is free enough for hot loops |
| `TxCommitWithMetrics` (32B-class put) | 1 / op | 1 / op | ~2.9 KB/op | 0 (sync=false) | 0 | 0 / 0 | **38,835 ns/op, 2,872 B/op, 15 allocs/op** — matches Track A baseline band |
| Group commit, 16 writers | per-commit | — | — | **~0.063 / commit** | — | — | from `ConcurrentCommitPolicies` — [durability-pipeline](../tracks/durability-pipeline.md) |
| Static mid-bench merge | — | — | — | — | — | **0 / ~69 KB written** | from `CompactionForegroundImpact/static-merge` — [slo-compaction](../tracks/slo-compaction.md) |

Counters explain policy deltas (`Syncs`, `MergeBytesWritten`) without changing the on-disk format. Re-run:

```bash
go test ./engine -run '^$' -bench '^Benchmark(MetricsSnapshot|TxCommitWithMetrics)$' -benchmem -count=5
```
