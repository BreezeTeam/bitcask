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

## Correctness invariant

Metrics are observational. They must not affect transaction visibility, recovery decisions, merge filtering, or index contents.

## Benchmark design

```bash
go test . -run '^$' -bench 'BenchmarkMetricsSnapshot|BenchmarkTxCommitWithMetrics' -benchmem -count=3
```

Interpretation:

- `BenchmarkMetricsSnapshot` measures read-side observability overhead.
- `BenchmarkTxCommitWithMetrics` measures the write path with counters enabled.

## Expected trade-off

Atomic counters add small fixed overhead to the write path, but they make later experiments easier to explain: sync policy changes should move `Syncs`, compaction policy changes should move `MergeBytesRead` and `MergeBytesWritten`, and recovery experiments should move recovery counters.

## Observed result template

| Workload | Commits | Entries | Bytes | Syncs | Rotations | Merge read/write | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TBD | TBD | TBD | TBD | TBD | TBD | TBD | TBD |
