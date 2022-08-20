# SLO-aware Compaction

## Research question

Can compaction decisions use measured live and obsolete bytes instead of segment-capacity placeholders, and later respect a foreground latency SLO?

## Hypothesis

Logical bytes parsed from each segment and live bytes referenced by the authoritative B+ tree provide a safer and more useful garbage estimate than configured segment capacity. These measurements can become inputs to a pure latency/amplification controller without allowing statistics to decide record liveness.

## Current mechanism

`DB.Merge()` supports file-ID, garbage-ratio, and hot/cold candidate modes. `mergeSegments` now builds measured snapshots:

- logical bytes: the sum of valid encoded entries parsed from a data file
- live bytes: the encoded size of non-deleted, non-expired records currently referenced by the in-memory B+ tree
- obsolete bytes: logical bytes minus live bytes
- garbage ratio: obsolete bytes divided by logical bytes

Logical size is intentionally separate from physical size. Data files are preallocated to `SegmentSize`, so filesystem length cannot represent bytes containing entries.

The B+ tree remains the liveness authority. Segment scanning measures log contents; it does not independently decide that a stale record is live. If stats cannot be built safely, candidate selection falls back conservatively to sorted file-ID order.

Core Get and range/prefix value resolution now increment concurrency-safe per-segment read counters. `mergeSegments` includes these measured counts, so the hot/cold picker no longer receives placeholder zero hotness for RAM index modes.

## Correctness invariants

1. `0 <= liveBytes <= logicalBytes` for every measured segment.
2. Physical preallocation is never counted as logical garbage.
3. Only the current B+ tree record contributes live bytes for a key.
4. Deleted and expired current records do not contribute live bytes.
5. Statistics prioritize candidates only; merge still validates every entry against current index position, tombstone, and TTL state.
6. Missing or corrupt measurements fall back to file-ID selection rather than silently excluding a segment.
7. Merge must preserve every latest committed live value regardless of candidate mode.

## Tests

Root-package tests cover:

- exact nonnegative logical/live bounds across rotated files
- an overwritten record creating measurable garbage
- deleted and expired records being excluded from live bytes
- garbage-ratio selection returning a measured candidate
- live-data preservation after merge
- tombstone and TTL preservation after merge

Run:

```bash
go test -count=1 ./engine -run 'Test(PickMerge|MergeSegments|DBMergePolicy)'
```

## Benchmarks

Core measured-stat construction:

```bash
go test ./engine -run '^$' -bench '^BenchmarkMergeSegmentsMeasuredStats$' -benchmem -count=5
```

Pure picker cost:

```bash
go test ./experiments/compaction -run '^$' -bench 'Benchmark(Pick|CompactionPolicy)' -benchmem -count=5
```

The core benchmark scans rotated files and enumerates B+ tree records. The pure benchmark measures policy math only. Neither currently represents background compaction impact on foreground p99 latency.

## Core compaction observation

`DB.CompactionObservation()` returns an immutable snapshot containing:

- logical, live, and obsolete bytes
- physical preallocated bytes
- merge bytes written
- approximate write amplification `(foreground + merge bytes) / foreground bytes`
- approximate space amplification `physical bytes / live bytes`
- a copied per-segment read-count map

Read counts are process-local and reset on reopen without affecting compaction correctness. Physical space amplification intentionally reflects preallocation and therefore represents reserved filesystem space, not necessarily allocated disk blocks.

```bash
go test -count=1 ./engine -run 'Test(CompactionObservation|MergeSegmentsIncludeMeasuredReadCount)'
go test ./engine -run '^$' -bench '^BenchmarkCompactionObservation$' -benchmem -count=5
```

## Pure SLO controller

`experiments/compaction.Controller` consumes:

- foreground p99 and target p99
- pending garbage and total data bytes
- write amplification
- space amplification
- recent compaction throughput (reserved for throughput-aware refinement)
- workload phase
- current budget and cooldown state

It outputs a bounded byte budget, picker mode, confidence, reason, and whether the stable control state changed. The decision priority is:

1. Emergency space amplification permits bounded work even when latency is high.
2. Foreground p99 above target throttles the budget to zero.
3. Garbage below the minimum threshold performs no work.
4. High write amplification reduces the rewrite budget.
5. Otherwise garbage pressure receives a base or elevated budget, reduced during write-heavy phases.

Read-heavy phases recommend hot/cold selection; overwrite-heavy phases recommend garbage-ratio selection. Consecutive-window hysteresis, logical-window cooldown, and base-budget-sized steps prevent oscillation or sudden budget jumps.

Pure tests cover latency throttling, emergency space pressure, ordinary garbage pressure, phase-aware picker selection, high write-amplification limits, noisy alternating windows, cooldown, and bounded budget changes.

```bash
go test -count=1 ./experiments/compaction
go test ./experiments/compaction -run '^$' -bench 'BenchmarkSLOController' -benchmem -count=5
```

`CompactionOptions.EnableSLORecommendation` now adds a recommendation-only core bridge. `DB.RecommendCompaction(foregroundP99)` builds an immutable core observation, maps the optional autonomous workload phase, evaluates the pure controller, and returns budget, picker, reason, confidence, transition, and availability. Passing a non-positive p99 uses the measured `DB.CommitLatency().P99` bucket bound. Target p99, pressure thresholds, budget bounds, hysteresis, and cooldown are configurable.

The recommendation API does not call `Merge` or mutate `CompactionOptions.Mode`; callers can inspect recommendations safely while static behavior remains authoritative.

For explicit manual execution, `DB.MergeWithBudget(maxBytes)` limits selected candidates by measured logical segment bytes, and `DB.MergeRecommended(p99)` evaluates the current recommendation and executes only when it is available with a positive budget. A zero/throttled recommendation performs no merge. Default `DB.Merge()` remains unchanged and unbudgeted.

Budget enforcement is deterministic and never splits a segment: candidates that would exceed the remaining budget are skipped. The actual rewrite still validates current liveness and preserves live data.

`DB.CompactionAudit()` returns an immutable oldest-to-newest snapshot of bounded recommendation and explicit-execution events. Each event includes a monotonic sequence, action (`recommend`, `noop`, or `merge`), recommendation fields, measured logical/obsolete bytes, amplification snapshot, foreground p99 input, merge bytes written, execution status, and error string. The ring is process-local and exists only when SLO recommendation is enabled; it is research observability, not recovery state.

```bash
go test -count=1 ./engine -run 'Test(CompactionRecommendation|CompactionAudit|MergeRecommended)'
go test ./engine -run '^$' -bench '^BenchmarkCompaction(Recommendation|AuditSnapshot)$' -benchmem -count=5
```

No background scheduler should be introduced before merge has a crash-recoverable staging/manifest protocol and DB close can stop all workers safely.

## Workload and foreground-impact methodology

`experiments/compaction` now provides deterministic overwrite-hotset, cold-garbage, and mixed read/write operation streams plus a report for logical/live/obsolete/rewrite bytes and write/space amplification. These pure reports validate workload shape independently of filesystem noise.

The core `BenchmarkCompactionForegroundImpact` compares no merge, static merge, and logical-byte-budgeted merge. It records foreground write p50/p99 and `MergeBytesWritten` for the same preloaded overwrite-heavy database.

```bash
go test ./experiments/compaction -run '^$' -bench '^BenchmarkAnalyzeCompactionWorkload$' -benchmem -count=5
go test ./engine -run '^$' -bench '^BenchmarkCompactionForegroundImpact$' -benchmem -count=5
```

The core harness currently runs merge synchronously in the benchmark goroutine, so the measured foreground sample at the merge boundary includes only the foreground write, not concurrent background contention. A later background scheduler benchmark must measure actual overlapping IO and p99 impact after crash-safe worker shutdown exists.

### Measured results

`BenchmarkCompactionForegroundImpact` (median of `-count=5`):

| Mode | ns/op | p50-ns | p99-ns | rewrite-B | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| no-merge | 39,603 | 3,666 | 359,000 | 0 | 1,135 | 16 |
| static-merge | 1,737,462 | 4,583 | 653,125 | 69,368 | 1,911 | 29 |
| budgeted-merge (8 KiB) | 43,823 | 4,042 | 355,875 | 0 | 1,383 | 19 |

Budgeted merge stays within ~11% of the no-merge baseline (~44 vs 40 µs) because the 8 KiB logical-byte budget skips candidates that would exceed it (`rewrite-B = 0`). Static `Merge()` amortizes to ~1.74 ms/op and rewrites ~69 KB on the same preload — roughly a 44× inflation of amortized commit cost when merge is unbounded.

Other suite rows:

| Benchmark | ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| `CompactionRecommendation` | 1,273,127 | 264,161 | 3,400 | median of `-count=5`; observation + controller |
| `AnalyzeCompactionWorkload` | 139,930 | 108,829 | 21 | pure workload math (median of `-count=5`) |

`BenchmarkDBMergePolicy` uses a fixed overwrite workload and one merge per timed op so
`make bench-d` stays on the order of minutes.

Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem`. Raw:
[`../../results/track-d-compaction.txt`](../../results/track-d-compaction.txt).

## Limitations

- Stats are rebuilt by scanning candidate files, so selection cost scales with log bytes.
- Per-segment read hotness is process-local; persisted age and decayed/windowed hotness are not yet measured.
- Write/space amplification snapshots are approximate and rebuilt by scanning files/indexes.
- Controller thresholds are hypotheses and have not been calibrated against end-to-end workloads.
- `CompactionBytesSec` is reserved in the observation but not yet used in budget math.
- Compaction audit is process-local and intentionally not persisted across reopen.
- Merge uses a versioned CRC manifest with prepared/installed recovery (see [crash explorer](crash-explorer.md) and [recovery](../methodology/recovery-and-compaction.md)); it is not a fully concurrent background worker with crash-safe overlapping IO.
- Merge concurrency with foreground writes needs a stronger snapshot/locking contract before a continuous background scheduler is enabled.
