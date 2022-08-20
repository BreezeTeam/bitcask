# Recovery and Compaction

## Research question

How can a Bitcask-style log preserve transaction visibility while supporting delete markers, TTL, merge, compaction policy, and deterministic crash experiments?

## Hypothesis

Committed-marker recovery and live-entry rewrite are enough to keep the storage engine understandable if every advanced experiment preserves the same visibility invariant.

## Mechanism

Recovery scans data files and builds two temporary structures:

- unconfirmed records discovered in log order
- committed transaction ids discovered from entries whose status is `Committed`

After scanning, a record is installed into the in-memory index only if its transaction id appears in the committed set. This makes a partial transaction tail invisible after reopen.

Merge reads candidate segment files, skips deleted/expired/stale entries, and rewrites only live entries into a new active file. For each source, a versioned CRC `merge.manifest` records prepared and installed target-file ranges. Replacement files are synchronized before the installed phase; source removal is followed by directory sync and manifest cleanup. Before index rebuild, `Open` retains a source when targets are missing, or removes it when every installed target exists; an explicitly empty target range is valid for an all-obsolete source and is treated as complete. `CompactionOptions` can change which segment ids are selected before rewrite:

- `CompactionByFileID`: default file-order behavior.
- `CompactionByGarbageRatio`: prefer files estimated to contain more garbage.
- `CompactionHotCold`: downweight hot segments when selecting garbage-heavy candidates.

Fault injection wraps the DataFile `RWManager` when `FaultInjectionOptions.Enable` is true. It can inject deterministic write failures, short writes, sync failures, write corruption, and recovery read corruption. The default path is disabled.

Manual experiments may use `DB.MergeWithBudget(maxBytes)` to cap selected logical segment bytes, or `DB.MergeRecommended(p99)` to execute a positive SLO-controller recommendation. Default `DB.Merge()` retains its existing unbudgeted behavior. Budget selection never changes liveness filtering.

## Correctness invariant

Recovery must ignore uncommitted entries. Merge must never drop the newest committed live value for a key and must preserve tombstone/TTL semantics. Fault injection tests are valid only when they assert this invariant before studying performance.

## Benchmark design

Compaction policy:

```bash
go test ./engine -run '^$' -bench 'BenchmarkDBMergePolicy' -benchmem -count=3
go test ./experiments/compaction -run '^$' -bench Benchmark -benchmem -count=3
```

Fault injection:

```bash
go test ./engine -run '^$' -bench 'BenchmarkFaultInjection' -benchmem -count=3
go test ./experiments/fault -run '^$' -bench Benchmark -benchmem -count=3
```

## Expected trade-off

Recovery stays simple because transaction visibility is derived from committed transaction ids. Compaction policy can reduce rewrite work, but the current core segment statistics are intentionally conservative and should be refined before drawing production conclusions. Fault injection adds a small opt-in wrapper cost and makes crash assumptions measurable.

## Observed results

Representative recovery / compaction-fault observations (Apple M3 Max; see track chapters for full tables):

| Experiment | Workload | Files before/after | Live keys | Error injected | Result | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Torn commit-marker recovery | 1 truncated committed-marker prefix | 1 data file | 0 incomplete keys visible | torn write at commit-marker boundary | open or reject safely; incomplete entry hidden | `BenchmarkRecoveryTornCommitMarkers` ≈ 129 µs/op — [crash-explorer](../tracks/crash-explorer.md) |
| Many partial transactions | 1,000 committed + 1,000 partial | rebuilt index on reopen | only committed IDs visible | none (tail is incomplete txs) | ≈ 7.03 ms/op full recovery | `BenchmarkRecoveryManyPartialTransactions` |
| Group-commit subprocess crash | concurrent group epoch, child `os.Exit` | same dir reopened by parent | acknowledged epoch durable; pre-marker hidden | abrupt exit mid/after epoch | ≈ 36.9 ms/op oracle-checked | `BenchmarkGroupCommitSubprocessRecovery` — [track-c](../../results/track-c-crash.txt) |
| Budgeted vs static merge | overwrite-hotset preload (64 keys × 128B) | merge mid-benchmark | live keys preserved | none | budgeted ≈ 44 µs (~baseline); static ≈ 1.74 ms + ~69 KB rewrite | `CompactionForegroundImpact` — [slo-compaction](../tracks/slo-compaction.md) |
