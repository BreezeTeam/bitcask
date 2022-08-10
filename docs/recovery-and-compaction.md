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

Merge reads candidate segment files, skips deleted/expired/stale entries, and rewrites only live entries into a new active file. `CompactionOptions` can change which segment ids are selected before rewrite:

- `CompactionByFileID`: default file-order behavior.
- `CompactionByGarbageRatio`: prefer files estimated to contain more garbage.
- `CompactionHotCold`: downweight hot segments when selecting garbage-heavy candidates.

Fault injection wraps the DataFile `RWManager` when `FaultInjectionOptions.Enable` is true. It can inject deterministic write failures, short writes, sync failures, write corruption, and recovery read corruption. The default path is disabled.

## Correctness invariant

Recovery must ignore uncommitted entries. Merge must never drop the newest committed live value for a key and must preserve tombstone/TTL semantics. Fault injection tests are valid only when they assert this invariant before studying performance.

## Benchmark design

Compaction policy:

```bash
go test . -run '^$' -bench 'BenchmarkDBMergePolicy' -benchmem -count=3
go test ./experiments/compaction -run '^$' -bench Benchmark -benchmem -count=3
```

Fault injection:

```bash
go test . -run '^$' -bench 'BenchmarkFaultInjection' -benchmem -count=3
go test ./experiments/fault -run '^$' -bench Benchmark -benchmem -count=3
```

## Expected trade-off

Recovery stays simple because transaction visibility is derived from committed transaction ids. Compaction policy can reduce rewrite work, but the current core segment statistics are intentionally conservative and should be refined before drawing production conclusions. Fault injection adds a small opt-in wrapper cost and makes crash assumptions measurable.

## Observed result template

| Experiment | Workload | Files before/after | Live keys | Error injected | Result | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| TBD | TBD | TBD | TBD | TBD | TBD | TBD |
