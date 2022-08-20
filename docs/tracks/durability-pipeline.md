# Modern Durability Pipeline

## Research question

Can concurrent writers share one durability barrier while preserving committed-marker recovery and returning a common sync result to every writer in an epoch?

## Current group-commit mechanism

`SyncPolicyGroupCommit` now uses a cross-writer coordinator rather than mapping directly to one sync per transaction.

The write path is split into two phases:

1. Under the DB write lock, a transaction appends entries and the final committed marker, updates indexes, records dirty bytes/commits, and joins the current group epoch.
2. The transaction releases the DB write lock and waits for its epoch result. One coordinator flush covers all waiters collected before `GroupMaxWrites` or `GroupMaxDelay` closes the epoch.

The coordinator assigns every waiter a monotonic append sequence. Each closed epoch has a monotonic epoch ID and maximum included sequence (durability frontier). A successful sync advances the durable frontier; a failed sync resolves waiters with the error but does not advance it. The pending epoch is strictly bounded by `GroupMaxWrites`; arrivals wait for the coordinator to detach a full epoch rather than overflowing the configured size.

The coordinator:

- maintains a pending waiter set
- closes an epoch at `GroupMaxWrites`
- starts a timer for `GroupMaxDelay`
- elects one flush path
- broadcasts the same sync result to all captured waiters
- starts a new timer if writers arrived during a sync
- flushes pending waiters during `DB.Close`

Default group bounds are 16 writes and 1ms when the explicit values are zero. The mechanism still serializes append/index mutation under `db.mu`; only durability waiting occurs outside that lock.

## Visibility and durability semantics

The current linearization contract is:

- The committed marker and index publication establish in-process visibility.
- A group-commit writer returns success only after the epoch sync succeeds.
- A reader may observe a value while its writer is still waiting for the group barrier.
- A sync error means durability is unknown, not that appended bytes or index changes were rolled back.
- Every commit attempt remains terminal and releases its transaction lock.

Recovery continues to require a valid committed marker before installing transaction records.

## Correctness invariants

1. Every successful waiter belongs to an epoch whose sync completed successfully.
2. Multiple writers in one epoch receive one shared sync result.
3. A sync failure is propagated to every waiter captured by that epoch.
4. No writer waits while holding the main DB write lock.
5. Appends and index publication remain serialized.
6. `DB.Close` rejects new transactions, flushes pending group waiters, resolves them, then closes the active file.
7. The final committed-marker recovery invariant is unchanged.
8. A failed sync is reported as durability-unknown and never misrepresented as rollback.

## Semantic fault points

Durability ordering exposes named deterministic faults before value append, value sync, main sync, every entry append, and the final commit marker. Tests prove pre-marker faults leave the whole transaction uncommitted, separated-value faults propagate safely, main-sync faults report durability unknown, and occurrence counts are replayable.

```bash
go test -count=1 ./engine -run '^TestSemanticFault'
go test ./engine -run '^$' -bench '^BenchmarkSemanticFaultPoint' -benchmem -count=5
```

## Tests

```bash
go test -count=1 ./engine -run '^TestGroupCommit'
```

Coverage includes:

- many concurrent writers sharing fewer syncs than commits
- max-write epoch closure
- sync-error fanout to all epoch waiters
- transaction unlock after group failure
- close flushing a pending long-delay epoch
- reopen persistence after group commit

The race suite exercises coordinator, close, writer, and metrics interactions:

```bash
go test -race -count=1 ./...
```

## Benchmarks

```bash
go test ./engine -run '^$' -bench '^BenchmarkConcurrentCommitPolicies$' -benchmem -count=5
```

The benchmark compares none, every-commit, and group commit at writer counts 1, 4, and 16. It reports:

- throughput through the standard `ns/op` result
- p50 commit latency
- p99 commit latency
- syncs per successful commit

Latency collection has its own overhead and is intended for policy comparison rather than a zero-overhead production histogram.

`DB.Metrics()` also exposes fixed group-size distribution buckets: single-writer epochs, size 2, size 3-4, size 5-8, and size 9+. These buckets make coalescing shape visible without returning mutable slices from metrics snapshots.

### Measured results

`BenchmarkConcurrentCommitPolicies` (policy x concurrent writers):

| Benchmark | ns/op | p50-ns | p99-ns | syncs/commit | B/op | allocs/op |
|---|---|---|---|---|---|---|
| policy=none/writers=1 | 39237 | 2417 | 19333 | 0 | 825 | 15 |
| policy=none/writers=4 | 39489 | 3292 | 8671375 | 0 | 806 | 15 |
| policy=none/writers=16 | 39038 | 3833 | 9563792 | 0 | 802 | 15 |
| policy=every/writers=1 | 7704016 | 8007250 | 10067500 | 1.000 | 804 | 15 |
| policy=every/writers=4 | 8792263 | 34867417 | 42164500 | 1.000 | 813 | 15 |
| policy=every/writers=16 | 8873376 | 143041667 | 156061292 | 1.000 | 827 | 15 |
| policy=group/writers=1 | 8553166 | 8021250 | 10035125 | 1.000 | 1085 | 22 |
| policy=group/writers=4 | 2126337 | 8034208 | 12041791 | 0.2509 | 994 | 20 |
| policy=group/writers=16 | 636271 | 9554875 | 18622875 | 0.06296 | 985 | 19 |

At 16 concurrent writers, group commit reduces per-commit `ns/op` roughly 14x versus every-commit sync (636271 vs 8873376) while collapsing sync rate from 1.0 to ~0.063 syncs per commit — one shared fsync serves an epoch of writers instead of one fsync per writer.

Other representative benchmarks (single sample dimension held at batch=1/value=32B for `TxCommitSyncPolicy`, batch=64/value=1024B shown for large-batch comparison):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| TxCommitSyncPolicy/policy=none/batch=1/value=32B | 73200 | 901 | 15 |
| TxCommitSyncPolicy/policy=every-commit/batch=1/value=32B | 8776056 | 891 | 15 |
| TxCommitSyncPolicy/policy=group/batch=1/value=32B | 11359764 | 1128 | 21 |
| TxCommitSyncPolicy/policy=none/batch=64/value=1024B | 2396188 | 109019 | 617 |
| TxCommitSyncPolicy/policy=every-commit/batch=64/value=1024B | 11833807 | 107870 | 578 |
| TxCommitSyncPolicy/policy=group/batch=64/value=1024B | 13262305 | 107978 | 584 |
| TxCommitAdaptiveSync/every-commit | 8188748 | 2872 | 15 |
| TxCommitAdaptiveSync/group | 10846625 | 3126 | 21 |
| TxCommitAdaptiveSync/adaptive | 407770 | 2892 | 15 |
| ExplicitFlush | 9883253 | 899 | 15 |
| GroupCommitDurabilityResources/separated=false | 666952 | 3070 | 20 |
| GroupCommitDurabilityResources/separated=true | 1262828 | 2071 | 21 |
| AdaptiveSyncDelayedWrite | 82098 | 845 | 15 |

`TxCommitAdaptiveSync/adaptive` shows the benefit of delaying sync under the dirty-bytes/commits thresholds: roughly 20x faster than forcing a sync every commit, at the cost of a bounded durability window. `AdaptiveSyncDelayedWrite` confirms the background max-delay loop flushes intentionally delayed writes (~0.0156 syncs/commit) without requiring a later foreground write.

Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, -benchmem -count=5; sync ns/op is device-dependent. Raw: ../../results/track-b-durability.txt

## Durability resource ordering

`syncDurabilityResources` now enforces value-log-before-main-log ordering whenever KV separation is enabled. Value-log append tracks the exact dirty segment IDs, so a barrier synchronizes only touched value segments rather than every open historical segment:

1. append value bytes to one or more value-log segments
2. sync each dirty value-log segment
3. append pointer records and the committed marker
4. sync retained rotated main files and the active main log
5. acknowledge the commit or group epoch

Dirty value-segment IDs are cleared only after all selected value-log syncs succeed. A value-log sync failure therefore retains the complete dirty set for retry. If every value segment sync succeeds but a later main-log sync fails, the value-log dirty set may be cleared safely: those bytes are already durable, while the DB-wide failed-durability state and pending main-log resources force a later barrier before retry success is reported.

Data-file rotation under durable policies now retains the old `DataFile` in a pending resource set instead of forcing a foreground sync. The next every-commit, group epoch, adaptive threshold/timer, or close barrier synchronizes value logs, every retained rotated file, and the active file. Retained files close only after a successful barrier; failures keep them open for retry.

This ensures a transaction spanning rotation cannot acknowledge its final marker until every file containing earlier entries is durable, while allowing group commit to use one epoch sync. Tests prove a rotating group transaction performs one sync, failed barriers retain old files, successful retry releases them, and reopen sees the whole transaction.

The remaining gap is associating resource sets with individual sequence frontiers rather than one DB-wide pending set. Current serialized append makes the set safe, but finer ownership is required for concurrent durability retry/accounting.

## Explicit flush and retry

`DB.Flush()` executes the same ordered durability resource pipeline without closing the DB. It works independently of the configured automatic sync policy, so callers can establish an explicit barrier for `None` or delayed adaptive writes.

A failed durability attempt marks retained resources as pending. Later barriers count retries while the failure remains unresolved; success closes retained rotated files, clears dirty state, increments retry-success metrics, and records how many consecutive failed retry attempts preceded recovery. `DB.Metrics()` exposes `DurabilityRetries`, `DurabilityRetryOK`, and fixed recovery-streak buckets for success after 1, 2, 3, or 4+ failures. Tests cover semantic main-sync failure, explicit retry, whole-transaction reopen, delayed adaptive separated values, and closed-DB rejection.

```bash
go test -count=1 ./engine -run '^TestFlush'
go test ./engine -run '^$' -bench '^BenchmarkExplicitFlush$' -benchmem -count=5
```

There is no automatic exponential backoff yet; retries are triggered by a later commit barrier, adaptive timer, close, or explicit `Flush`.

## Close and error semantics

`DB.Close` marks the DB closed under the write lock, releases that lock, asks the coordinator to flush and await pending epochs, then reacquires the lock for resource close. This prevents deadlock because the sync path takes a read lock while resolving the epoch.

If the closing flush fails, pending waiters receive the same error and `Close` returns the sync error after closing resources. A later retry mechanism for dirty resources is still required before async/adaptive durability can claim bounded recovery guarantees after transient sync failures.

## Adaptive durability loop

`SyncPolicyAdaptive` now has a background max-delay loop. A foreground commit still synchronizes immediately when dirty bytes, dirty commits, or the existing adaptive decision requires it. If a commit is intentionally delayed, it notifies the loop; the loop starts one max-delay timer and flushes ordered durability resources even when no later write arrives.

`DirtyCommitsLimit` adds a count bound alongside `DirtyBytesLimit` and `AdaptiveMaxDelay`. Close stops the loop, flushes remaining dirty resources, waits for termination, and returns the last background sync error. Separated values use the same value-log-before-main-log order.

```bash
go test -count=1 ./engine -run '^TestAdaptiveSync'
go test ./engine -run '^$' -bench '^BenchmarkAdaptiveSyncDelayedWrite$' -benchmem -count=5
```

Tests cover max-delay flushing without a new foreground write, dirty-commit threshold sync, delayed dirty state, close/reopen, and separated-value close ordering.

## Limitations

- Epochs expose append sequences and a successful durable frontier, but resources are not yet attached to individual sequence ranges.
- Dirty value-log segment IDs and rotated main files are retained in DB-wide sets rather than attached to explicit per-epoch frontiers.
- A value-log sync scans segment IDs up to the active ID to find dirty segments; a direct dirty-ID worklist would avoid this metadata scan for very large segment counts.
- Timer tests observe coordinator state and use a bounded polling deadline; a future injectable clock should replace real-time scheduling tests.
- Adaptive timing tests still use real timers; an injectable clock/tick abstraction would make scheduling fully deterministic.
- A background sync failure is retained and returned by close, but dirty-resource retry/backoff is not yet implemented.
- `DB.Metrics()` exposes successful group epochs, total waiters, maximum group size, last epoch ID, durable append frontier, fixed group-size buckets, and retry recovery-streak buckets; automatic backoff scheduling is not exposed.
