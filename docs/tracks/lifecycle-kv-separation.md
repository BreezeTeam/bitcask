# Lifecycle-aware KV Separation

## Research question

Should value placement depend on lifecycle signals—size, age, read hotness, and update frequency—rather than only a static size threshold?

## Hypothesis

Large stable values benefit from value-log separation because they reduce main-log rewrite bytes. Hot or frequently updated large values can be better inline because pointer reads add indirection and overwrites create value-log garbage. Very large, old values are candidates for a future cold tier.

## Current mechanism

The core engine remains backward-compatible and threshold-based. When `KVSeparationOptions.Enable` is true, values at or above `Threshold` are appended to `values.vlog`, and the main log stores a CRC-protected `DataStructureValuePointer`.

`experiments/kvseparation` now adds a pure lifecycle policy with these inputs:

- value size
- read count
- update count
- logical age in observation windows
- recent overwrite interval

It returns one of:

- `inline`
- `value-log`
- `cold-tier` (recommendation placeholder only)

Each decision includes a reason. Insufficient history falls back to the existing size threshold. Placement changes require stable consecutive windows, preventing one noisy observation from moving a value between representations.

The pure `Store` also reports total, live, and stale value-log bytes. Overwriting a key creates a new pointer; the previous bytes become stale. This makes the future GC problem measurable without claiming that reclamation exists in the core engine.

## Correctness invariants

1. Placement changes representation only; the logical key, value, TTL, and transaction visibility remain unchanged.
2. Missing lifecycle history falls back to threshold semantics.
3. Advisory counters may reset without making persisted data unreadable.
4. Every core-visible pointer must decode, remain within the named value-log file, and return bytes matching its CRC.
5. Value-log GC must never delete source bytes while a live committed pointer references them.
6. A replacement value must be durable before its replacement pointer can be acknowledged durable.
7. Core lifecycle placement and GC remain disabled until segmented value logs and ordered multi-resource durability are implemented.

## Tests

The pure model tests cover:

- small values staying inline
- hot large values staying inline
- frequently updated large values staying inline
- stable large values using the value log
- very large old values becoming cold-tier candidates
- insufficient-history threshold fallback
- stable-window placement transitions
- exact live/stale byte accounting after overwrite

Core tests already cover threshold placement, pointer round-trip, reopen, merge, and corrupt value-log reads.

Run:

```bash
go test -count=1 ./experiments/kvseparation
go test -count=1 ./engine -run 'TestKVSeparation|TestValuePointer'
```

## Benchmarks

```bash
go test ./experiments/kvseparation -run '^$' -bench 'Benchmark(LifecyclePolicy|ValueLogStats)' -benchmem -count=5
go test ./engine -run '^$' -bench 'BenchmarkKVSeparation(Put|Get)' -benchmem -count=5
```

- `BenchmarkLifecyclePolicyDecide` isolates lifecycle classification and hysteresis cost.
- `BenchmarkValueLogStats` measures exact live/stale accounting over current pointers.
- Core benchmarks measure real inline versus separated IO and pointer resolution.

### Measured results

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkKVSeparationPut/kvsep=false/value=512B` | 56,493 | 1,784 | 15 |
| `BenchmarkKVSeparationPut/kvsep=true/value=512B` | 166,562 | 1,784 | 15 |
| `BenchmarkKVSeparationPut/kvsep=false/value=4096B` | 721,550 | 9,666 | 15 |
| `BenchmarkKVSeparationPut/kvsep=true/value=4096B` | 590,495 | 4,931 | 16 |
| `BenchmarkKVSeparationPut/kvsep=false/value=65536B` | 1,882,589 | 139,972 | 15 |
| `BenchmarkKVSeparationPut/kvsep=true/value=65536B` | 913,702 | 66,408 | 16 |
| `BenchmarkKVSeparationGet/kvsep=false` | 38,710 | 128 | 4 |
| `BenchmarkKVSeparationGet/kvsep=true` | 39,078 | 4,384 | 7 |
| `BenchmarkSegmentedValueLogAppendRead/segment=0` | 7,100 | 4,096 | 1 |
| `BenchmarkSegmentedValueLogAppendRead/segment=65536` | 16,183 | 4,139 | 1 |
| `BenchmarkValueLogGC` | 144,251,571 | 77,416 | 506 |
| `BenchmarkValueLogStatsCore` | 56,252 | 151,080 | 55 |
| `BenchmarkLifecyclePlacementPut/lifecycle=false` | 39,202 | 1,891 | 16 |
| `BenchmarkLifecyclePlacementPut/lifecycle=true` | 187,660 | 1,969 | 18 |

Median of `-count=5`. Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem -count=5`. Raw: [`../../results/track-e-kv-separation.txt`](../../results/track-e-kv-separation.txt).

## Pointer consistency invariant

At every externally visible state, a committed main-log pointer must refer to a readable value-log range whose CRC matches. Merge may preserve an existing pointer payload, but GC must copy live values and durably publish new pointers before removing an old value-log segment.

The safe future order is:

1. copy live value bytes to a replacement segment
2. sync the replacement value-log segment
3. commit replacement pointer records to the main log
4. sync every main-log resource containing those records
5. verify no live index pointer references the source segment
6. remove the source segment and fsync the directory

A checksummed GC manifest is required if pointer replacement spans multiple transactions.

## Limitations

- Core placement still uses only a static threshold.
- Lifecycle observations are supplied to a pure model and do not persist.
- All discovered value-log segments remain open; a bounded descriptor/cache policy is not implemented.
- Durability tracks dirty value-log segment IDs and syncs only touched segments, but ownership is DB-wide rather than attached to individual group-commit epochs.
- Stats scan all live B+ tree records and may read main-log pointer entries in key-only mode; incremental accounting is not implemented.
- The manifest derives recovery from authoritative live pointers but does not list replacement segments or retain a full multi-batch progress journal.
- Manifest rename/directory-sync faults are not yet independently injected or tested with subprocess termination.
- GC rewrites under a stop-the-world DB lock and may create foreground latency spikes.
- Failed replacement writes can leave unreachable bytes in replacement segments; they remain safe but require later GC.
- The cold tier is a policy placeholder, not a storage implementation.
- The initial thresholds are hypotheses and need end-to-end workload validation.

## Segmented value-log mechanism

`ValueLogSegmentSize` now activates backward-compatible rotation. The existing `values.vlog` remains file ID 0; new files use monotonically increasing names such as `values-00000000000000000001.vlog`. Reopen discovers all segments, restores the largest active ID and exact append offsets, and resolves pointers by `FileID` with offset/size/CRC bounds checks.

Tests cover legacy reopen after enabling segmentation, multiple rotations, nonzero pointer file IDs, reopen reads from every segment, missing-segment failure, corrupt/truncated reads, merge compatibility, and ordered durability under every/group/adaptive policies.

```bash
go test -count=1 ./engine -run 'Test(KVSeparation|ValuePointer)'
go test ./engine -run '^$' -bench '^BenchmarkSegmentedValueLogAppendRead$' -benchmem -count=5
```

## Core segment statistics and GC selection

`DB.ValueLogStats()` returns immutable, file-ID-sorted snapshots for every discovered segment:

- total bytes from the actual segment append offset
- live bytes referenced by current, non-deleted, non-expired B+ tree pointer records
- stale bytes as total minus live
- active-segment flag

The implementation supports both RAM key/value and RAM key-only indexes. In key-only mode it reads pointer entries from the main data log before decoding the value pointer. `DB.PickValueLogGCCandidate` selects the non-active segment with the highest stale ratio above a caller threshold, using lower file ID as a deterministic tie-breaker. Selection is read-only: it does not copy or delete bytes.

```bash
go test -count=1 ./engine -run 'Test(ValueLogStats|ValueLogGCCandidate)'
go test ./engine -run '^$' -bench '^BenchmarkValueLogStatsCore$' -benchmem -count=5
```

Tests cover overwrite-generated stale bytes, multiple segments, both supported RAM index modes, reopen reconstruction, deterministic selection, and exclusion of the active segment.

## Guarded core lifecycle placement

`KVSeparationOptions.LifecycleEnable` enables advisory lifecycle placement for future writes only. It requires `KVSeparationOptions.Enable`; opening an impossible lifecycle-without-value-log configuration fails. Static threshold placement remains the default.

The core tracks in-memory per-key reads, updates, and logical observation windows. With insufficient observations, placement falls back to the configured size threshold. Once history is sufficient:

- small values remain inline
- hot-read large values remain inline
- frequently updated large values remain inline to reduce value-log garbage
- stable large values remain separated

`DB.KVPlacementMetrics()` exposes immutable counters for inline decisions, value-log decisions, and threshold fallbacks. Lifecycle history deliberately resets on reopen; persisted data remains readable because placement correctness depends only on the entry/pointer representation, never on advisory history.

```bash
go test -count=1 ./engine -run '^TestLifecyclePlacement'
go test ./engine -run '^$' -bench '^BenchmarkLifecyclePlacementPut$' -benchmem -count=5
```

Cold age/value options are reserved for a future physical cold tier and do not currently move values outside inline/value-log storage.

## Stop-the-world value-log GC

`DB.ValueLogGC(minStaleRatio)` now reclaims one selected non-active segment while holding the DB write lock for the rewrite phase:

1. choose the deterministic non-active candidate
2. snapshot current live pointers naming the source
3. read and CRC-validate each live value
4. append values to current replacement segments
5. write replacement pointer entries through an internal transaction path that preserves pointer payloads
6. commit using the configured durability policy
7. run an explicit ordered value-log-before-main-log durability barrier
8. re-enumerate live pointers and require zero references to the source
9. close and remove the source segment
10. fsync the DB directory

The returned `ValueLogGCResult` reports source/live/stale bytes, copied value count, and reclaimed source bytes. No-candidate calls return `ErrValueLogGCNoCandidate` without changing segments.

Tests cover mixed live/stale source segments, both RAM index modes, immediate reads, close/reopen, physical source removal, repeated no-candidate GC, and no-candidate state preservation. Merge continues to preserve replacement pointer payloads.

```bash
go test -count=1 ./engine -run '^TestValueLogGC'
go test ./engine -run '^$' -bench '^BenchmarkValueLogGC$' -benchmem -count=5
```

Subprocess recovery benchmarks exercise every semantic GC crash phase and include child startup, abrupt exit, parent reopen, source-retention/removal oracle checks, and manifest cleanup:

```bash
go test ./engine -run '^$' -bench '^BenchmarkValueLogGCSubprocessRecovery$' -benchmem -count=5
```

## GC manifest recovery

Value-log GC writes a versioned, CRC-protected `value-gc.manifest` using temp-file sync, atomic rename, and directory sync. New version 2 records the source plus the inclusive first/last replacement segment IDs and four phases:

- `PREPARED`: source selected; the record is rewritten with the final replacement range before pointer commit
- `POINTERS_INSTALLED`: replacement values and pointer records completed the explicit value-log-before-main-log durability barrier
- `SOURCE_REMOVED`: source unlink and directory sync completed
- `FINALIZED`: terminal bookkeeping completed; only manifest cleanup remains

Version 1 two-phase records remain readable. Open-time recovery rebuilds the authoritative B+ tree first and remains pointer-authoritative: a referenced source is retained, while an unreferenced source may be removed only after every declared version 2 replacement segment exists. Replacement ranges are validation and trace metadata, not ownership lists, so recovery never deletes unreachable replacement bytes as rollback. `SOURCE_REMOVED` and `FINALIZED` recovery are idempotent.

Semantic points `gc-prepared`, `gc-pointers-installed`, `gc-before-source-remove`, `gc-source-removed`, and `gc-finalized` expose each core state boundary. Tests cover version 1 compatibility, all version 2 phases, CRC-invalid manifests, missing replacement rejection, source retention/removal, interruption after pointer installation, immediate reopen, and manifest cleanup. Real subprocess termination covers every GC semantic phase (including two consecutive recovery opens); the subprocess benchmark matrix measures all five phases with the same recovery oracle.
