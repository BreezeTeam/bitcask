# Bitcask Write Path

## Research question

Where does write latency come from in the current Bitcask implementation, and which costs are fixed per entry versus proportional to value size?

## Current mechanism

The main write path is:

```text
DB.Update / DB.PutBatch
  -> DB.Begin(writable=true)
  -> Tx.Put / Tx.PutBatch / Tx.PutWithTimestamp
  -> Tx.Commit
  -> commitEntries
  -> appendEntry
  -> Entry.EncodeTo
  -> DataFile.WriteAt
  -> updateIndexesForEntry
  -> syncAfterTransaction
  -> transaction unlock
```

`DB.PutBatch` and `Tx.PutBatch` are first-class batch APIs. `Tx.PutBatch` validates the complete bucket/key set before mutating pending writes, then allocates one exact-capacity pending slice and constructs owned entries. Batch key/value bytes are copied into one transaction-owned arena while bucket bytes are shared as one immutable slice, reducing per-entry staging allocations without changing retained-object lifetime. Invalid middle/last items cannot leave a partial batch staged, and pre-existing pending writes remain intact. The final entry in the combined transaction remains the committed marker for the whole transaction.

## Correctness invariant

A transaction becomes visible only after its final entry is written with `Committed` status and its transaction id is recorded in the in-memory committed set or sparse-index transaction-id index.

Recovery relies on that committed marker: uncommitted entries are ignored, and entries belonging to a committed transaction id can be made visible again after reopen.

Every `Commit` attempt is terminal. Whether append, index publication, or sync succeeds or fails, the transaction releases its database lock exactly once and cannot be reused. A failure before the final marker leaves the transaction uncommitted. A sync failure after the marker does not roll back bytes or in-memory visibility; it means durability is unknown and the original sync error is returned. This distinction is required before cross-writer durability waiters can be introduced safely.

## Current fixed costs

Small-value writes pay several costs that do not scale with value size:

- transaction creation and lock acquisition
- key and bucket validation
- `Entry` and `MetaData` construction
- timestamp and transaction id assignment
- CRC calculation over the encoded entry
- append syscall
- optional fsync
- B+ tree insertion

This is why 0B, 8B, 16B, and 32B values should be benchmarked separately from 1KB and 16KB values.

## Allocation model

`Entry.EncodeTo` allows the commit loop to reuse the encode buffer. This removes encode-buffer allocation from the hot loop, but end-to-end writes can still allocate for pending entries, metadata, formatted benchmark keys, index hints, and B+ tree nodes.

Pending entry construction now flows through `newPendingEntry`, and sparse-index position tracking uses `entryIndexKey`. Batched writes use a transaction-owned key/value arena plus immutable shared bucket bytes, so staging has one payload allocation for the batch rather than separate key/value allocations for each entry. These helpers are intentionally small: their purpose is to make allocation sources measurable and provide stable insertion points for later experiments, not to hide the write path behind a new abstraction.

More aggressive pooling is intentionally deferred until object lifetime is proven safe. In `HintKeyValAndRAMIdxMode`, index records can retain pointers to entries, so returning entry objects to a pool too early would corrupt visible data.

## Object lifetime and ownership map

| Object/bytes | Owner and lifetime | Retained after commit? | Reuse rule |
| --- | --- | --- | --- |
| caller key/value slices | caller; may be mutated after `Put` returns | no | `newPendingEntry` copies both slices before retaining them |
| bucket bytes | pending entry/batch | potentially through `MetaData` in index hints | single writes own one allocation; batch entries share one immutable transaction-owned bucket slice |
| `Tx.pendingWrites` | transaction until terminal `Commit`/`Rollback` | entries may escape into RAM indexes | slice is cleared when the transaction closes; entries are not pooled |
| `Entry` / `MetaData` | transaction, then index in key/value RAM mode | yes in `HintKeyValAndRAMIdxMode` | never mutate or pool after publication |
| `Hint` / `Record` | B+ tree | yes | immutable identity/location metadata until replaced by a newer record |
| encode buffer | commit loop | no; `WriteAt` consumes bytes before reuse | safe to reuse only within the serialized append loop |
| composite position-map key | position map | yes for active sparse-index construction | length-prefixed bucket plus key prevents boundary collisions |
| resolved value-pointer result | read transaction/caller | no index retention | returned as a copied `Entry`/`MetaData` with freshly read value bytes |

The write API now owns pending key/value bytes, so caller mutation after `Put` cannot alter the committed key, indexed value, or encoded log record. This adds proportional copy cost and is measured explicitly rather than traded for aliasing risk. `PutBatch` additionally allocates bucket bytes once and shares that immutable slice across all entries in the batch; entries may retain it in RAM index metadata, so it is never mutated or pooled.

`entryIndexKey` uses a 32-bit bucket-length prefix followed by bucket and key bytes. This distinguishes pairs such as `(ab,c)` and `(a,bc)`, which raw concatenation cannot distinguish. The sparse B+ tree's persisted ordering key still uses its existing layout; changing that format requires a separate compatibility migration.

## Baseline benchmark map

- `BenchmarkTxCommitLifecycle`: transaction begin/terminal-cleanup cost with zero or one write.
- `BenchmarkTxCommitSingleEntryBaseline`: end-to-end single-entry transaction cost.
- `BenchmarkTxCommitBatchBaseline`: transaction amortization over batch sizes.
- `BenchmarkTxPutBatchStaging`: validation, exact-capacity pending allocation, ownership copies, and entry construction by batch size.
- `BenchmarkTxPutBatchArenaStaging`: transaction-owned batch key/value arena cost across batch and value sizes.
- `BenchmarkTxPutBatch`: first-class batch API cost across batch and value sizes.
- `BenchmarkSmallValueFixedCostBaseline`: fixed-cost behavior for tiny values.
- `BenchmarkWritePathEncode`: encode allocation versus reusable buffer.
- `BenchmarkTxPendingEntryConstruction`: pending entry ownership copies and metadata construction cost.
- `BenchmarkEntryIndexKey`: collision-free composite position-map key construction.
- `BenchmarkWriteAllocationMatrix`: exact 0B/8B/32B/1KB/16KB/64KB values across batches 1/8/64/256.
- `BenchmarkSmallValueFixedCostOptimized`: optimized small-value end-to-end write path after helper refactors.
- `BenchmarkWritePathDataFileAppend`: append-only data file write cost.
- `BenchmarkWritePathDataFileSync`: write plus fsync cost.
- `BenchmarkWritePathBPTreeInsert`: index insertion cost.

## Expected trade-offs

- Batch transactions reduce per-entry transaction overhead.
- Transaction-level sync preserves committed-marker recovery semantics while reducing fsync count inside a batch.
- `SyncPolicyGroupCommit` now coalesces concurrent writers: append/index publication remains serialized, then writers release the DB lock and share an epoch sync before returning.
- `SyncPolicyAdaptive` tracks dirty bytes and elapsed time since the last sync to decide whether a transaction must sync immediately or can be delayed within configured bounds.
- `KVSeparationOptions` can redirect large values to `values.vlog`; the main log then stores a pointer entry and reads transparently resolve the logical value.

## Tests

Ownership and batch staging correctness exercises the shipped write path:

```bash
go test -count=1 ./engine -run 'Test(PendingWriteOwnsCallerBuffers|EntryIndexKeySeparatesBucketAndKey|TxPutBatch|EntryEncodeToMatchesEncode|DB_PutBatch)'
```

## Benchmarks (reproducible)

```bash
go test ./engine -run '^$' -bench 'Benchmark(WriteAllocationMatrix|TxPutBatchArenaStaging|TxPutBatchStaging|WritePathEncode|TxPendingEntryConstruction|EntryIndexKey|WritePathDataFileAppend|WritePathDataFileSync|WritePathBPTreeInsert|SmallValueFixedCost)' -benchmem -count=5
```

Use `benchstat` on two captured runs after a candidate allocation change. Short single-run samples are noise, not proof of improvement.

### Measured results

Median of `-count=5` on Apple M3 Max, darwin/arm64, Go 1.26.2, APFS SSD (`-benchmem`).
sync=true `ns/op` is device-dominated. Raw: [`../../results/track-a-write-path.txt`](../../results/track-a-write-path.txt).

| Benchmark | ns/op | B/op | allocs/op | Reading |
| --- | ---: | ---: | ---: | --- |
| `TxCommitSingleEntryBaseline/32B/sync=false` | 39,134 | 865 | 15 | fixed-cost floor for a tiny value |
| `TxCommitSingleEntryBaseline/32B/sync=true` | 4,407,716 | 836 | 15 | one fsync ≈ 4.4 ms dominates end-to-end |
| `TxCommitSingleEntryBaseline/1024B/sync=false` | 38,947 | 2,914 | 15 | payload copy shows in B/op, not ns/op |
| `SmallValueFixedCostBaseline/0B` | 38,950 | 802 | 14 | pure per-entry floor (no value bytes) |
| `TxPutBatch/batch=1/32B` | 38,977 | 922 | 15 | one entry per transaction |
| `TxPutBatch/batch=64/32B` | 102,400 | 37,880 | 640 | ≈ 1,600 ns/entry — batching amortizes the ~39 µs transaction cost ~24× |
| `WritePathEncode/1024B/alloc` | 291.8 | 1,152 | 1 | encode into a fresh buffer |
| `WritePathEncode/1024B/reuse` | 138.7 | 0 | 0 | **reused buffer: 1→0 allocs/op, ~2× faster** |
| `WritePathDataFileAppend/1024B` | 1,287 | 0 | 0 | append syscall, no allocation |
| `WritePathBPTreeInsert` | ~620 | 309 | 6 | index insertion cost |

The portable signal is `allocs/op`/`B/op`: encode-buffer reuse removes the per-entry
encode allocation, and batching collapses per-entry transaction overhead. The `ns/op`
gap between `sync=false` (~39 µs) and `sync=true` (~4.4 ms) is the motivation for Track B.

## Fixed cost versus proportional cost

| Cost class | Examples | Scales with value size? |
| --- | --- | --- |
| Fixed per entry / transaction | begin/commit lock path, metadata construction, CRC of fixed header fields, B+ tree insert of a key, optional fsync | No (or weakly) |
| Proportional to payload | key/value ownership copies, encode buffer fill, data-file append bytes, value-log append when separated | Yes |
| Amortized by batching | one transaction lock, one commit marker, one sync barrier for many entries, one arena allocation for batch payload | Partially |

Tiny values (0B–32B) are dominated by fixed costs; 1KB–64KB values shift the profile toward copy and syscall byte volume. The allocation matrix and encode/append microbenchmarks separate those regimes.

## Limitations

- Encode-buffer reuse is confined to the serialized commit loop; index modes that retain entry pointers forbid pooling `Entry`/`MetaData` after publication.
- Arena staging reduces per-entry key/value allocations inside a batch, but each entry still allocates an `Entry`/`MetaData` pair that may escape into RAM indexes.
- Benchmarks measure process-local cost on a single machine; they are not calibrated multi-tenant or multi-disk profiles.
- Sparse-index on-disk key layout is unchanged; only the in-memory position-map key uses the length-prefixed collision-free form.
- Write-path optimizations do not change durability policy; sync cost still dominates end-to-end latency when every commit fsyncs.
