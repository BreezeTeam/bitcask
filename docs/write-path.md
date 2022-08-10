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

`DB.PutBatch` and `Tx.PutBatch` are first-class batch APIs. They reuse the same pending-write transaction semantics as repeated `Tx.Put` calls, so the final entry in the batch remains the committed marker for the whole transaction.

## Correctness invariant

A transaction becomes visible only after its final entry is written with `Committed` status and its transaction id is recorded in the in-memory committed set or sparse-index transaction-id index.

Recovery relies on that committed marker: uncommitted entries are ignored, and entries belonging to a committed transaction id can be made visible again after reopen.

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

Pending entry construction now flows through `newPendingEntry`, and sparse-index position tracking uses `entryIndexKey`. These helpers are intentionally small: their purpose is to make allocation sources measurable and provide stable insertion points for later experiments, not to hide the write path behind a new abstraction.

More aggressive pooling is intentionally deferred until object lifetime is proven safe. In `HintKeyValAndRAMIdxMode`, index records can retain pointers to entries, so returning entry objects to a pool too early would corrupt visible data.

## Baseline benchmark map

- `BenchmarkTxCommitSingleEntryBaseline`: end-to-end single-entry transaction cost.
- `BenchmarkTxCommitBatchBaseline`: transaction amortization over batch sizes.
- `BenchmarkTxPutBatch`: first-class batch API cost across batch and value sizes.
- `BenchmarkSmallValueFixedCostBaseline`: fixed-cost behavior for tiny values.
- `BenchmarkWritePathEncode`: encode allocation versus reusable buffer.
- `BenchmarkTxPendingEntryConstruction`: pending entry and metadata construction cost.
- `BenchmarkSmallValueFixedCostOptimized`: optimized small-value end-to-end write path after helper refactors.
- `BenchmarkWritePathDataFileAppend`: append-only data file write cost.
- `BenchmarkWritePathDataFileSync`: write plus fsync cost.
- `BenchmarkWritePathBPTreeInsert`: index insertion cost.

## Expected trade-offs

- Batch transactions reduce per-entry transaction overhead.
- Transaction-level sync preserves committed-marker recovery semantics while reducing fsync count inside a batch.
- `SyncPolicyGroupCommit` is currently an opt-in framework mapped to transaction-level sync; true cross-writer coalescing requires separating append, visibility, and durability wait under the write lock.
- `SyncPolicyAdaptive` tracks dirty bytes and elapsed time since the last sync to decide whether a transaction must sync immediately or can be delayed within configured bounds.
- `KVSeparationOptions` can redirect large values to `values.vlog`; the main log then stores a pointer entry and reads transparently resolve the logical value.
