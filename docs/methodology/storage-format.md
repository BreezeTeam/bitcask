# Storage Format

## Research question

Which bytes must remain stable for recovery, compaction, KV separation, and sparse-index experiments to be comparable?

## Hypothesis

A small fixed entry header plus append-only segment files is sufficient for many modern KV-storage experiments if new mechanisms are encoded as opt-in metadata states instead of changing the base layout.

## Mechanism

The main data log is a sequence of entries stored in files named `<fileID>.data`. Each entry is encoded as:

```text
| crc | timestamp | keySize | valueSize | flag | ttl | bucketSize | status | ds | txID | bucket | key | value |
```

The fixed header size is `DataEntryHeaderSize`. The variable section stores bucket, key, and value bytes. The CRC covers the header after the CRC field plus the bucket, key, and value payload.

`MetaData.Status` distinguishes `UnCommitted` from `Committed`. The final entry in a transaction is marked `Committed`; earlier entries share the same `TxID` and become visible only when recovery has observed that committed marker.

`MetaData.Ds` identifies how the value should be interpreted:

- `DataStructureBPTree`: normal indexed key/value entry.
- `DataStructureNone`: structural marker such as bucket deletion.
- `DataStructureValuePointer`: value bytes are a pointer payload into `values.vlog`.

The value-log pointer payload is:

```text
| magic | version | fileID | offset | size | crc |
```

It is stored as the Bitcask entry value and is protected by both the main-entry CRC and the value-log CRC over the referenced value bytes. Pointer version 1 already includes `fileID`: legacy `values.vlog` is file ID 0, while new rotated segments are named `values-<20-digit-id>.vlog`. This activates segmentation without changing pointer encoding, so existing databases remain readable. Under durable sync policies, all open value-log segments are synchronized before the main data log containing the pointer and committed marker, so a successful durable return cannot acknowledge a pointer before its referenced bytes.

## Correctness invariant

Readers must not expose an entry unless its transaction id is known to be committed, its tombstone/TTL state is visible, and its CRC validates. For separated values, the pointer must decode and the value-log read must validate before returning the logical value. Byte-truncated headers, bucket/key/value payloads, and committed markers are exhaustively tested: recovery may fail open or ignore an incomplete tail, but no prefix lacking the full valid committed marker can expose the transaction.

Merge recovery uses `merge.manifest`, a versioned CRC record containing phase, source file ID, and inclusive replacement file-ID range. `PREPARED` retains the source; `INSTALLED` permits source deletion only when the complete target range exists. Recovery runs before normal data-file discovery and index rebuild.

Core value-log statistics derive liveness from current B+ tree pointer records rather than scanning historical pointer entries. A segment's stale bytes are its actual appended bytes minus the sizes of live pointers naming that file. The active segment is never selected as a GC source. Value-log GC copies live values, commits new version-1 pointers, revalidates that no current pointer names the source, removes the source, and fsyncs the database directory. Version 2 of the CRC-protected `value-gc.manifest` records source ID, an inclusive replacement segment range, and `PREPARED`, `POINTERS_INSTALLED`, `SOURCE_REMOVED`, or `FINALIZED`; version 1 two-phase records remain readable. `POINTERS_INSTALLED` is published only after the ordered value-log-before-main-log barrier. Open-time recovery uses rebuilt live pointers as authority, validates declared replacement segments before source deletion, and completes cleanup idempotently.

## Benchmark design

Use inline versus separated value benchmarks to measure format-level trade-offs:

```bash
go test ./engine -run '^$' -bench 'BenchmarkKVSeparation(Put|Get)' -benchmem -count=3
```

Use write-path encode benchmarks to measure entry-format encoding cost:

```bash
go test ./engine -run '^$' -bench 'BenchmarkWritePathEncode' -benchmem -count=3
```

## Expected trade-off

The base entry format is simple and easy to recover. KV separation reduces main-log rewrite pressure for large values but adds pointer decode, value-log IO, and extra integrity checks on reads.

## Observed results

Inline vs separated puts (median of `-count=5`, Apple M3 Max). Full matrix: [lifecycle-kv-separation](../tracks/lifecycle-kv-separation.md); raw: [`../../results/track-e-kv-separation.txt`](../../results/track-e-kv-separation.txt).

| Format experiment | Value size | Inline result | Separated result | Correctness tests | Notes |
| --- | ---: | ---: | ---: | --- | --- |
| `KVSeparationPut` | 512B | 56,493 ns/op · 1,784 B/op | 166,562 ns/op · 1,784 B/op | `TestKVSeparation*` / `TestValuePointer*` | below threshold: separation adds pointer path cost |
| `KVSeparationPut` | 4,096B | 721,550 ns/op · 9,666 B/op | 590,495 ns/op · 4,931 B/op | same | crossover: separation wins on put + B/op |
| `KVSeparationPut` | 65,536B | 1,882,589 ns/op · 139,972 B/op | 913,702 ns/op · 66,408 B/op | same | ~2× put cut; get stays ~39 µs either mode |
| `KVSeparationGet` | (resolved read) | 38,710 ns/op · 128 B/op · 4 allocs | 39,078 ns/op · 4,384 B/op · 7 allocs | same | pointer decode overhead is small vs inline |
