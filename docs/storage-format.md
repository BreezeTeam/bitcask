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

It is stored as the Bitcask entry value and is protected by both the main-entry CRC and the value-log CRC over the referenced value bytes.

## Correctness invariant

Readers must not expose an entry unless its transaction id is known to be committed, its tombstone/TTL state is visible, and its CRC validates. For separated values, the pointer must decode and the value-log read must validate before returning the logical value.

## Benchmark design

Use inline versus separated value benchmarks to measure format-level trade-offs:

```bash
go test . -run '^$' -bench 'BenchmarkKVSeparation(Put|Get)' -benchmem -count=3
```

Use write-path encode benchmarks to measure entry-format encoding cost:

```bash
go test . -run '^$' -bench 'BenchmarkWritePathEncode' -benchmem -count=3
```

## Expected trade-off

The base entry format is simple and easy to recover. KV separation reduces main-log rewrite pressure for large values but adds pointer decode, value-log IO, and extra integrity checks on reads.

## Observed result template

| Format experiment | Value size | Inline result | Separated result | Correctness tests | Notes |
| --- | --- | --- | --- | --- | --- |
| TBD | TBD | TBD | TBD | TBD | TBD |
