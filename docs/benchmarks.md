# Benchmark Methodology

## Research question

How do individual write-path costs compose into end-to-end Bitcask write latency?

## Benchmark groups

### End-to-end transaction benchmarks

```bash
go test . -run '^$' -bench 'BenchmarkTxCommit(SingleEntry|Batch)Baseline|BenchmarkSmallValueFixedCostBaseline' -benchmem -count=3
```

These benchmarks measure the public transaction path, including transaction setup, pending entry construction, encoding, append, optional sync, and index update. `BenchmarkTxPutBatch` measures the first-class `DB.PutBatch` API and should be compared with repeated one-entry transactions.

### Write-path microbenchmarks

```bash
go test . -run '^$' -bench 'BenchmarkWritePath' -benchmem -count=3
```

These benchmarks isolate encoding, data-file append, sync, and B+ tree index insertion.

### Backend scenario benchmarks

```bash
go test ./example -run '^$' -bench 'BenchmarkBackend' -benchmem -count=3
```

These benchmarks use the example backend wrapper to compare realistic object writes, mixed payloads, batch writes, segment sizes, and sync settings.

### Experiment package benchmarks

```bash
go test ./experiments/... -run '^$' -bench Benchmark -benchmem -count=3
```

These benchmarks evaluate pure policy models such as workload generation, compaction selection, adaptive sync, value separation, and fault injection. Core fault-injection overhead is measured by `BenchmarkFaultInjectionDisabledWritePath` and `BenchmarkFaultInjectionEnabledNoFault`.

## Before / after comparison

Save benchmark output outside the repository:

```bash
go test . -run '^$' -bench 'BenchmarkTxCommit|BenchmarkSmallValue|BenchmarkWritePath' -benchmem -count=5 > /tmp/bitcask-before.txt
go test . -run '^$' -bench 'BenchmarkTxCommit|BenchmarkSmallValue|BenchmarkWritePath' -benchmem -count=5 > /tmp/bitcask-after.txt
benchstat /tmp/bitcask-before.txt /tmp/bitcask-after.txt
```

Do not commit raw benchmark output. Keep only methodology and representative conclusions in documentation.

## Interpreting small values

For values below roughly one cache line, throughput is usually dominated by fixed per-entry cost: transaction setup, entry/meta allocation, key formatting, CRC, append syscall, and index update. MB/s is therefore less informative than ns/op and allocs/op.

## Interpreting sync benchmarks

Sync-enabled benchmarks measure storage-device and filesystem behavior as much as Go code. Always compare:

- sync disabled
- every transaction sync
- batch transaction sync
- transaction-level sync policy
- future true group/adaptive policies

## Benchmark result template

| Experiment | Baseline | Optimized | Delta | Notes |
| --- | --- | --- | --- | --- |
| Tx single 32B, sync=false | TBD | TBD | TBD | fixed-cost dominated |
| Tx batch 64x1KB, sync=true | TBD | TBD | TBD | fsync amortization |
| Tx commit sync policy | TBD | TBD | TBD | none vs every-commit vs group framework |
| Tx commit adaptive sync | TBD | TBD | TBD | every-commit vs group framework vs adaptive |
| Metrics snapshot | TBD | TBD | TBD | observability read overhead |
| Tx commit with metrics | TBD | TBD | TBD | write-path counter overhead |
| DB merge policy | TBD | TBD | TBD | file-id vs garbage-ratio vs hot-cold |
| KV separation put/get | TBD | TBD | TBD | inline vs value-log pointer |
| Fault injection disabled/enabled | TBD | TBD | TBD | data-file RWManager wrapper overhead |
| Fault schedule replay | TBD | TBD | TBD | deterministic crash-point model |
| Pending entry construction | TBD | TBD | TBD | entry/meta allocation source |
| Encode 1KB reuse | TBD | TBD | TBD | allocation reduction |
