# Benchmark Methodology

## Research question

How do individual write-path costs compose into end-to-end Bitcask write latency?

## Benchmark groups

### End-to-end transaction benchmarks

```bash
go test ./engine -run '^$' -bench 'BenchmarkTxCommit(SingleEntry|Batch)Baseline|BenchmarkSmallValueFixedCostBaseline' -benchmem -count=3
```

These benchmarks measure the public transaction path, including transaction setup, pending entry construction, encoding, append, optional sync, and index update. `BenchmarkTxPutBatch` measures the first-class `DB.PutBatch` API and should be compared with repeated one-entry transactions.

### Write-path microbenchmarks

```bash
go test ./engine -run '^$' -bench 'BenchmarkWritePath' -benchmem -count=3
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

### Autonomous phase transitions

```bash
go test ./engine -run '^$' -bench '^BenchmarkAutonomousPhaseTransitions$' -benchmem -count=5
```

Compare identical write/read/overwrite/large-value phases with autonomous observation/application disabled and enabled. Report commit p50/p99 bucket bounds, audit events, allocations, final phase/policy, and correctness tests. The current harness applies only the final recommendation, so it measures observation/detection overhead rather than continuous closed-loop gains.

### Compaction amplification and latency

```bash
go test ./experiments/compaction -run '^$' -bench '^BenchmarkAnalyzeCompactionWorkload$' -benchmem -count=5
go test ./engine -run '^$' -bench '^BenchmarkCompactionForegroundImpact$' -benchmem -count=5
```

Use deterministic overwrite-hotset, cold-garbage, and mixed streams. Report logical/live/obsolete/rewrite bytes, write/space amplification, foreground p50/p99, merge mode, and logical byte budget. Do not compare p99 without identical segment size, preload, sync policy, and filesystem.

## Reproducibility harness

The curated per-track suites are wired into the repo `Makefile`, and the raw output
captured on the maintainer's machine is committed under [`../../results/`](../../results/)
as the evidence behind the tables below:

```bash
make bench            # all six tracks, COUNT=5, into results/track-*.txt
make bench-a          # a single track (a…f)
make bench COUNT=10   # tighter estimates
```

Environment and portability caveats are in [`../../results/README.md`](../../results/README.md).

## Before / after comparison

For a *candidate change*, capture two runs **outside** the repository and diff them —
these ad-hoc comparison runs are not committed:

```bash
go test ./engine -run '^$' -bench 'BenchmarkTxCommit|BenchmarkSmallValue|BenchmarkWritePath' -benchmem -count=8 > /tmp/bitcask-before.txt
# ...make a change...
go test ./engine -run '^$' -bench 'BenchmarkTxCommit|BenchmarkSmallValue|BenchmarkWritePath' -benchmem -count=8 > /tmp/bitcask-after.txt
benchstat /tmp/bitcask-before.txt /tmp/bitcask-after.txt
```

Commit policy: keep the curated **measured tables** here plus the **representative raw
runs** under `results/`; keep transient `benchstat` A/B runs external.

## Interpreting small values

For values below roughly one cache line, throughput is usually dominated by fixed per-entry cost: transaction setup, entry/meta allocation, key formatting, CRC, append syscall, and index update. MB/s is therefore less informative than ns/op and allocs/op.

## Interpreting sync benchmarks

Sync-enabled benchmarks measure storage-device and filesystem behavior as much as Go code. Always compare:

- sync disabled
- every transaction sync
- batch transaction sync
- transaction-level sync policy
- future true group/adaptive policies

## Measured results

Captured on **Apple M3 Max, darwin/arm64, Go 1.26.2, APFS SSD**, `-benchmem -count=5`
(median of 5). Raw output: [`../../results/`](../../results/). `allocs/op` and `B/op` are
the portable signal; sync-bearing `ns/op` is device-dependent (see caveats above).

Each row names the backing benchmark so the number can be regenerated directly.

| Experiment | Benchmark | ns/op | B/op | allocs/op | Notes |
| --- | --- | ---: | ---: | ---: | --- |
| Tx single 32B, sync=false | `TxCommitSingleEntryBaseline/32B/sync=false` | 39,134 | 865 | 15 | fixed-cost dominated |
| Tx single 32B, sync=true | `TxCommitSingleEntryBaseline/32B/sync=true` | 4,407,716 | 836 | 15 | one fsync ≈ 4.4 ms |
| Tx single 1KB, sync=false | `TxCommitSingleEntryBaseline/1024B/sync=false` | 38,947 | 2,914 | 15 | payload copy in B/op |
| Small value fixed cost 0B | `SmallValueFixedCostBaseline/0B` | 38,950 | 802 | 14 | pure per-entry floor |
| Batch put, 64×32B | `TxPutBatch/batch=64/32B` | 102,400 | 37,880 | 640 | ≈ 1,600 ns/entry (~24× amortization) |
| Encode 1KB, fresh buffer | `WritePathEncode/1024B/alloc` | 291.8 | 1,152 | 1 | one alloc per encode |
| Encode 1KB, reused buffer | `WritePathEncode/1024B/reuse` | 138.7 | 0 | 0 | reuse: 1→0 allocs/op |
| Data-file append 1KB | `WritePathDataFileAppend/1024B` | 1,287 | 0 | 0 | append syscall cost |
| B+ tree insert | `WritePathBPTreeInsert` | ~620 | 309 | 6 | index insertion cost |

Track B–F numbers are filled from `results/track-{b..f}-*.txt` and mirrored in each
track chapter's "Measured results" section.

Fuller per-track tables (durability policies, compaction, KV separation, autonomous) live
in each track chapter under [`../tracks/`](../tracks/); their numbers come from the same
`results/track-*.txt` captures.
