# Benchmark results

Raw `go test -bench` output for the headline numbers in
[`../README.md`](../README.md) and [`../docs/paper.md`](../docs/paper.md).

Captured on the maintainer’s machine. Regenerate with `make bench` (or `make bench-a` … `f`).

## Environment

| Field | Value |
| --- | --- |
| Machine | Apple M3 Max |
| GOARCH / GOOS | arm64 / darwin |
| CPU threads (`-14` suffix) | 14 |
| Go toolchain | go1.26.2 |
| Filesystem | APFS (SSD) |
| Capture command | `make bench COUNT=5` |

> **Portability caveat.** Absolute `ns/op` numbers — especially for sync/fsync,
> subprocess-recovery, and value-log GC benchmarks — are dominated by the storage
> device and OS and will differ on other hardware. Treat these as a **baseline on one
> machine**, not universal constants. `allocs/op` and `B/op` are far more portable and
> are the primary signal for the allocation-focused Track A work. Compare runs with
> `benchstat`, and only across identical segment size, sync policy, preload, and
> filesystem.

## Files

| File | Track | Suite |
| --- | --- | --- |
| `track-a-write-path.txt` | A | write-path allocation matrix, encode/append/index micro, batch staging |
| `track-b-durability.txt` | B | concurrent commit policies, sync policy, adaptive, explicit flush |
| `track-c-crash.txt` | C | partial-transaction recovery, torn markers, subprocess & group-epoch recovery, pure scenarios |
| `track-d-compaction.txt` | D | foreground merge impact, merge policy, recommendation, workload analysis |
| `track-e-kv-separation.txt` | E | KV separation put/get, segmented IO, value-log GC, stats, lifecycle placement |
| `track-f-autonomous.txt` | F | static-vs-autonomous phase transitions, observation/apply cost, detector |
| `headline-smoke.txt` | — | short re-run of headline benches (`-count=3 -benchtime=500ms`); qualitative check only — **do not** replace `COUNT=5` tables with this file |

## Reproduce

```bash
make bench            # all tracks, COUNT=5, into results/track-*.txt
make bench-a          # a single track
make bench COUNT=10   # tighter estimates (slower)
```

For before/after comparisons, capture two runs **outside** the repo and diff them:

```bash
go test ./engine -run '^$' -bench 'BenchmarkTxCommit|BenchmarkWritePath' -benchmem -count=8 > /tmp/before.txt
# ...make a change...
go test ./engine -run '^$' -bench 'BenchmarkTxCommit|BenchmarkWritePath' -benchmem -count=8 > /tmp/after.txt
benchstat /tmp/before.txt /tmp/after.txt
```
