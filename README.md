# Bitcask Research KV Engine

A Go Bitcask-style log-structured key/value store, packaged as a **research study**:
six experimental tracks, each with a research question, mechanism, correctness
invariant, tests, and measured results.

**中文文档：** [`README.zh.md`](README.zh.md) · [`docs/paper.zh.md`](docs/paper.zh.md)

**Start with the paper:** [`docs/paper.md`](docs/paper.md)

| Document | Contents |
| --- | --- |
| [docs/paper.md](docs/paper.md) | Abstract, questions, headline results, conclusions |
| [docs/tracks/](docs/tracks/) | Per-track chapters (mechanism, tests, tables, limits) |
| [docs/methodology/](docs/methodology/) | How measurements and formats are interpreted |
| [docs/ledger/research-tracks-2026.md](docs/ledger/research-tracks-2026.md) | Six-track status matrix and cross-track invariants |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Source-tree map |
| [results/](results/) | Raw `go test -bench` output behind the tables |

**Status: v1.0.0 (complete).**

> Not a production multi-tenant database. Experimental durability, compaction,
> value-log, and autonomy features are **opt-in**. Guarantees stop where the tests stop.

---

## Motivation

Bitcask keeps an append-only data log and an in-memory index to each key’s latest live
record. That simplicity is useful for storage research: one mechanism can be added at a
time—group commit, a crash oracle, budgeted merge, a value log—and its cost observed
without an LSM’s compaction noise dominating the signal.

This repository asks how far that substrate can be pushed in Go while remaining honest
about semantics. Each track implements a mechanism behind options, states an invariant,
tests it (including real process crashes where relevant), measures it, and records what
was *not* claimed.

---

## Research questions and findings

Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem -count=5`. Absolute
`ns/op` for sync and subprocess rows is device-dependent; prefer `allocs/op` / `B/op`
across machines. Raw output: [`results/`](results/).

| RQ | Question | Finding |
| --- | --- | --- |
| 1 | Where does write latency come from? | ≈ **39 µs** without sync vs ≈ **4.4 ms** with every-commit sync (fsync-bound) |
| 2 | How much does group commit help? | At 16 writers, ≈ **14×** vs every-commit (636 µs vs 8.87 ms; syncs/commit 1.0 → 0.06) |
| 3 | Can crash outcomes be checked, not asserted? | Real `os.Exit` + reopen + oracle; group-epoch recovery ≈ **37 ms/op** |
| 4 | Can merge respect a latency budget? | Budgeted merge ≈ **44 µs** (near no-merge); static merge ≈ **1.74 ms** + ~69 KB rewrite |
| 5 | When does value separation pay off? | 64 KB put ≈ **2×** faster when separated (1.88 ms → 914 µs); get stays ≈ 39 µs |
| 6 | What does guarded autonomy cost? | Observation ≈ **+13 µs**/commit; apply ≈ **33 ns**; sync policy is never weakened |

Full argument: [`docs/paper.md`](docs/paper.md).

---

## The six tracks

| Track | Topic | Chapter |
| --- | --- | --- |
| A | Write path & allocation | [write-path](docs/tracks/write-path.md) |
| B | Durability pipeline | [durability-pipeline](docs/tracks/durability-pipeline.md) |
| C | Crash consistency explorer | [crash-explorer](docs/tracks/crash-explorer.md) |
| D | SLO-aware compaction | [slo-compaction](docs/tracks/slo-compaction.md) |
| E | Lifecycle KV separation | [lifecycle-kv-separation](docs/tracks/lifecycle-kv-separation.md) |
| F | Autonomous storage | [autonomous-storage](docs/tracks/autonomous-storage.md) |

Pure policy models live under [`experiments/`](experiments/) and do not import the engine.

---

## Reading order

1. [`docs/paper.md`](docs/paper.md) — abstract, questions, results, conclusions  
2. A chapter under [`docs/tracks/`](docs/tracks/) for the mechanism of interest  
3. [`ARCHITECTURE.md`](ARCHITECTURE.md) — subsystem ↔ file map  
4. [`results/`](results/) — benchmark evidence  

Historical planning (closed at v1.0):
[`docs/ledger/research-roadmap.md`](docs/ledger/research-roadmap.md).

---

## Reproducibility

```bash
make check      # gofmt + go vet + go test + go test -race + git diff --check
make bench      # all six suites → results/track-*.txt  (COUNT=5)
make bench-a    # a single track (a…f)
```

Notes: [`results/README.md`](results/README.md) ·
[`docs/methodology/benchmarks.md`](docs/methodology/benchmarks.md).

---

## Repository layout

```text
github.com/BreezeTeam/bitcask/
├── engine/           package bitcask — public engine API + tests
├── internal/         rwmanager, helper, id
├── experiments/      pure models (compaction, fault, autonomous, …)
├── docs/             paper, tracks, methodology, ledger
├── results/          committed raw bench output
├── example/          scenario workloads
├── Makefile
└── README.md
```

```go
import "github.com/BreezeTeam/bitcask/engine"

db, err := bitcask.Open(bitcask.DefaultOptions)
```

---

## Limitations (summary)

- Single-machine synthetic microbenchmarks; not a multi-tenant calibration study  
- Merge and value-log GC are staged/triggered, not continuous background workers in v1.0  
- Crash coverage is semantic faults plus abrupt process exit—not full power-loss simulation  
- Autonomous apply is observation-first and off by default; closed-loop QoS gains are not claimed  

See each track’s *Limitations* and the paper’s *Threats to validity*.

---

## References

- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [rosedb](https://github.com/flower-corp/rosedb) · [nutsdb](https://github.com/xujiajun/nutsdb)
- [从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
- [优雅的 Bitcask/BeansDB](https://zhuanlan.zhihu.com/p/53682577)
