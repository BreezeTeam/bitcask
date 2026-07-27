# Bitcask Research KV Engine

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.16+-00ADD8?logo=go&logoColor=white" alt="Go"/>
  <img src="https://img.shields.io/badge/platform-darwin%2Flinux-lightgrey" alt="Platform"/>
  <img src="https://img.shields.io/badge/bench-Apple_M3_Max-black" alt="Bench"/>
  <img src="https://img.shields.io/badge/status-v1.0.0_complete-brightgreen" alt="Status"/>
</p>

A Go implementation of the Bitcask log-structured key/value store, built as a **six-track research study**. Each track poses a question, implements a mechanism behind options, states a correctness invariant, tests it (including real process crashes), measures it, and records what was *not* claimed.

**Start here:** [`docs/paper.md`](docs/paper.md) · **中文:** [`README.zh.md`](README.zh.md) / [`docs/paper.zh.md`](docs/paper.zh.md)

---

## Why Bitcask?

Bitcask keeps an append-only data log and an in-memory index mapping each key to its latest live record. That simplicity makes it an ideal substrate for storage research: one mechanism can be added at a time—group commit, a crash oracle, budgeted merge, a value log—and its cost observed without an LSM's compaction noise dominating the signal.

---

## Research Findings

Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem -count=5`.
Raw output: [`results/`](results/) · Methodology: [`docs/methodology/benchmarks.md`](docs/methodology/benchmarks.md)

| # | Question | Key Result |
|:-:|----------|------------|
| 1 | Where does write latency come from? | ≈ 39 µs without sync vs ≈ 4.4 ms with fsync (fsync-bound) |
| 2 | How much does group commit help? | ≈ **14×** at 16 writers (636 µs vs 8.87 ms) |
| 3 | Can crash outcomes be checked, not asserted? | Real `os.Exit` + reopen + oracle; recovery ≈ 37 ms/op |
| 4 | Can merge respect a latency budget? | Budgeted ≈ 44 µs vs static ≈ 1.74 ms |
| 5 | When does value separation pay off? | 64 KB put ≈ **2×** faster when separated |
| 6 | What does guarded autonomy cost? | Observation +13 µs/commit; sync policy never weakened |

---

## The Six Tracks

| Track | Topic | Chapter |
|:-----:|-------|---------|
| A | Write path & allocation | [write-path](docs/tracks/write-path.md) |
| B | Durability pipeline | [durability-pipeline](docs/tracks/durability-pipeline.md) |
| C | Crash consistency explorer | [crash-explorer](docs/tracks/crash-explorer.md) |
| D | SLO-aware compaction | [slo-compaction](docs/tracks/slo-compaction.md) |
| E | Lifecycle KV separation | [lifecycle-kv-separation](docs/tracks/lifecycle-kv-separation.md) |
| F | Autonomous storage | [autonomous-storage](docs/tracks/autonomous-storage.md) |

Pure policy models live under [`experiments/`](experiments/) and do not import the engine.

---

## Quick Start

```go
import "github.com/BreezeTeam/bitcask/engine"

db, err := bitcask.Open(bitcask.DefaultOptions)
defer db.Close()

db.Put([]byte("hello"), []byte("world"))
val, _ := db.Get([]byte("hello"))
```

## Reproducibility

```bash
make check      # gofmt + go vet + go test + go test -race + git diff --check
make bench      # all six suites → results/track-*.txt  (COUNT=5)
make bench-a    # single track (a…f)
```

---

## Repository Layout

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

## Documentation Index

| Document | Contents |
|----------|----------|
| [docs/paper.md](docs/paper.md) | Abstract, questions, headline results, conclusions |
| [docs/tracks/](docs/tracks/) | Per-track chapters (mechanism, tests, tables, limits) |
| [docs/methodology/](docs/methodology/) | Measurement and format interpretation |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Source-tree map |
| [results/](results/) | Raw `go test -bench` output |

---

## Limitations

- Single-machine synthetic microbenchmarks; not a multi-tenant calibration study
- Merge and value-log GC are staged/triggered, not continuous background workers
- Crash coverage is semantic faults plus abrupt process exit—not full power-loss simulation
- Autonomous apply is observation-first and off by default; closed-loop QoS gains are not claimed

See each track's *Limitations* section and the paper's *Threats to validity*.

---

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
- [rosedb](https://github.com/flower-corp/rosedb) · [nutsdb](https://github.com/xujiajun/nutsdb)
- [从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
- [优雅的 Bitcask/BeansDB](https://zhuanlan.zhihu.com/p/53682577)

---

<p align="center"><sub>Not a production database. Guarantees stop where the tests stop.</sub></p>
