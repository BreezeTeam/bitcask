# Architecture

Source-tree map for the Bitcask research engine. The transactional core lives in
[`engine/`](engine/) as `package bitcask` (one package for tightly coupled `*DB`
state—also the approach in bbolt/badger). `internal/` and `experiments/` keep I/O
helpers and pure policy models separable.

Module: `github.com/BreezeTeam/bitcask`  
Public import: `github.com/BreezeTeam/bitcask/engine` (package name remains `bitcask`)

See also [`docs/paper.md`](docs/paper.md) and
[`docs/ledger/research-tracks-2026.md`](docs/ledger/research-tracks-2026.md).

**Reading path through code:** `engine/tx.go` (commit visibility) →
`engine/group_commit.go` (fsync epochs) → `engine/subprocess_crash_test.go`
(abrupt exit + reopen) → `engine/value_log_gc.go` (pointer safety) → the matching
chapter under [`docs/tracks/`](docs/tracks/).

## Package layout

```
github.com/BreezeTeam/bitcask/
├── engine/                 package bitcask — public engine API + tests/benchmarks
├── internal/               engine-private leaf packages
│   ├── rwmanager/          file read/write abstraction
│   ├── helper/             path + strconv helpers
│   └── id/                 snowflake transaction-ID generator
├── experiments/            pure, dependency-free policy models
│   ├── workload/ adaptivesync/ compaction/ kvseparation/
│   ├── fault/ autonomous/ minilsm/ raftlog/
├── docs/                   paper, tracks/, methodology/, ledger/
├── results/                committed raw benchmark output
├── example/                ecommerce-like backend + scenario benchmarks
├── Makefile                make check / make bench-a…f
└── README.md
```

### Why the core engine is one package

The transactional engine (`db.go`, `tx.go`, `sync_policy.go`, …) is a tightly-coupled
set of methods on `*DB` that share unexported fields. Splitting these into multiple
importable packages would force exporting internals or a large rewrite. Structure is
expressed by keeping that cohesive package under `engine/`, with `internal/` leaves and
`experiments/` pure models outside it.

## Engine file map (`engine/`, package `bitcask`)

| Subsystem | Files | Responsibility |
| --- | --- | --- |
| Core / DB | `db.go`, `options.go`, `entry.go` | open/close/recovery, options, on-disk entry format |
| Transactions | `tx.go` | commit markers, batch writes, ownership-safe pending entries |
| Index | `bptree.go`, `bptree_root_idx.go` | B+ tree RAM and key-only indexes |
| Storage files | `datafile.go`, `merge_manifest.go` | append-only data files, versioned merge manifest |
| Durability (Track B) | `sync_policy.go`, `group_commit.go`, `adaptive_sync_loop.go` | sync policies, group-commit epochs, adaptive loop |
| Compaction (Track D) | `compaction_policy.go`, `compaction_observation.go`, `compaction_recommendation.go` | measured stats, pickers, SLO recommendation, budgeted merge |
| Value-log / KV separation (Track E) | `value_log.go`, `value_log_stats.go`, `value_log_gc.go`, `value_log_gc_manifest.go`, `lifecycle_placement.go` | segmented value logs, GC, CRC manifest v2, lifecycle placement |
| Crash / fault (Track C) | `fault_injection.go`, `metadata_ops.go` | deterministic RW + semantic/metadata fault injection |
| Autonomous (Track F) | `autonomous.go`, `autotune.go` | phase detection, guarded policy application |
| Observability | `observability.go`, `latency_metrics.go` | metric snapshots, commit-latency histogram |

Tests and benchmarks live beside their subsystem in `engine/` (`*_test.go`,
`*_bench_test.go`), plus `crash_explorer_test.go` and `subprocess_crash_test.go`.

## Dependency direction

```
example ─┐
         ├─> engine (package bitcask) ─> internal/{rwmanager, helper, id}
tests ───┘         │
                   └─> experiments/*  (models only; experiments do not import engine)
```

## Where to start reading

1. [`docs/paper.md`](docs/paper.md) — research questions, contributions, results.
2. [`docs/ledger/research-tracks-2026.md`](docs/ledger/research-tracks-2026.md) — six-track status matrix.
3. A track report under [`docs/tracks/`](docs/tracks/).
4. The corresponding files under `engine/`, then their `*_test.go`.
