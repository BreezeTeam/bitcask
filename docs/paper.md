# Bitcask Research KV Engine — an experimental study of write-path, durability, crash-consistency, compaction, KV-separation, and self-tuning trade-offs in a log-structured key/value store

> **v1.0.0 complete.** Six finished tracks (mechanism · invariant · tests · measured
> tables · limits). Overview: [`../README.md`](../README.md).
> 中文：[`../README.zh.md`](../README.zh.md) · [`paper.zh.md`](paper.zh.md).

## Key findings at a glance

Measured on Apple M3 Max / darwin/arm64 / Go 1.26.2 / `-count=5`. Raw:
[`../results/`](../results/).

| Finding | Number |
| --- | --- |
| Small put, no sync | ≈ **39 µs** |
| Same put, every-commit sync | ≈ **4.4 ms** (fsync-bound) |
| Group commit vs every-commit @ 16 writers | ≈ **14×** (636 µs vs 8.87 ms) |
| Budgeted vs static merge (amortized) | ≈ **44 µs** vs ≈ **1.74 ms** |
| 64 KB put inline → separated | ≈ **1.88 ms → 914 µs** |
| Real group-epoch crash recover + oracle | ≈ **37 ms/op** |

Read the argument, not only the table: sections below.

> This document is the "paper" view of the repository: research questions, six tracks,
> headline measured results, and links to reproducible evidence under
> [`tracks/`](tracks/), [`methodology/`](methodology/), and [`../results/`](../results/).

## Abstract

Bitcask is a simple, well-understood log-structured key/value design: an append-only
data log with an in-memory index over the latest live record of each key. Its
simplicity makes it an unusually good substrate for *studying* storage-engine
trade-offs, because each mechanism can be added and measured in isolation without an
LSM's compaction machinery obscuring the signal. This project takes a working Go
Bitcask engine and extends it along six research tracks — write-path allocation,
durability pipelining, crash-consistency exploration, SLO-aware compaction, lifecycle
KV separation, and autonomous self-tuning — each with an explicit mechanism, a stated
correctness invariant, deterministic tests (including real subprocess-crash recovery),
and reproducible benchmarks. The engine uses import path
`github.com/BreezeTeam/bitcask/engine` (package name `bitcask`) and unchanged on-disk
formats throughout; every experimental feature is
opt-in and documented with its limitations. The contribution is not a new algorithm
but a *measured, honest, reproducible* account of where the costs are and what each
mechanism does and does not buy.

## Research questions

- **RQ1 (Write path).** Where does per-write latency come from, and which costs are
  fixed per entry versus proportional to value size? Can batch APIs and buffer/arena
  reuse reduce allocations without changing object-lifetime safety?
- **RQ2 (Durability).** How much does coalescing fsyncs (group commit) and adapting
  sync timing (adaptive sync) reduce durability cost, and can that be done without
  inventing stronger-than-configured durability?
- **RQ3 (Crash consistency).** Can crash outcomes be made *deterministic* and
  *checkable* — semantic fault points plus a real process that exits without a clean
  close — so that "uncommitted transactions are invisible after reopen" is a tested
  invariant rather than a claim?
- **RQ4 (Compaction).** Can compaction be driven by *measured* garbage/hotness and a
  latency budget instead of a fixed heuristic, and what is the foreground-latency cost
  of merging?
- **RQ5 (KV separation).** When values are separated into a segmented value log, what
  is the put/get cost, and can lifecycle placement plus pointer-safe GC reclaim space
  without dangling references?
- **RQ6 (Autonomous).** Can the engine detect workload phases and recommend/apply
  policy changes under guardrails that never weaken explicit durability, and what is
  the observation overhead?

## Contributions

1. A **single cohesive engine** under [`engine/`](../ARCHITECTURE.md) (`package bitcask`)
   with engine-private leaves under `internal/`, kept format-compatible across all six
   tracks.
2. Per-track **mechanisms with stated invariants**, each backed by deterministic tests
   and, for durability/crash tracks, **real subprocess-crash recovery** tests that
   `os.Exit` mid-flight and reopen against an oracle.
3. A **reproducibility harness** (`make check`, `make bench`) and committed raw
   benchmark data under [`../results/`](../results/), so every headline number in this
   paper can be regenerated on the reader's hardware with one command.
4. An **honest limitations posture**: each track chapter ends with what is *not* yet
   covered (continuous background workers, full filesystem-loss simulation, closed-loop
   autonomous gains), so no untested claim is presented as a guarantee.

## System overview

The write path is `DB.Update/PutBatch → Begin → Tx.Put → Commit → append → encode →
WriteAt → index update → sync`. Visibility is gated by a **committed marker**: a
transaction becomes visible only after its final entry is written with `Committed`
status and its id is recorded; recovery ignores uncommitted entries. On top of this
core, the tracks add: sync policies and a group-commit coordinator (B); semantic and
metadata-stage fault injection with a crash oracle (C); measured compaction
observations, pickers, and a budgeted merge (D); a segmented value log with lifecycle
placement and stop-the-world GC behind CRC manifests (E); and a workload phase detector
with guarded policy application (F). See [`../ARCHITECTURE.md`](../ARCHITECTURE.md) for
the subsystem-to-file map and the acyclic dependency direction.

## The six tracks

| Track | Research question | Mechanism | Key invariant | Chapter |
| --- | --- | --- | --- | --- |
| A — Write Path & Allocation | RQ1 | batch APIs, encode-buffer + key/value arena reuse, owned pending entries | caller mutation after `Put` cannot alter committed/indexed/encoded bytes | [write-path](tracks/write-path.md) |
| B — Durability Pipeline | RQ2 | `None`/`EveryCommit`/`GroupCommit`/`Adaptive`, ordered vlog-before-main sync | group commit coalesces shared fsyncs without exceeding configured durability | [durability-pipeline](tracks/durability-pipeline.md) |
| C — Crash Consistency Explorer | RQ3 | semantic + metadata-stage fault points, subprocess abrupt-exit, crash oracle | uncommitted / torn-marker transactions are invisible after reopen | [crash-explorer](tracks/crash-explorer.md) |
| D — SLO-aware Compaction | RQ4 | measured garbage/hotness observations, pickers, latency-budgeted merge | merge preserves latest-live-record visibility under a byte budget | [slo-compaction](tracks/slo-compaction.md) |
| E — Lifecycle KV Separation | RQ5 | segmented value log, lifecycle placement, pointer-safe stop-the-world GC | GC never leaves a live key pointing at a reclaimed value | [lifecycle-kv-separation](tracks/lifecycle-kv-separation.md) |
| F — Autonomous Storage | RQ6 | workload phase detector, policy recommender, guarded apply | apply never changes sync policy or weakens durability | [autonomous-storage](tracks/autonomous-storage.md) |

Track status matrix and cross-track invariants:
[`ledger/research-tracks-2026.md`](ledger/research-tracks-2026.md).

## Headline results

Measured on **Apple M3 Max, darwin/arm64, Go 1.26.2, APFS SSD**, `-benchmem -count=5`.
Absolute `ns/op` for sync/fsync and subprocess rows are device-dependent; `allocs/op`
and `B/op` are the portable signal. Raw output: [`../results/`](../results/).

| Track | Representative benchmark | Measured (ns/op) | B/op | allocs/op | Reading |
| --- | --- | --- | --- | --- | --- |
| A | `TxCommitSingleEntryBaseline/32B/sync=false` | 39,134 | 865 | 15 | fixed-cost dominated at small values |
| A | `TxCommitSingleEntryBaseline/32B/sync=true` | 4,407,716 | 836 | 15 | fsync (~4.4 ms) dominates end-to-end latency |
| B | `ConcurrentCommitPolicies` group vs every, 16 writers | 636,271 vs 8,873,376 | 985 | 19 | group commit coalesces fsyncs ~14× under concurrency (syncs/commit 1.0→0.06) |
| C | `GroupCommitSubprocessRecovery` | 36,938,783 | 29,664 | 290 | real crash (`os.Exit`) + reopen, oracle-checked (~36.9 ms) |
| D | `CompactionForegroundImpact` budgeted vs static | 43,823 vs 1,737,462 | 1,383 | 19 | budgeted stays near no-merge (~40 µs); static amortizes ~1.74 ms + ~69 KB rewrite |
| E | `KVSeparationPut` 64KB (sep vs inline) | 913,702 vs 1,882,589 | 66,408 | 16 | separation ~halves large-value put; get stays ~39 µs |
| F | `AutonomousObservationOverhead` (enabled vs off) | 52,466 vs 39,237 | 772 | 15 | detection adds ~13 µs/commit under load; apply is 33 ns/op |

Per-track "Benchmarks" sections in each chapter carry the fuller tables; the measured
methodology and all mappings are in
[`methodology/benchmarks.md`](methodology/benchmarks.md).

## Conclusions

Under the stated harness (single machine, synthetic microbenchmarks, opt-in features):

1. **Write path (RQ1).** Small values are fixed-cost dominated; enabling per-commit sync
   moves end-to-end latency into the fsync regime (~ms). Encode-buffer reuse and batching
   cut portable allocation cost without changing ownership safety.
2. **Durability (RQ2).** Group commit helps under concurrency (≈14× at 16 writers) by
   collapsing syncs/commit; it does not invent stronger-than-configured durability.
   Adaptive sync trades a bounded delay window for lower sync rate.
3. **Crash consistency (RQ3).** Semantic fault points plus real `os.Exit` + reopen make
   “uncommitted work stays invisible” an oracle-checked property inside a bounded crash
   model — not a full power-loss simulator.
4. **Compaction (RQ4).** A logical-byte budget keeps foreground cost near the no-merge
   baseline in the synchronous harness; unbounded static merge amortizes far higher.
   Continuous overlapping background compaction is out of scope for v1.0.
5. **KV separation (RQ5).** Large-value separation roughly halves put cost while get stays
   near inline; pointer-safe GC is covered by staged manifests and crash tests.
6. **Autonomy (RQ6).** Phase detection and guarded recommendations have measurable, small
   overhead; apply never weakens explicit sync policy. Closed-loop QoS gains are not
   claimed.

These are **local, reproducible findings** for this artifact, not portable absolute
latencies or production SLOs. Scope boundaries are in each track’s *Limitations* and
in *Threats to validity* below.

## Reproducibility

```bash
make check      # gofmt + go vet + go test + go test -race + git diff --check
make bench      # all six track suites → results/track-*.txt  (COUNT=5)
make bench-a    # a single track (a…f)
```

Environment, commands, and portability caveats are recorded in
[`../results/README.md`](../results/README.md). The committed `results/track-*.txt`
files are the exact raw output behind the tables above.

## Threats to validity and limitations

- **Single-machine measurement.** All numbers are process-local on one laptop-class
  machine; they are not calibrated multi-tenant, multi-disk, or server profiles.
- **Device-dependent durability numbers.** fsync-bearing rows measure the SSD and OS as
  much as the Go code; do not port absolute latencies across hardware.
- **Bounded crash model.** Crash coverage is deterministic semantic/metadata fault
  points plus subprocess abrupt-exit and reopen; it is not full power-loss or
  filesystem-corruption simulation.
- **Not continuous.** Merge and value-log GC are staged and tested but run as
  foreground/triggered operations, not continuous background workers.
- **Autonomous is observation-first.** The apply path is guarded (never touches sync
  policy) and the current harness applies a final recommendation, so it measures
  detection overhead rather than proven closed-loop gains.

Each track chapter restates the limitations specific to its mechanism. Nothing in this
paper should be read as a durability or crash guarantee beyond what the linked tests
exercise.
