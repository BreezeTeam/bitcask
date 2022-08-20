# Six-track status (v1.0.0)

Status record for the Bitcask research program. **Complete.**  
Narrative and findings: [`../paper.md`](../paper.md). Chapters: [`../tracks/`](../tracks/).

A track is done when it has a mechanism, invariant, tests, benchmarks, measured report,
limitations, and reproducible commands. All six meet that bar.

## Matrix

| Track | Research question | Finding | Report |
| --- | --- | --- | --- |
| A — Write path | Which write costs are fixed vs proportional vs avoidable? | Small puts ≈39 µs; every-commit sync ≈4.4 ms; encode reuse → 0 allocs/op | [write-path](../tracks/write-path.md) |
| B — Durability | Can writers share an fsync barrier without false durability? | @16 writers group commit ≈14× vs every-commit (syncs/commit 1.0→0.06) | [durability-pipeline](../tracks/durability-pipeline.md) |
| C — Crash consistency | Can crash outcomes be replayed against an oracle? | Real `os.Exit` + reopen; group-epoch recovery ≈37 ms/op | [crash-explorer](../tracks/crash-explorer.md) |
| D — Compaction | Can measured garbage/hotness drive a latency budget? | Budgeted merge ≈44 µs vs static ≈1.74 ms (+~69 KB rewrite) | [slo-compaction](../tracks/slo-compaction.md) |
| E — KV separation | When do separated values pay off, and how to GC safely? | 64 KB put ≈2× with separation; get ≈39 µs; pointer-safe GC | [lifecycle-kv-separation](../tracks/lifecycle-kv-separation.md) |
| F — Autonomy | Can phase detection recommend policy without weakening sync? | Observation ≈+13 µs/commit; apply ≈33 ns; sync policy never weakened | [autonomous-storage](../tracks/autonomous-storage.md) |

Evidence: [`../../results/`](../../results/). Commands: `make check` · `make bench` / `make bench-a`…`f`.

## Done bar (A–F)

- [x] Mechanism behind opt-in options (default Bitcask path preserved)
- [x] Stated correctness invariant
- [x] Deterministic tests (fault/subprocess coverage where required)
- [x] Benchmarks and measured tables in the track report
- [x] Limitations and reproducible commands recorded

## Cross-track invariants

1. Default `github.com/BreezeTeam/bitcask/engine` API and on-disk formats remain backward compatible within v1.0.  
2. Performance and crash claims cite a command and, where applicable, a `results/` file.  
3. Pure policy models live in `experiments/`; persistence and recovery stay in the engine.  
4. Autonomous apply is off by default and must not weaken an explicit sync policy.  
5. Merge / value-log GC publish through staged, fsynced manifests before discarding sources.  
6. No unsafe pooling of objects that indexes, waiters, or recovery may retain.  
7. Continuous background compaction workers are out of scope for v1.0.

## Program history

Milestones v0.3→v1.0 are closed. Original roadmap intent (archive):
[`research-roadmap.md`](research-roadmap.md).
