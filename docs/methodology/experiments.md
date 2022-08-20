# Experiments

## Research question

How should this repository separate production-like Bitcask mechanisms from pure research models while keeping both benchmarkable?

## Hypothesis

Core DB options are best for mechanisms that must satisfy storage invariants, while `experiments/` packages are best for pure policy models that can be tested and benchmarked without file-system noise.

## Mechanism

Core opt-in mechanisms:

- `SyncPolicyOptions`: transaction-level, group-framework, and adaptive sync policies.
- `CompactionOptions`: file-id, garbage-ratio, and hot/cold merge selection.
- `KVSeparationOptions`: threshold-based value-log separation.
- `FaultInjectionOptions`: deterministic DataFile IO failures and corruption.
- `DB.Metrics()`: local observability snapshot.

Pure experiment packages:

- `experiments/workload`: deterministic workload generators for small values, hotset overwrites, and mixed read/write workloads.
- `experiments/adaptivesync`: adaptive sync decision model.
- `experiments/compaction`: garbage-ratio/hot-cold pickers and an SLO-aware p99/amplification budget controller.
- `experiments/kvseparation`: value-log pointer model, lifecycle placement policy, and live/stale byte accounting.
- `experiments/fault`: deterministic fault schedules, canonical crash scenarios, stable replay IDs, traces, and a committed-state oracle.
- `experiments/autonomous`: workload phase detection and stable policy recommendations.
- `experiments/minilsm`: mini-LSM learning scaffold.
- `experiments/raftlog`: replicated-log learning scaffold.

## Correctness invariant

A pure experiment package may simplify storage, but a core DB option must preserve Bitcask transaction visibility, CRC validation, merge correctness, and reopen persistence unless the test is explicitly injecting a fault.

## Benchmark design

Run core benchmarks when measuring real storage behavior:

```bash
go test ./engine -run '^$' -bench 'Benchmark(Tx|WritePath|DBMerge|KVSeparation|Fault|Metrics)' -benchmem -count=3
```

Run experiment benchmarks when isolating policy cost:

```bash
go test ./experiments/... -run '^$' -bench Benchmark -benchmem -count=3
```

## Expected trade-off

Core options give realistic behavior but include file-system and index costs. Experiment packages give cleaner policy measurements but cannot prove storage correctness by themselves.

## Observed results

| Package | Research question | Core counterpart | Benchmark | Correctness test | Limitation |
| --- | --- | --- | --- | --- | --- |
| `experiments/workload` | Can synthetic streams be deterministic? | none (feed-only) | package benches | generator shape tests | no disk; not a storage proof |
| `experiments/adaptivesync` | When should sync delay vs force? | `SyncPolicyAdaptive` | adaptive decision benches | threshold / bound tests | model ignores device variance |
| `experiments/compaction` | Can p99 + amp bound merge budget? | `CompactionOptions` + `MergeWithBudget` | `BenchmarkAnalyzeCompactionWorkload`, `BenchmarkSLOController` | controller + workload tests | thresholds uncalibrated across machines |
| `experiments/kvseparation` | Inline vs pointer placement? | `KVSeparationOptions` + value log | pure lifecycle/stats benches | pointer/lifecycle model tests | pure model ≠ IO cost |
| `experiments/fault` | Can crashes be replayed with an oracle? | `FaultInjectionOptions` + subprocess harness | `BenchmarkCrashScenario*` | scenario + oracle tests | not full power-loss / FS corruption |
| `experiments/autonomous` | Can phases map to safe recommendations? | `AutonomousOptions` / auto-tune | `BenchmarkDetectorAnalyze` | detector + guard tests | apply disabled by default; no sync weakening |
| `experiments/minilsm` | Teaching scaffold for LSM ideas | none | package benches | scaffold tests | not wired into core Bitcask |
| `experiments/raftlog` | Teaching scaffold for replicated log | none | package benches | scaffold tests | not a consensus production stack |

Core measured counterparts live under [`../tracks/`](../tracks/) and [`../../results/`](../../results/).
