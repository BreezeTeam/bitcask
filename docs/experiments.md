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
- `experiments/compaction`: garbage-ratio and hot/cold picker model.
- `experiments/kvseparation`: value-size threshold modeling.
- `experiments/fault`: deterministic fault schedule replay.
- `experiments/minilsm`: mini-LSM learning scaffold.
- `experiments/raftlog`: replicated-log learning scaffold.

## Correctness invariant

A pure experiment package may simplify storage, but a core DB option must preserve Bitcask transaction visibility, CRC validation, merge correctness, and reopen persistence unless the test is explicitly injecting a fault.

## Benchmark design

Run core benchmarks when measuring real storage behavior:

```bash
go test . -run '^$' -bench 'Benchmark(Tx|WritePath|DBMerge|KVSeparation|Fault|Metrics)' -benchmem -count=3
```

Run experiment benchmarks when isolating policy cost:

```bash
go test ./experiments/... -run '^$' -bench Benchmark -benchmem -count=3
```

## Expected trade-off

Core options give realistic behavior but include file-system and index costs. Experiment packages give cleaner policy measurements but cannot prove storage correctness by themselves.

## Observed result template

| Package | Research question | Core counterpart | Benchmark | Correctness test | Limitation |
| --- | --- | --- | --- | --- | --- |
| TBD | TBD | TBD | TBD | TBD | TBD |
