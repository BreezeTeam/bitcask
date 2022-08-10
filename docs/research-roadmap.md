# Bitcask 2026 Research Roadmap

## Research question

How far can a simple Bitcask-style append-only hash index be pushed as a learning platform for modern KV storage design?

## Hypothesis

A compact Bitcask implementation can demonstrate many production storage trade-offs if each advanced mechanism is isolated behind options, measured with repeatable benchmarks, and documented with correctness invariants.

## Current baseline

The project already supports close/reopen persistence, committed transaction recovery, tombstones, TTL, merge, range/prefix scans, example backend workloads, and write-path microbenchmarks.

## Frontier experiments

### 1. Group Commit / Batch Sync

- **Research question:** How much write throughput is lost when every entry performs fsync independently?
- **Hypothesis:** Transaction-level sync and group sync amortize fsync cost without changing committed-marker recovery semantics.
- **Mechanism:** Move from per-entry sync toward per-transaction and opt-in group commit policies.
- **Benchmark design:** Compare sync disabled, every commit, and group commit across batch sizes and value sizes.

### 2. Adaptive Sync Policy

- **Research question:** Can durability latency be adjusted from observed dirty bytes and sync cost?
- **Hypothesis:** Adaptive sync can reduce average write latency while bounding unsynced data.
- **Mechanism:** Sync when dirty bytes or max delay crosses a threshold; delay when recent sync latency is high and dirty bytes are low.
- **Current implementation:** A pure `experiments/adaptivesync` policy model plus a core opt-in `SyncPolicyAdaptive` path that tracks dirty bytes, dirty commits, last sync time, and last sync latency.
- **Benchmark design:** Compare every-commit, group-commit, and adaptive policies on mixed write workloads.

### 3. Hot/Cold Compaction Priority

- **Research question:** Can compaction rewrite fewer hot bytes by prioritizing cold garbage-heavy segments?
- **Hypothesis:** Garbage ratio plus hot/cold information improves compaction efficiency over file-id order.
- **Mechanism:** Score segments by garbage ratio and coldness, then rewrite selected candidates.
- **Current implementation:** `experiments/compaction` contains deterministic garbage-ratio and hot/cold pickers; core `DB.Merge()` routes file selection through `CompactionOptions` while preserving rewrite correctness.
- **Benchmark design:** Compare picked segments, rewritten bytes, and preserved live data under overwrite-heavy workloads.

### 4. Value-size-aware KV Separation

- **Research question:** When should large values move out of the main log?
- **Hypothesis:** Large values benefit from value-log separation, while small values suffer from pointer fixed cost.
- **Mechanism:** Store large values in a value log and keep pointer records in the Bitcask log.
- **Current implementation:** `KVSeparationOptions` enables threshold-based value separation. Large values are written to `values.vlog`, while the Bitcask log stores a CRC-protected pointer payload marked as `DataStructureValuePointer`.
- **Benchmark design:** Compare inline and separated values at 512B, 4KB, and 64KB.

### 5. Deterministic Crash / Fault Injection

- **Research question:** Which crash points can violate recovery, merge, or committed transaction visibility?
- **Hypothesis:** Named deterministic fault points make recovery invariants testable without relying on random crashes.
- **Mechanism:** `FaultInjectionOptions` wraps the DataFile `RWManager` and can inject write failures, short writes, sync failures, write corruption, and recovery read corruption on deterministic counters.
- **Current implementation:** Core DB data-file creation uses an opt-in fault wrapper, while `experiments/fault` provides a pure schedule model for benchmarkable fault-point reasoning.
- **Benchmark design:** Measure disabled overhead, enabled no-fault overhead, and recovery from partial transactions.

### 6. Storage Observability

- **Research question:** Which metrics explain write latency, compaction cost, and recovery behavior?
- **Hypothesis:** Low-cost local metrics make benchmark results explainable and reduce debugging ambiguity.
- **Mechanism:** Track commits, entries, bytes, syncs, sync errors, sync latency, rotations, merge bytes, and recovery counts through `DB.Metrics()`.
- **Benchmark design:** Measure metrics snapshot cost and write-path overhead with metrics enabled.

## Optimization priorities

1. Batch write transactions should be measured against one-transaction-per-entry writes.
2. Per-entry allocation should be reduced only where object lifetime is obvious.
3. Strong durability should not fsync every entry inside a batch.
4. Small-value writes should be interpreted as fixed-cost dominated rather than bandwidth dominated.

## Observed result template

Each experiment should record:

- benchmark command
- baseline result
- optimized result
- allocation delta
- latency or throughput delta
- correctness tests used
- remaining limitations
