# References

## Research question

Which external systems and papers should frame the next stages of this KV-storage learning lab?

## Primary references

- Bitcask: A Log-Structured Hash Table for Fast Key/Value Data.
- LevelDB and RocksDB: LSM-tree implementation references for compaction, write stalls, WAL, and iterator design.
- WiscKey: KV separation and value-log garbage collection.
- PebblesDB / Dostoevsky: compaction policy trade-offs and write amplification.
- Raft: replicated log and consensus foundation for distributed KV storage.

## Implementation references

- rosedb: Go log-structured KV storage design.
- nutsdb: Go Bitcask-inspired storage engine.
- BeansDB: Bitcask-style engineering trade-offs.

## How to use these references

Use references to generate experiments, not to cargo-cult implementation details. Each imported idea should be reduced to:

1. research question
2. hypothesis
3. minimal mechanism
4. correctness invariant
5. benchmark design
6. observed limitation

## Next research directions

- True cross-writer group commit by separating append, visibility, and durability wait.
- Iterator/range API with snapshot semantics.
- Value-log garbage collection for KV separation.
- More accurate per-segment live-byte and read-hotness tracking.
- Crash-safe merge install protocol.
- Mini-LSM flush, manifest, and compaction experiments.
- Raft-backed replicated KV storage artifact.
