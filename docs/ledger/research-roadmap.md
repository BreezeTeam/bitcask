# Research roadmap (archive)

> **Archive.** The six-track program is **complete at v1.0.0**.  
> Read findings in [`../paper.md`](../paper.md); completion record in
> [`research-tracks-2026.md`](research-tracks-2026.md).

## Original question

How far can a simple Bitcask-style append-only design be pushed as a platform for
studying modern KV storage trade-offs?

## Hypothesis

If each advanced mechanism is isolated behind options, measured with repeatable
benchmarks, and documented with correctness invariants, a compact Bitcask engine can
expose production-relevant trade-offs without claiming to be a multi-tenant database.

## What the program covered

| Theme (early roadmap name) | Landed as track |
| --- | --- |
| Group commit / batch sync, adaptive sync | B — Durability pipeline |
| Hot/cold & garbage-aware compaction | D — SLO-aware compaction |
| Value-size-aware KV separation | E — Lifecycle KV separation |
| Deterministic crash / fault injection | C — Crash consistency explorer |
| Storage observability | Cross-cutting (`DB.Metrics`, latency histograms); methodology notes |
| Write-path allocation & batching | A — Write path & allocation |
| Workload-aware policy recommendations | F — Autonomous storage |

## Outcome

v1.0.0 ships mechanisms, invariants, tests, measured tables, and limitations for all six
tracks. The program is closed at this release.
