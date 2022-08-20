# Autonomous Storage Intelligence

## Research question

Can a deterministic local controller infer workload phases and recommend storage policies without making correctness depend on classification accuracy?

## Hypothesis

Windowed operation ratios, overwrite and large-value ratios, and observed sync latency can identify useful workload phases. Minimum sample counts, consecutive-window confirmation, and cooldown prevent noisy inputs from causing policy flapping.

## Current mechanism

`experiments/autonomous` is a pure, filesystem-independent model. An observation window contains:

- reads and writes
- overwrites
- large-value writes
- observed sync latency

The detector classifies the window as:

- unknown
- mixed
- write-heavy
- read-heavy
- overwrite-heavy
- large-value-heavy
- sync-latency-sensitive

The recommender returns a phase, confidence, reason, and advisory choices for sync, compaction, and value placement. A phase transition requires consecutive matching windows and must satisfy a logical-window cooldown. Insufficient samples return unknown/no-change.

The current priority order is sync-latency-sensitive, overwrite-heavy, large-value-heavy, write-heavy, read-heavy, then mixed. This makes a specific storage pressure more informative than a broad read/write ratio.

## Correctness invariant

The model is advisory only:

1. Identical observation sequences produce identical classifications and recommendations.
2. Insufficient observations never trigger an aggressive recommendation.
3. Noise that does not persist for the configured number of windows cannot change the active phase.
4. A recommendation cannot alter transaction visibility, durability, compaction liveness, or pointer consistency because this experiment has no DB integration.
5. Future integration must keep user-configured durability floors authoritative and remain disabled by default.

## Tests

`experiments/autonomous/autonomous_test.go` covers:

- all synthetic phase classes
- confidence and reason output
- minimum sample behavior
- consecutive-window confirmation
- noisy alternating windows
- cooldown-controlled phase transitions
- no-change recommendations for unknown input

Run:

```bash
go test -count=1 ./experiments/autonomous
```

## Benchmarks

The package isolates policy cost from storage and filesystem work:

```bash
go test ./experiments/autonomous -run '^$' -bench BenchmarkDetector -benchmem -count=5
```

- `BenchmarkDetectorAnalyze` measures classification of one immutable window.
- `BenchmarkDetectorObservePhaseTransitions` measures stateful hysteresis and recommendation across changing phases.

Results must be interpreted as controller overhead only; they do not measure whether a recommendation improves DB performance.

### Measured results

| Benchmark | ns/op (median) | B/op (median) | allocs/op (median) |
|---|---|---|---|
| `BenchmarkDetectorAnalyze` | 2.877 | 0 | 0 |
| `BenchmarkAutonomousObservationOverhead/enabled=false` | 39,237 | 826 | 15 |
| `BenchmarkAutonomousObservationOverhead/enabled=true` | 52,466 | 772 | 15 |
| `BenchmarkApplyPolicyRecommendation` | 33.41 | 0 | 0 |
| `BenchmarkAutonomousPhaseTransitions/autonomous=false` | 5,056,609 | 1,130,171 | 1,621 |
| `BenchmarkAutonomousPhaseTransitions/autonomous=true` | 5,210,752 | 1,154,423 | 1,706 |

Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem -count=5`. Raw: [`../../results/track-f-autonomous.txt`](../../results/track-f-autonomous.txt).

## Recommendation-only core integration

`AutonomousOptions.EnableRecommendations` enables low-cost core observations and a recommendation-only API. It is disabled by default and never mutates configured policies.

`DB.AutonomousObservation()` returns cumulative immutable counters for reads, writes, overwrites, large-value writes, value-log pressure, and average observed sync latency. At each configured operation window, the DB computes operation deltas and a window-local average sync latency before feeding the pure detector. Old slow syncs therefore do not keep future fast windows classified as sync-latency-sensitive. `DB.PolicyRecommendation()` returns phase, confidence, sync/compaction/placement suggestions, reason, transition flag, and availability.

Options control window size, large-value threshold, minimum operations, consecutive-window confirmation, and cooldown. Insufficient data produces an unavailable recommendation. Reopen resets observations and detector history without affecting storage state.

```bash
go test -count=1 ./engine -run '^TestAutonomous'
go test ./engine -run '^$' -bench '^BenchmarkAutonomous(ObservationOverhead|WindowRecommendation)$' -benchmem -count=5
```

The compaction subsystem can optionally consume the autonomous phase in its recommendation-only `DB.RecommendCompaction` bridge. This still does not execute merge or mutate policy. Applying recommendations requires separate opt-in gates, confidence/dwell/cooldown limits, and hard user bounds.

Runtime policy changes must never:

- weaken an explicit durability requirement
- dynamically enable unopened value-log infrastructure
- change an on-disk format
- classify record liveness
- delete data

## Limitations

- Thresholds are initial deterministic hypotheses, not measured optimal values.
- Autonomous sync-latency classification now uses window-local sync latency, but commit-latency histograms are still cumulative rather than per-window deltas.
- A window has one primary phase even when several pressures coexist.
- Confidence is a ratio-based score, not a calibrated probability.
- Recommendations have not yet been compared against static policies in end-to-end storage benchmarks.

## Guarded policy application

`AutonomousOptions` now has disabled-by-default `ApplyCompaction` and `ApplyKVPlacement` gates plus `MinConfidence`. `DB.ApplyPolicyRecommendation()` applies only an available recommendation above the confidence floor:

- compaction changes only the runtime picker mode
- lifecycle placement can change only when value-log infrastructure was opened at startup
- sync/durability policy is never changed by this tuner
- unknown recommendation values preserve configured policy

`DB.ResetAutonomousPolicies()` restores startup compaction and lifecycle settings. Tests cover disabled gates, unavailable and below-confidence recommendations, stable overwrite-heavy application, reset, and refusal to create unopened value-log infrastructure.

```bash
go test -count=1 ./engine -run '^TestAutoTune'
go test ./engine -run '^$' -bench '^BenchmarkApplyPolicyRecommendation$' -benchmem -count=5
```

## Bounded audit and GC pressure

`AutonomousOptions.AuditCapacity` configures a fixed-size in-memory ring. `DB.PolicyAudit()` returns an immutable oldest-to-newest snapshot of recommendation and application events with monotonic sequence, phase, confidence, changed-policy flags, reason, and observed value-log stale bytes. Old events are deterministically evicted when capacity is reached; history resets safely on reopen.

`DB.AutonomousObservation()` now aggregates value-log total/live/stale bytes from authoritative core pointer statistics. Each recommendation audit event captures current stale pressure. Pressure is observational in this increment and does not automatically run GC.

```bash
go test -count=1 ./engine -run 'Test(PolicyAudit|AutonomousObservationIncludesValueLogPressure)'
go test ./engine -run '^$' -bench '^BenchmarkPolicyAuditSnapshot$' -benchmem -count=5
```

## Static versus autonomous phase-transition study

`BenchmarkAutonomousPhaseTransitions` runs deterministic write-heavy, read-heavy, overwrite-heavy, and large-value-heavy phases against static and guarded autonomous configurations. It reports total ns/op, allocations, commit histogram p50/p99 bounds, and audit-event count. Correctness tests require the final large-value phase classification, monotonic audit production, expected successful commit count, and no policy/audit changes in static mode.

```bash
go test -count=1 ./engine -run 'TestAutonomousPhase|TestStaticPhase'
go test ./engine -run '^$' -bench '^BenchmarkAutonomousPhaseTransitions$' -benchmem -count=5
```

The current harness applies only the final recommendation after all phases. It evaluates detector transition stability and observation overhead, not continuous closed-loop adaptation quality. Short benchmark samples must not be interpreted as autonomous performance improvement.
