# Crash Consistency Explorer

## Research question

Can deterministic crash scenarios enumerate partial transaction states, reopen the real engine, and verify committed-marker recovery invariants automatically?

## Crash model

The v1 explorer has two layers:

1. `experiments/fault` defines canonical scenarios, stable IDs, replay traces, and a pure expected-state oracle.
2. Root-package tests materialize those operations as real log entries, open the DB, and compare observed visibility with the oracle.

A stable scenario ID has this form:

```text
recovery/<scenario-name>/fault=<semantic-point>
```

Examples:

```text
recovery/before-commit-marker/fault=before-commit-marker
recovery/uncommitted-newer-overwrite/fault=before-second-commit-marker
recovery/committed-tombstone/fault=after-delete-marker
```

The identifier is based on semantic fields rather than test order, random map iteration, or temporary paths. A failed subtest prints the ID and can be replayed with `go test -run`.

## Invariant oracle

The pure oracle first identifies transaction IDs with a valid committed marker, then applies operations in log order. It tracks:

- latest committed value
- newer uncommitted overwrite
- committed tombstone
- committed expiration against a supplied logical time
- live committed value

The initial invariants are:

1. No operation from a transaction without a valid committed marker is visible.
2. All earlier records sharing a transaction ID become eligible only when that transaction has a marker.
3. The latest committed value wins.
4. A newer uncommitted value cannot hide an older committed value after recovery.
5. A committed tombstone keeps the key absent.
6. An expired committed value remains absent.
7. Corruption never returns corrupted user bytes; the current policy is to fail `Open` safely when entry CRC validation fails.

## Initial scenario matrix

| Scenario | Expected result |
| --- | --- |
| before first write | empty recovered state |
| before commit marker | partial transaction remains invisible |
| after commit marker | all records in the committed transaction are visible |
| uncommitted newer overwrite | older committed value remains visible |
| committed tombstone | key remains absent |
| expired committed value | key remains absent |
| corrupt entry value/header payload | `Open` fails safely |

The low-level `FaultInjectionOptions` wrapper supports deterministic write failure, short write, sync failure, write corruption, and recovery-read corruption counters. It now also supports one configured semantic point plus deterministic occurrence count:

- `before-entry-append`
- `before-commit-marker`
- `before-value-append`
- `before-value-sync`
- `before-main-sync`

`SemanticFailAfter=0` fails the first matching occurrence; larger values permit deterministic replay of later matching events. Legacy IO counters remain compatible.

## Replay

Run all core explorer scenarios:

```bash
go test -count=1 ./engine -run '^TestCrashExplorer'
```

Replay one scenario:

```bash
go test -count=1 ./engine -run 'TestCrashExplorerRecoveryScenarios/recovery/uncommitted-newer-overwrite/fault=before-second-commit-marker'
```

Run the pure model:

```bash
go test -count=1 ./experiments/fault
```

## Benchmarks

Pure scenario generation and oracle replay:

```bash
go test ./experiments/fault -run '^$' -bench 'BenchmarkCrashScenario' -benchmem -count=5
```

Core recovery with 1,000 committed and 1,000 partial transaction records:

```bash
go test ./engine -run '^$' -bench '^BenchmarkRecoveryManyPartialTransactions$' -benchmem -count=5
```

The pure benchmark measures explorer overhead. The core benchmark includes data-file parsing, CRC validation, committed-ID discovery, index reconstruction, and close.

### Measured results

| Benchmark | ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| `BenchmarkCrashScenarioEnumerate` | 1,692 | 2,665 | 57 | pure scenario-ID enumeration |
| `BenchmarkCrashScenarioReplay` | 83,728 | 288,007 | 26 | pure oracle replay |
| `BenchmarkRecoveryTornCommitMarkers` | 129,236 | 6,572 | 64 | real data-file parse across a torn-write boundary |
| `BenchmarkRecoveryManyPartialTransactions` | 7,027,457 | 1,649,596 | 36,938 | ≈ 7.03 ms/op — full core recovery, 1,000 committed + 1,000 partial transaction records |
| `BenchmarkSubprocessCrashRecovery` | 27,332,444 | 20,376 | 102 | ≈ 27.33 ms/op — real child-process crash, parent reopen and oracle check (per-transaction commit) |
| `BenchmarkGroupCommitSubprocessRecovery` | 36,938,783 | 29,664 | 290 | ≈ 36.94 ms/op — real child-process crash, parent reopen and oracle check (group-commit epoch) |

Median of `-count=5`. Measured on Apple M3 Max, darwin/arm64, Go 1.26.2, `-benchmem -count=5`; subprocess-recovery ns/op is device-dependent. Raw: [`../../results/track-c-crash.txt`](../../results/track-c-crash.txt).

## Torn-write boundary exploration

The explorer enumerates every truncation byte across the fixed header, bucket, key, value, and the complete committed-marker entry. IDs include the semantic region, absolute offset, and region end:

```text
recovery/torn-write/region=header/offset=17/size=42
recovery/torn-write/region=value/offset=53/size=58
recovery/torn-write/region=commit-marker/offset=31/size=58
```

Core tests materialize each prefix as a real data file. Recovery may either reject the malformed/torn tail or open safely, but it must never expose the incomplete entry. When an earlier uncommitted transaction entry is followed by a torn committed marker, neither key may become visible.

```bash
go test -count=1 ./engine -run 'TestCrashExplorerTorn'
go test ./experiments/fault -run '^$' -bench '^BenchmarkTornWriteScenarioEnumerate$' -benchmem -count=5
go test ./engine -run '^$' -bench '^BenchmarkRecoveryTornCommitMarkers$' -benchmem -count=5
```

## Abrupt subprocess termination

The root test binary can relaunch itself as a child process, execute one named scenario, and call `os.Exit` without `DB.Close`. The parent requires the dedicated exit code, reopens the same directory, and runs the visibility oracle. Stable subtest IDs include:

```text
subprocess/committed-before-exit
subprocess/before-commit-marker
```

The committed scenario uses EveryCommit and must survive abrupt exit. The pre-marker scenario injects the semantic final-marker fault, exits with an uncommitted tail, and neither transaction key may become visible.

Value-log GC now uses the same real-process harness at `PREPARED`, `POINTERS_INSTALLED`, `before source remove`, `SOURCE_REMOVED`, and `FINALIZED`. The child builds a source containing live and stale values, injects one semantic phase boundary, and exits without `DB.Close`. The parent reopens twice, verifies both logical values, checks pointer-authoritative source retention/removal, and requires manifest cleanup. A crash at `PREPARED` retains the still-referenced source; all later states recover installed pointers and remove the unreferenced source idempotently.

```bash
go test -count=1 ./engine -run '^(TestCrashExplorerSubprocess|TestValueLogGCSubprocess)'
go test ./engine -run '^$' -bench 'Benchmark(SubprocessCrashRecovery|ValueLogGCSubprocessRecovery)' -benchmem -count=5
```

The benchmarks include child process startup, abrupt exit, parent reopen/recovery, and close; they are intentionally separate from pure scenario throughput.

## Group-epoch crash identity

Earlier subprocess scenarios used `EveryCommit`, so they exercised the direct per-transaction sync path. Group commit uses a different path: a coordinator collects concurrent writers into one epoch, releases the DB write lock, runs a single shared durability barrier, and only then resolves every waiter. Two real-process scenarios now cover that path:

```text
subprocess/group-committed-concurrent
subprocess/group-before-commit-marker
```

`group-committed-concurrent` launches eight concurrent `GroupCommit` writers, waits until every `Update` returns success — which happens only after the shared epoch barrier fsyncs — then calls `os.Exit` without `DB.Close`. The parent reopens and requires all eight keys. This proves that once the coordinator acknowledges an epoch, its records survive abrupt (non-power-loss) termination. Because the outcome is asserted only after all writers succeed, the concurrency is deterministic from the recovery oracle's point of view even though epoch batching is non-deterministic.

`group-before-commit-marker` runs a `GroupCommit` `PutBatch` with the semantic `before-commit-marker` fault. The failure occurs while appending under the DB write lock, before the transaction ever enqueues to the coordinator, so no marker is written and nothing is enqueued. After abrupt exit the parent requires both keys absent. This confirms a group transaction that fails before its marker leaves nothing visible and never joins an epoch.

```bash
go test -count=1 ./engine -run '^TestGroupCommitSubprocessRecovery$'
go test ./engine -run '^$' -bench '^BenchmarkGroupCommitSubprocessRecovery$' -benchmem -count=5
```

## Corruption policy

The current policy is conservative: malformed or CRC-invalid entries make `Open` return an error. The engine does not silently expose corrupted bytes. A future tail classifier may safely ignore a provably torn final entry, but it must distinguish that case from corruption in the middle of a durable log before behavior changes.

## Manifest metadata fault model

`experiments/fault` now enumerates manifest metadata crash scenarios independently of the filesystem. Stable IDs cover manifest kind, logical phase, and metadata stage:

```text
manifest/merge/phase=prepared/stage=temp-write
manifest/merge/phase=installed/stage=directory-sync
manifest/value-gc/phase=pointers-installed/stage=rename
manifest/value-gc/phase=finalized/stage=directory-sync
```

The pure oracle maps each scenario to a conservative recovery action:

- temp-write or file-sync failure: ignore the incomplete attempt
- rename failure: retain the source because publication is uncertain
- prepared directory sync: retain the source
- installed or pointers-installed directory sync: replacement is published, so source removal may be completed idempotently
- source-removed or finalized directory sync: clear terminal manifest state idempotently

The core routes merge and value-log GC manifest publication through one atomic metadata helper with pre-operation injection at temp write, file sync, rename, and directory sync. `MetadataStage` selects the boundary and `MetadataFailAfter` deterministically selects its occurrence; defaults remain disabled. The pure model now maps actual publication attempts: merge prepared/installed are occurrences 1/2, while value-log GC initial prepared/range-updated prepared/pointers-installed/source-removed/finalized are occurrences 1–5. Stable IDs include the occurrence.

Unit tests prove which final bytes are visible at every boundary. Core integration tests execute all 8 merge and all 20 GC phase-by-stage combinations, disable injection, reopen, verify live values, enforce source retention before pointer installation and removal afterward, and require manifest cleanup. The same 28 boundaries now run in real child processes: each child fails the selected metadata operation and exits without `DB.Close`; the parent reopens, applies the same value/source oracle, and requires manifest cleanup.

```bash
go test -count=1 ./experiments/fault -run 'Test(Manifest|EnumerateManifest)'
go test ./experiments/fault -run '^$' -bench '^BenchmarkManifestFaultScenarioEnumerate$' -benchmem -count=5
```

## Merge recovery protocol

Merge now writes a versioned CRC manifest for each source with `PREPARED` and `INSTALLED` phases plus first/last replacement file IDs. Replacement data is synchronized before `INSTALLED`. Source removal and manifest removal each receive directory sync. Open-time recovery runs before index rebuild:

- missing/incomplete target range: retain source and clear attempt
- all installed targets present, including an explicitly empty target range: remove source idempotently and clear manifest
- malformed/CRC-invalid manifest: fail open safely

Semantic points `merge-prepared`, `merge-installed`, and `merge-before-source-remove` support deterministic interruption. Each point now also runs in a real child process that exits without `DB.Close`. The parent inspects the manifest before recovery, verifies prepared has an empty target range and retains its source, verifies installed phases name complete synced targets and remove their source, then checks live values and manifest cleanup after reopen. Tests also cover corrupt manifests, missing-target rollback, and metadata-operation matrices.

Limitations remain: replacement files are normal same-directory files rather than a staging directory; the manifest is per-source rather than one atomic multi-source merge set; subprocess termination and manifest-rename/directory-sync faults are not yet explored.

## Limitations

- Subprocess termination covers committed/pre-marker writes, all current GC semantic phases, merge phases, the full metadata matrix, and group-commit epoch acknowledgement/pre-marker paths, but not yet value/data sync failures mid-epoch.
- Torn writes are enumerated by byte-prefix truncation, but semantic injected `WriteAt` faults still use coarse operation counters.
- Initial semantic points identify durability phases but do not yet include resource file IDs, append offsets, epoch IDs, merge phases, or GC phases.
- TTL core tests use controlled relative timestamps; the DB does not yet have an injectable clock.
- Manifest temp-write/sync/rename/directory-sync failures are covered by complete in-process and subprocess merge/GC publication matrices. Manifest deletion directory sync has a distinct injected stage and focused merge/GC recovery tests, deliberately outside publication occurrence numbering.
- Filesystem and hardware write reordering are outside the current model.
