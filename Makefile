# Bitcask Research KV Engine — reproducibility harness.
#
# Verification gates and the per-track benchmark suites used to produce the
# measured tables in docs/methodology/benchmarks.md and results/.
#
# Benchmark numbers are device- and OS-dependent (fsync latency especially).
# Raw output captured on the maintainer's machine lives under results/.
# Regenerate on your own hardware with `make results`.

GO        ?= go
COUNT     ?= 5
BENCHFLAGS = -run '^$$' -benchmem -count=$(COUNT)
RESULTS    = results

.PHONY: all check fmt vet test race diffcheck \
        bench bench-a bench-b bench-c bench-d bench-e bench-f results clean-results help

## help: list targets
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed 's/## //'

## all: fmt vet test
all: fmt vet test

## check: full verification gate (fmt + vet + test + race + whitespace)
check: fmt vet test race diffcheck

## fmt: format all Go sources in place
fmt:
	gofmt -w .

## vet: go vet the whole module
vet:
	$(GO) vet ./...

## test: run the full test suite once
test:
	$(GO) test -count=1 ./...

## race: run the full test suite under the race detector
race:
	$(GO) test -race -count=1 ./...

## diffcheck: reject stray whitespace errors
diffcheck:
	git diff --check

# ----- Benchmark suites (Track A–F). Output also echoed to results/<track>.txt -----

## bench: run every track benchmark suite into results/
bench: bench-a bench-b bench-c bench-d bench-e bench-f

## bench-a: Track A — write path & allocation
bench-a:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(WriteAllocationMatrix|TxCommitSingleEntryBaseline|TxPutBatch|WritePathEncode|WritePathDataFileAppend|WritePathBPTreeInsert|SmallValueFixedCost.*)$$' | tee $(RESULTS)/track-a-write-path.txt

## bench-b: Track B — durability pipeline
bench-b:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(ConcurrentCommitPolicies|TxCommitSyncPolicy|TxCommitAdaptiveSync|ExplicitFlush|GroupCommitDurabilityResources|AdaptiveSyncDelayedWrite)$$' | tee $(RESULTS)/track-b-durability.txt

## bench-c: Track C — crash consistency (includes subprocess forks; slower)
bench-c:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(RecoveryManyPartialTransactions|RecoveryTornCommitMarkers|GroupCommitSubprocessRecovery|SubprocessCrashRecovery)$$' | tee $(RESULTS)/track-c-crash.txt
	$(GO) test ./experiments/fault $(BENCHFLAGS) -bench '^BenchmarkCrashScenario' | tee -a $(RESULTS)/track-c-crash.txt

## bench-d: Track D — SLO-aware compaction
bench-d:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(CompactionForegroundImpact|DBMergePolicy|CompactionRecommendation)$$' | tee $(RESULTS)/track-d-compaction.txt
	$(GO) test ./experiments/compaction $(BENCHFLAGS) -bench '^BenchmarkAnalyzeCompactionWorkload$$' | tee -a $(RESULTS)/track-d-compaction.txt

## bench-e: Track E — lifecycle KV separation
bench-e:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(KVSeparationPut|KVSeparationGet|SegmentedValueLogAppendRead|ValueLogGC|ValueLogStatsCore|LifecyclePlacementPut)$$' | tee $(RESULTS)/track-e-kv-separation.txt

## bench-f: Track F — autonomous storage intelligence
bench-f:
	@mkdir -p $(RESULTS)
	$(GO) test ./engine $(BENCHFLAGS) -bench '^Benchmark(AutonomousPhaseTransitions|AutonomousObservationOverhead|ApplyPolicyRecommendation)$$' | tee $(RESULTS)/track-f-autonomous.txt
	$(GO) test ./experiments/autonomous $(BENCHFLAGS) -bench '^BenchmarkDetectorAnalyze$$' | tee -a $(RESULTS)/track-f-autonomous.txt

## results: alias for bench (regenerate all committed raw benchmark data)
results: bench

## clean-results: remove captured benchmark output
clean-results:
	rm -f $(RESULTS)/track-*.txt
