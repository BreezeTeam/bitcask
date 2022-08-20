package fault

import "testing"

func BenchmarkFaultScheduleDisabled(b *testing.B) {
	schedule := Schedule{WriteFailAfter: -1, SyncFailAfter: -1, ReadCorruptAfter: -1}
	counters := Counters{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = schedule.Observe(counters, PointWrite)
		counters.Writes++
	}
}

func BenchmarkFaultScheduleEnabledNoFault(b *testing.B) {
	schedule := Schedule{WriteFailAfter: int64(b.N + 1), SyncFailAfter: int64(b.N + 1), ReadCorruptAfter: int64(b.N + 1)}
	counters := Counters{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = schedule.Observe(counters, PointWrite)
		counters.Writes++
	}
}

func BenchmarkFaultScheduleRecoveryManyPartialTransactions(b *testing.B) {
	schedule := Schedule{WriteFailAfter: 2, SyncFailAfter: -1, ReadCorruptAfter: -1}
	points := []Point{PointWrite, PointWrite, PointWrite, PointSync}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Replay(schedule, points)
	}
}

func BenchmarkCrashScenarioReplay(b *testing.B) {
	scenario := Scenario{
		Name:       "many-partial-transactions",
		FaultPoint: "before-final-commit-marker",
		Operations: make([]ScenarioOperation, 0, 2000),
	}
	for i := 0; i < 1000; i++ {
		scenario.Operations = append(scenario.Operations,
			ScenarioOperation{Kind: OperationPut, TxID: uint64(i*2 + 1), Key: "committed", Value: "value", Committed: true},
			ScenarioOperation{Kind: OperationPut, TxID: uint64(i*2 + 2), Key: "partial", Value: "hidden"},
		)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ReplayScenario(scenario, 100)
	}
}

func BenchmarkTornWriteScenarioEnumerate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EnumerateTornWriteScenarios(42, 8, 32, 1024)
	}
}

func BenchmarkCrashScenarioEnumerate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EnumerateRecoveryScenarios()
	}
}

func BenchmarkManifestFaultScenarioEnumerate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EnumerateManifestFaultScenarios()
	}
}
