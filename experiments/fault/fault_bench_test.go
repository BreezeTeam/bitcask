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
