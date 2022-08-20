package autonomous

import (
	"testing"
	"time"
)

func BenchmarkDetectorAnalyze(b *testing.B) {
	detector := NewDetector(DefaultConfig())
	obs := Observation{
		Reads:            200,
		Writes:           800,
		Overwrites:       300,
		LargeValueWrites: 100,
		SyncLatency:      2 * time.Millisecond,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = detector.Analyze(obs)
	}
}

func BenchmarkDetectorObservePhaseTransitions(b *testing.B) {
	detector := NewDetector(DefaultConfig())
	observations := []Observation{
		{Reads: 200, Writes: 800},
		{Reads: 800, Writes: 200},
		{Reads: 200, Writes: 800, Overwrites: 600},
		{Reads: 200, Writes: 800, LargeValueWrites: 600},
		{Reads: 200, Writes: 800, SyncLatency: 10 * time.Millisecond},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = detector.Observe(observations[i%len(observations)])
	}
}
