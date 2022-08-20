package compaction

import (
	"testing"
	"time"
)

func BenchmarkPickByGarbageRatio(b *testing.B) {
	segments := benchmarkSegments(1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = PickByGarbageRatio(segments, 0.3)
	}
}

func BenchmarkPickHotCold(b *testing.B) {
	segments := benchmarkSegments(1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = PickHotCold(segments, 0.3, 100)
	}
}

func BenchmarkCompactionPolicyLargeSegmentSet(b *testing.B) {
	segments := benchmarkSegments(64 * 1024)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = PickHotCold(segments, 0.3, 100)
	}
}

func BenchmarkSLOControllerEvaluate(b *testing.B) {
	controller := NewController(DefaultControllerConfig())
	obs := ControllerObservation{
		ForegroundP99:       5 * time.Millisecond,
		TargetP99:           10 * time.Millisecond,
		PendingGarbageBytes: 128 << 20,
		DataBytes:           256 << 20,
		WriteAmplification:  1.5,
		SpaceAmplification:  1.5,
		Phase:               PhaseOverwriteHeavy,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = controller.Evaluate(obs)
	}
}

func BenchmarkSLOControllerObserveTransitions(b *testing.B) {
	controller := NewController(DefaultControllerConfig())
	observations := []ControllerObservation{
		{ForegroundP99: 5 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20},
		{ForegroundP99: 20 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20},
		{ForegroundP99: 5 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20, SpaceAmplification: 2.5},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = controller.Observe(observations[i%len(observations)])
	}
}

func benchmarkSegments(n int) []Segment {
	segments := make([]Segment, n)
	for i := range segments {
		size := int64(1024 + i%4096)
		segments[i] = Segment{ID: i, Size: size, LiveBytes: int64(i % int(size)), ReadCount: int64(i % 256)}
	}
	return segments
}
