package bitcask

import (
	"sync"
	"testing"
	"time"
)

func TestCommitLatencyHistogramPercentiles(t *testing.T) {
	var histogram commitLatencyHistogram
	for _, latency := range []time.Duration{
		5 * time.Microsecond,
		20 * time.Microsecond,
		75 * time.Microsecond,
		2 * time.Millisecond,
		20 * time.Millisecond,
	} {
		histogram.observe(latency)
	}
	got := histogram.snapshot()
	if got.Count != 5 || got.P50 != 100*time.Microsecond || got.P95 != 25*time.Millisecond || got.P99 != 25*time.Millisecond || got.Max != 20*time.Millisecond {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
}

func TestCommitLatencyEmptySnapshot(t *testing.T) {
	var histogram commitLatencyHistogram
	if got := histogram.snapshot(); got != (CommitLatencySnapshot{}) {
		t.Fatalf("unexpected empty snapshot: %+v", got)
	}
}

func TestCommitLatencyCountsSuccessfulConcurrentCommits(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	db := openTestDB(t, opt)
	defer db.Close()

	const writers = 32
	var wait sync.WaitGroup
	wait.Add(writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wait.Done()
			if err := db.Update(func(tx *Tx) error {
				return tx.Put("latency", []byte{byte(i)}, []byte("value"), Persistent)
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	got := db.CommitLatency()
	if got.Count != writers || got.P50 == 0 || got.P99 == 0 || got.Max == 0 {
		t.Fatalf("unexpected latency snapshot: %+v", got)
	}
}

func TestCompactionRecommendationUsesObservedP99(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = time.Nanosecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 100
	opt.Compaction.ControllerStableWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "latency", []byte("key"), []byte("value"), Persistent)
	got := db.RecommendCompaction(0)
	if got.Reason != "foreground p99 exceeds target" || got.BudgetBytes != 0 {
		t.Fatalf("observed p99 was not used: %+v latency=%+v", got, db.CommitLatency())
	}
}

func BenchmarkCommitLatencyObservation(b *testing.B) {
	var histogram commitLatencyHistogram
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		histogram.observe(100 * time.Microsecond)
	}
}

func BenchmarkCommitLatencySnapshot(b *testing.B) {
	var histogram commitLatencyHistogram
	for i := 0; i < 1000; i++ {
		histogram.observe(time.Duration(i+1) * time.Microsecond)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = histogram.snapshot()
	}
}
