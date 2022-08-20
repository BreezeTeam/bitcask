package bitcask

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestAutonomousRecommendationsDisabledByDefault(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "auto", []byte("key"), []byte("value"), Persistent)
	if got := db.AutonomousObservation(); got != (AutonomousObservation{}) {
		t.Fatalf("unexpected observation: %+v", got)
	}
	if got := db.PolicyRecommendation(); got != (PolicyRecommendation{}) {
		t.Fatalf("unexpected recommendation: %+v", got)
	}
}

func TestAutonomousObservationCountsWorkload(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 100
	opt.Autonomous.LargeValueThreshold = 16
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "auto", []byte("key"), []byte("small"), Persistent)
	putValue(t, db, "auto", []byte("key"), []byte("large-value-over-threshold"), Persistent)
	assertValue(t, db, "auto", []byte("key"), []byte("large-value-over-threshold"))
	got := db.AutonomousObservation()
	if got.Reads != 1 || got.Writes != 2 || got.Overwrites != 1 || got.LargeValueWrites != 1 {
		t.Fatalf("unexpected observation: %+v", got)
	}
}

func TestAutonomousRecommendationClassifiesWriteHeavyWindow(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()

	for i := 0; i < 10; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
	}
	got := db.PolicyRecommendation()
	if !got.Available || got.Phase != "write-heavy" || got.Sync != "group" {
		t.Fatalf("unexpected recommendation: %+v", got)
	}
}

func TestAutonomousRecommendationWaitsForMinimumWindow(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()

	for i := 0; i < 9; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
	}
	if got := db.PolicyRecommendation(); got.Available {
		t.Fatalf("recommendation available too early: %+v", got)
	}
}

func TestAutonomousRecommendationUsesWindowedSyncLatency(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	opt.Autonomous.CooldownWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()

	atomic.StoreUint64(&db.metrics.syncs, 1)
	atomic.StoreUint64(&db.metrics.totalSyncNanos, uint64((20 * time.Millisecond).Nanoseconds()))
	for i := 0; i < 10; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("slow-%02d", i)), []byte("value"), Persistent)
	}
	first := db.PolicyRecommendation()
	if first.Phase != "sync-latency-sensitive" {
		t.Fatalf("first window did not see high sync latency: %+v", first)
	}

	for i := 0; i < 10; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("fast-%02d", i)), []byte("value"), Persistent)
	}
	second := db.PolicyRecommendation()
	if second.Phase == "sync-latency-sensitive" {
		t.Fatalf("stale cumulative sync latency leaked into next window: %+v", second)
	}
}

func BenchmarkAutonomousWindowRecommendation(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 1
	opt.Autonomous.MinOperations = 1
	opt.Autonomous.ConsecutiveWindows = 1
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("bench", []byte(fmt.Sprintf("key-%09d", i)), []byte("value"), Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAutonomousObservationOverhead(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		enabled := enabled
		b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 128 * 1024 * 1024
			opt.Autonomous.EnableRecommendations = enabled
			opt.Autonomous.WindowOperations = 100
			opt.Autonomous.MinOperations = 100
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error {
					return tx.Put("bench", []byte(fmt.Sprintf("key-%09d", i)), []byte("value"), Persistent)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
