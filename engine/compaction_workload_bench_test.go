package bitcask

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func BenchmarkCompactionForegroundImpact(b *testing.B) {
	modes := []struct {
		name   string
		budget int64
		merge  bool
	}{
		{name: "no-merge"},
		{name: "static-merge", merge: true},
		{name: "budgeted-merge", merge: true, budget: 8 * 1024},
	}
	for _, mode := range modes {
		mode := mode
		b.Run(mode.name, func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 4 * 1024
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			value := make([]byte, 128)
			for i := 0; i < 256; i++ {
				key := []byte(fmt.Sprintf("key-%03d", i%64))
				if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
					b.Fatal(err)
				}
			}
			latencies := make([]time.Duration, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				key := []byte(fmt.Sprintf("foreground-%09d", i))
				if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
					b.Fatal(err)
				}
				latencies[i] = time.Since(start)
				if mode.merge && i == b.N/2 {
					if mode.budget > 0 {
						err = db.MergeWithBudget(mode.budget)
					} else {
						err = db.Merge()
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			if len(latencies) > 0 {
				b.ReportMetric(float64(latencies[len(latencies)/2].Nanoseconds()), "p50-ns")
				b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds()), "p99-ns")
			}
			metrics := db.Metrics()
			b.ReportMetric(float64(metrics.MergeBytesWritten), "rewrite-B")
		})
	}
}
