package bitcask

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func BenchmarkSegmentedValueLogAppendRead(b *testing.B) {
	for _, segmentSize := range []int64{0, 64 * 1024} {
		segmentSize := segmentSize
		b.Run(fmt.Sprintf("segment=%d", segmentSize), func(b *testing.B) {
			valueLog, err := openValueLog(b.TempDir(), segmentSize)
			if err != nil {
				b.Fatal(err)
			}
			defer valueLog.Close()
			value := make([]byte, 4096)
			b.SetBytes(int64(len(value)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ptr, err := valueLog.Append(value)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := valueLog.Read(ptr); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAdaptiveSyncDelayedWrite(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 256 * 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.DirtyCommitsLimit = 64
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = time.Second
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("bench", []byte(fmt.Sprintf("key-%09d", i)), value, Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	metrics := db.Metrics()
	if metrics.Commits > 0 {
		b.ReportMetric(float64(metrics.Syncs)/float64(metrics.Commits), "syncs/commit")
	}
}

func BenchmarkGroupCommitDurabilityResources(b *testing.B) {
	for _, separated := range []bool{false, true} {
		separated := separated
		b.Run(fmt.Sprintf("separated=%t", separated), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 128 * 1024
			opt.SyncPolicy.Mode = SyncPolicyGroupCommit
			opt.SyncPolicy.GroupMaxWrites = 8
			opt.SyncPolicy.GroupMaxDelay = time.Millisecond
			opt.KVSeparation.Enable = separated
			opt.KVSeparation.Threshold = 16
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			value := make([]byte, 1024)
			b.SetBytes(int64(len(value)))
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := []byte(fmt.Sprintf("key-%d", time.Now().UnixNano()))
					if err := db.Update(func(tx *Tx) error {
						return tx.Put("bench", key, value, Persistent)
					}); err != nil {
						b.Error(err)
					}
				}
			})
			metrics := db.Metrics()
			if metrics.Commits > 0 {
				b.ReportMetric(float64(metrics.Syncs)/float64(metrics.Commits), "syncs/commit")
			}
		})
	}
}

func BenchmarkConcurrentCommitPolicies(b *testing.B) {
	policies := []struct {
		name string
		mode SyncPolicyMode
	}{
		{name: "none", mode: SyncPolicyNone},
		{name: "every", mode: SyncPolicyEveryCommit},
		{name: "group", mode: SyncPolicyGroupCommit},
	}
	for _, policy := range policies {
		policy := policy
		for _, writers := range []int{1, 4, 16} {
			writers := writers
			b.Run(fmt.Sprintf("policy=%s/writers=%d", policy.name, writers), func(b *testing.B) {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 256 * 1024 * 1024
				opt.SyncPolicy.Mode = policy.mode
				opt.SyncPolicy.GroupMaxDelay = time.Millisecond
				opt.SyncPolicy.GroupMaxWrites = writers
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				latencies := make([]time.Duration, b.N)
				jobs := make(chan int)
				var workers sync.WaitGroup
				workers.Add(writers)
				b.ResetTimer()
				for w := 0; w < writers; w++ {
					go func() {
						defer workers.Done()
						for i := range jobs {
							start := time.Now()
							err := db.Update(func(tx *Tx) error {
								return tx.Put("bench", []byte(fmt.Sprintf("key-%09d", i)), []byte("value"), Persistent)
							})
							latencies[i] = time.Since(start)
							if err != nil {
								b.Error(err)
							}
						}
					}()
				}
				for i := 0; i < b.N; i++ {
					jobs <- i
				}
				close(jobs)
				workers.Wait()
				b.StopTimer()
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				if len(latencies) > 0 {
					b.ReportMetric(float64(latencies[len(latencies)/2].Nanoseconds()), "p50-ns")
					b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds()), "p99-ns")
				}
				metrics := db.Metrics()
				if metrics.Commits > 0 {
					b.ReportMetric(float64(metrics.Syncs)/float64(metrics.Commits), "syncs/commit")
				}
			})
		}
	}
}
