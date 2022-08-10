package adaptivesync

import (
	"testing"
	"time"
)

func BenchmarkAdaptiveSyncDecision(b *testing.B) {
	policy := Policy{DirtyBytesLimit: 64 * 1024, MinDelay: 100 * time.Microsecond, MaxDelay: 5 * time.Millisecond, TargetSyncLatency: time.Millisecond}
	obs := Observation{DirtyBytes: 1024, Commits: 1, LastSyncLatency: 2 * time.Millisecond, SinceLastSync: 50 * time.Microsecond}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = policy.Decide(obs)
	}
}

func BenchmarkAdaptiveSyncWorkloadModel(b *testing.B) {
	policy := Policy{DirtyBytesLimit: 64 * 1024, MinDelay: 100 * time.Microsecond, MaxDelay: 5 * time.Millisecond, TargetSyncLatency: time.Millisecond}
	var syncs int
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		obs := Observation{DirtyBytes: int64((i % 128) * 1024), Commits: i % 64, LastSyncLatency: time.Duration(i%4) * time.Millisecond, SinceLastSync: time.Duration(i%10) * time.Millisecond}
		if policy.Decide(obs).ShouldSync {
			syncs++
		}
	}
	_ = syncs
}
