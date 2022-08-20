package kvseparation

import "testing"

func BenchmarkLifecyclePolicyDecide(b *testing.B) {
	policy := NewLifecyclePolicy(DefaultLifecycleConfig())
	observations := []LifecycleObservation{
		{ValueSize: 32, ReadCount: 10},
		{ValueSize: 4096, ReadCount: 10},
		{ValueSize: 4096, UpdateCount: 5},
		{ValueSize: 4096, ReadCount: 2, UpdateCount: 2},
		{ValueSize: 128 * 1024, ReadCount: 2, UpdateCount: 2, AgeWindows: 10},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = policy.Decide(observations[i%len(observations)])
	}
}

func BenchmarkValueLogStats(b *testing.B) {
	store := NewStore()
	value := make([]byte, 4096)
	for i := 0; i < 1024; i++ {
		store.Put([]byte{byte(i >> 8), byte(i)}, value)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = store.Stats()
	}
}
