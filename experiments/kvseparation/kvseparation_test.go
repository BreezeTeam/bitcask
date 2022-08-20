package kvseparation

import (
	"errors"
	"testing"
)

func TestStoreIndexesPointersIntoValueLog(t *testing.T) {
	store := NewStore()
	first := store.Put([]byte("key"), []byte("old"))
	second := store.Put([]byte("key"), []byte("new-value"))

	if first == second {
		t.Fatal("expected overwrite to create a new value-log pointer")
	}
	got, err := store.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new-value" {
		t.Fatalf("got %q want %q", got, "new-value")
	}
	pointers := store.LivePointers()
	if pointers["key"] != second {
		t.Fatalf("live pointer got %#v want %#v", pointers["key"], second)
	}
}

func TestStoreMissingKey(t *testing.T) {
	store := NewStore()
	if _, err := store.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v want %v", err, ErrNotFound)
	}
}

func TestStoreStatsTrackLiveAndStaleBytes(t *testing.T) {
	store := NewStore()
	store.Put([]byte("key"), []byte("old"))
	store.Put([]byte("key"), []byte("new-value"))
	store.Put([]byte("other"), []byte("live"))

	got := store.Stats()
	if got.TotalBytes != 16 || got.LiveBytes != 13 || got.StaleBytes != 3 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestLifecyclePolicyClassifiesValueLifecycle(t *testing.T) {
	tests := []struct {
		name string
		obs  LifecycleObservation
		want Placement
	}{
		{name: "small-inline", obs: LifecycleObservation{ValueSize: 32, ReadCount: 10}, want: PlacementInline},
		{name: "hot-large-inline", obs: LifecycleObservation{ValueSize: 4096, ReadCount: 10}, want: PlacementInline},
		{name: "frequently-updated-inline", obs: LifecycleObservation{ValueSize: 4096, UpdateCount: 5}, want: PlacementInline},
		{name: "stable-large-value-log", obs: LifecycleObservation{ValueSize: 4096, ReadCount: 2, UpdateCount: 2}, want: PlacementValueLog},
		{name: "large-old-cold-tier", obs: LifecycleObservation{ValueSize: 128 * 1024, ReadCount: 2, UpdateCount: 2, AgeWindows: 10}, want: PlacementColdTier},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := NewLifecyclePolicy(DefaultLifecycleConfig())
			got := policy.Decide(tt.obs)
			if got.Placement != tt.want {
				t.Fatalf("got %q want %q (%s)", got.Placement, tt.want, got.Reason)
			}
			if got.Reason == "" {
				t.Fatal("missing decision reason")
			}
		})
	}
}

func TestLifecyclePolicyFallsBackToSizeWithoutHistory(t *testing.T) {
	policy := NewLifecyclePolicy(DefaultLifecycleConfig())
	if got := policy.Decide(LifecycleObservation{ValueSize: 32}); got.Placement != PlacementInline {
		t.Fatalf("small fallback got %+v", got)
	}

	policy = NewLifecyclePolicy(DefaultLifecycleConfig())
	if got := policy.Decide(LifecycleObservation{ValueSize: 4096}); got.Placement != PlacementValueLog {
		t.Fatalf("large fallback got %+v", got)
	}
}

func TestLifecyclePolicyRequiresStablePlacementChange(t *testing.T) {
	config := DefaultLifecycleConfig()
	config.StableWindows = 2
	policy := NewLifecyclePolicy(config)
	largeStable := LifecycleObservation{ValueSize: 4096, ReadCount: 2, UpdateCount: 2}
	hotLarge := LifecycleObservation{ValueSize: 4096, ReadCount: 10}

	if got := policy.Decide(largeStable); got.Placement != PlacementValueLog {
		t.Fatalf("initial placement got %+v", got)
	}
	if got := policy.Decide(hotLarge); got.Placement != PlacementValueLog || got.Changed {
		t.Fatalf("changed after one window: %+v", got)
	}
	if got := policy.Decide(largeStable); got.Placement != PlacementValueLog || got.Changed {
		t.Fatalf("unstable candidate changed placement: %+v", got)
	}
	policy.Decide(hotLarge)
	if got := policy.Decide(hotLarge); got.Placement != PlacementInline || !got.Changed {
		t.Fatalf("stable candidate did not change placement: %+v", got)
	}
}
