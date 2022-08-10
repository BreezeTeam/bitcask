package adaptivesync

import (
	"testing"
	"time"
)

func TestAdaptiveSyncForcesSyncAtDirtyBytesLimit(t *testing.T) {
	policy := Policy{DirtyBytesLimit: 1024, MaxDelay: time.Second}
	decision := policy.Decide(Observation{DirtyBytes: 1024})
	if !decision.ShouldSync {
		t.Fatalf("expected sync at dirty bytes limit")
	}
}

func TestAdaptiveSyncForcesSyncAtMaxDelay(t *testing.T) {
	policy := Policy{DirtyBytesLimit: 1024, MaxDelay: time.Second}
	decision := policy.Decide(Observation{DirtyBytes: 128, SinceLastSync: time.Second})
	if !decision.ShouldSync {
		t.Fatalf("expected sync at max delay")
	}
}

func TestAdaptiveSyncAllowsDelayBelowThreshold(t *testing.T) {
	policy := Policy{DirtyBytesLimit: 1024, MinDelay: time.Millisecond, MaxDelay: time.Second, TargetSyncLatency: 5 * time.Millisecond}
	decision := policy.Decide(Observation{DirtyBytes: 128, LastSyncLatency: 10 * time.Millisecond})
	if decision.ShouldSync || decision.Delay == 0 {
		t.Fatalf("expected delay below threshold, got %#v", decision)
	}
}

func TestAdaptiveSyncBoundsDelay(t *testing.T) {
	policy := Policy{DirtyBytesLimit: 1024, MinDelay: 2 * time.Second, MaxDelay: time.Second, TargetSyncLatency: time.Millisecond}
	decision := policy.Decide(Observation{DirtyBytes: 128, LastSyncLatency: 2 * time.Millisecond})
	if decision.Delay != time.Second {
		t.Fatalf("got %s want %s", decision.Delay, time.Second)
	}
}
