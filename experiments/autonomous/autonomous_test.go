package autonomous

import (
	"testing"
	"time"
)

func TestAnalyzeClassifiesSyntheticPhases(t *testing.T) {
	detector := NewDetector(DefaultConfig())
	tests := []struct {
		name string
		obs  Observation
		want Phase
	}{
		{name: "write-heavy", obs: Observation{Reads: 20, Writes: 80}, want: PhaseWriteHeavy},
		{name: "read-heavy", obs: Observation{Reads: 80, Writes: 20}, want: PhaseReadHeavy},
		{name: "overwrite-heavy", obs: Observation{Reads: 20, Writes: 80, Overwrites: 50}, want: PhaseOverwriteHeavy},
		{name: "large-value-heavy", obs: Observation{Reads: 20, Writes: 80, LargeValueWrites: 50}, want: PhaseLargeValueHeavy},
		{name: "sync-sensitive", obs: Observation{Reads: 20, Writes: 80, SyncLatency: 10 * time.Millisecond}, want: PhaseSyncLatencySensitive},
		{name: "mixed", obs: Observation{Reads: 50, Writes: 50}, want: PhaseMixed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detector.Analyze(tt.obs)
			if got.Phase != tt.want {
				t.Fatalf("got %q want %q", got.Phase, tt.want)
			}
			if got.Confidence <= 0 || got.Confidence > 1 {
				t.Fatalf("confidence %f outside (0,1]", got.Confidence)
			}
			if got.Reason == "" {
				t.Fatal("missing reason")
			}
		})
	}
}

func TestAnalyzeRequiresMinimumSamples(t *testing.T) {
	detector := NewDetector(DefaultConfig())
	got := detector.Analyze(Observation{Writes: 99})
	if got.Phase != PhaseUnknown {
		t.Fatalf("got %q want %q", got.Phase, PhaseUnknown)
	}
}

func TestDetectorRequiresConsecutiveWindows(t *testing.T) {
	config := DefaultConfig()
	config.ConsecutiveWindows = 2
	config.CooldownWindows = 0
	detector := NewDetector(config)
	writeHeavy := Observation{Reads: 20, Writes: 80}

	first := detector.Observe(writeHeavy)
	if first.Phase != PhaseUnknown || first.Changed {
		t.Fatalf("first window changed phase: %+v", first)
	}
	second := detector.Observe(writeHeavy)
	if second.Phase != PhaseWriteHeavy || !second.Changed {
		t.Fatalf("second window did not change phase: %+v", second)
	}
	if second.Sync != SyncGroup || second.Compaction != CompactionGarbageRatio {
		t.Fatalf("unexpected recommendation: %+v", second)
	}
}

func TestDetectorNoiseDoesNotFlap(t *testing.T) {
	config := DefaultConfig()
	config.ConsecutiveWindows = 2
	config.CooldownWindows = 2
	detector := NewDetector(config)

	writeHeavy := Observation{Reads: 20, Writes: 80}
	readHeavy := Observation{Reads: 80, Writes: 20}
	detector.Observe(writeHeavy)
	if got := detector.Observe(writeHeavy); got.Phase != PhaseWriteHeavy {
		t.Fatalf("initial phase got %q", got.Phase)
	}

	for i := 0; i < 8; i++ {
		obs := readHeavy
		if i%2 == 1 {
			obs = writeHeavy
		}
		got := detector.Observe(obs)
		if got.Phase != PhaseWriteHeavy || got.Changed {
			t.Fatalf("phase flapped at window %d: %+v", i, got)
		}
	}
}

func TestDetectorTransitionsAfterCooldown(t *testing.T) {
	config := DefaultConfig()
	config.ConsecutiveWindows = 2
	config.CooldownWindows = 3
	detector := NewDetector(config)
	writeHeavy := Observation{Reads: 20, Writes: 80}
	readHeavy := Observation{Reads: 80, Writes: 20}

	detector.Observe(writeHeavy)
	detector.Observe(writeHeavy)
	if got := detector.Observe(readHeavy); got.Phase != PhaseWriteHeavy {
		t.Fatalf("changed before consecutive windows: %+v", got)
	}
	if got := detector.Observe(readHeavy); got.Phase != PhaseWriteHeavy {
		t.Fatalf("changed during cooldown: %+v", got)
	}
	if got := detector.Observe(readHeavy); got.Phase != PhaseReadHeavy || !got.Changed {
		t.Fatalf("did not change after cooldown: %+v", got)
	}
}

func TestUnknownWindowMakesNoRecommendation(t *testing.T) {
	detector := NewDetector(DefaultConfig())
	got := detector.Observe(Observation{Writes: 1})
	if got.Phase != PhaseUnknown || got.Sync != SyncNoChange || got.Compaction != CompactionNoChange || got.Placement != PlacementNoChange {
		t.Fatalf("unexpected unknown recommendation: %+v", got)
	}
}
