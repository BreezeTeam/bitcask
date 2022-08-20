package compaction

import (
	"testing"
	"time"
)

func TestControllerEvaluateCompactionPressure(t *testing.T) {
	controller := NewController(DefaultControllerConfig())
	tests := []struct {
		name       string
		obs        ControllerObservation
		wantBudget bool
		wantPicker PickerMode
		wantReason string
	}{
		{
			name:       "latency-throttle",
			obs:        ControllerObservation{ForegroundP99: 20 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20},
			wantPicker: PickerFileID, wantReason: "foreground p99 exceeds target",
		},
		{
			name:       "emergency-space",
			obs:        ControllerObservation{ForegroundP99: 20 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20, SpaceAmplification: 2.5},
			wantBudget: true, wantPicker: PickerGarbageRatio, wantReason: "emergency space amplification",
		},
		{
			name:       "garbage-pressure",
			obs:        ControllerObservation{ForegroundP99: 5 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 192 << 20, DataBytes: 256 << 20, Phase: PhaseOverwriteHeavy},
			wantBudget: true, wantPicker: PickerGarbageRatio, wantReason: "garbage pressure within latency budget",
		},
		{
			name:       "read-heavy-hot-cold",
			obs:        ControllerObservation{ForegroundP99: 5 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20, Phase: PhaseReadHeavy},
			wantBudget: true, wantPicker: PickerHotCold, wantReason: "garbage pressure within latency budget",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := controller.Evaluate(tt.obs)
			if (got.BudgetBytes > 0) != tt.wantBudget || got.Picker != tt.wantPicker || got.Reason != tt.wantReason {
				t.Fatalf("unexpected decision: %+v", got)
			}
			if got.Confidence < 0 || got.Confidence > 1 {
				t.Fatalf("confidence outside [0,1]: %f", got.Confidence)
			}
		})
	}
}

func TestControllerLimitsBudgetForHighWriteAmplification(t *testing.T) {
	config := DefaultControllerConfig()
	controller := NewController(config)
	got := controller.Evaluate(ControllerObservation{
		ForegroundP99:       5 * time.Millisecond,
		TargetP99:           10 * time.Millisecond,
		PendingGarbageBytes: 128 << 20,
		DataBytes:           256 << 20,
		WriteAmplification:  config.HighWriteAmp,
	})
	if got.BudgetBytes != config.BaseBudgetBytes/2 {
		t.Fatalf("budget got %d want %d", got.BudgetBytes, config.BaseBudgetBytes/2)
	}
}

func TestControllerNoiseDoesNotChangeBudget(t *testing.T) {
	config := DefaultControllerConfig()
	config.StableWindows = 2
	config.CooldownWindows = 2
	controller := NewController(config)
	work := ControllerObservation{ForegroundP99: 5 * time.Millisecond, TargetP99: 10 * time.Millisecond, PendingGarbageBytes: 128 << 20, DataBytes: 256 << 20}
	throttle := work
	throttle.ForegroundP99 = 20 * time.Millisecond

	controller.Observe(work)
	if got := controller.Observe(work); got.BudgetBytes == 0 || !got.Changed {
		t.Fatalf("initial work budget not activated: %+v", got)
	}
	for i := 0; i < 8; i++ {
		obs := throttle
		if i%2 == 1 {
			obs = work
		}
		got := controller.Observe(obs)
		if got.BudgetBytes == 0 || got.Changed {
			t.Fatalf("noise changed budget at %d: %+v", i, got)
		}
	}
}

func TestControllerUsesBoundedBudgetSteps(t *testing.T) {
	config := DefaultControllerConfig()
	config.StableWindows = 1
	config.CooldownWindows = 0
	controller := NewController(config)
	obs := ControllerObservation{PendingGarbageBytes: 192 << 20, DataBytes: 256 << 20, TargetP99: 10 * time.Millisecond, ForegroundP99: time.Millisecond}
	got := controller.Observe(obs)
	if got.BudgetBytes != config.BaseBudgetBytes {
		t.Fatalf("first bounded step got %d want %d", got.BudgetBytes, config.BaseBudgetBytes)
	}
}
