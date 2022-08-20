package bitcask

import "testing"

func TestAutonomousPhaseTransitionsRemainCorrect(t *testing.T) {
	result := runAutonomousPhases(t, true, 16)
	if !result.Recommendation.Available || result.Recommendation.Phase != "large-value-heavy" {
		t.Fatalf("unexpected final recommendation: %+v", result.Recommendation)
	}
	if result.AuditEvents < 5 {
		t.Fatalf("audit events got %d want at least 5", result.AuditEvents)
	}
	if result.Latency.Count != 16*3+16 {
		t.Fatalf("commit count got %d want %d", result.Latency.Count, 64)
	}
}

func TestStaticPhaseWorkloadDoesNotChangePolicy(t *testing.T) {
	result := runAutonomousPhases(t, false, 16)
	if result.Recommendation.Available || result.AuditEvents != 0 || result.CompactionMode != CompactionByFileID {
		t.Fatalf("static workload changed policy: %+v", result)
	}
}
