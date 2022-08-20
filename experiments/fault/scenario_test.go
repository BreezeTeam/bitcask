package fault

import "testing"

func TestScenarioIDIsStable(t *testing.T) {
	scenario := Scenario{Name: "batch-two", FaultPoint: "before-commit-marker"}
	if got, want := scenario.ID(), "recovery/batch-two/fault=before-commit-marker"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestReplayScenarioAppliesOnlyCommittedTransactions(t *testing.T) {
	scenario := Scenario{
		Name:       "latest-committed",
		FaultPoint: "before-second-commit-marker",
		Operations: []ScenarioOperation{
			{Kind: OperationPut, TxID: 1, Key: "key", Value: "old", Committed: true},
			{Kind: OperationPut, TxID: 2, Key: "key", Value: "new"},
		},
	}
	got := ReplayScenario(scenario, 100)
	if value := got.Expected["key"]; !value.Found || value.Value != "old" {
		t.Fatalf("unexpected expected state: %+v", got.Expected)
	}
	if got.Trace[1].Applied || got.Trace[1].Reason == "" {
		t.Fatalf("unexpected uncommitted trace: %+v", got.Trace[1])
	}
}

func TestReplayScenarioRespectsTombstoneAndTTL(t *testing.T) {
	scenario := Scenario{
		Name:       "visibility",
		FaultPoint: "after-markers",
		Operations: []ScenarioOperation{
			{Kind: OperationPut, TxID: 1, Key: "deleted", Value: "value", Committed: true},
			{Kind: OperationDelete, TxID: 2, Key: "deleted", Committed: true},
			{Kind: OperationPut, TxID: 3, Key: "expired", Value: "value", Committed: true, ExpiresAt: 10},
			{Kind: OperationPut, TxID: 4, Key: "live", Value: "value", Committed: true, ExpiresAt: 200},
		},
	}
	got := ReplayScenario(scenario, 100)
	if got.Expected["deleted"].Found || got.Expected["expired"].Found {
		t.Fatalf("deleted or expired value is visible: %+v", got.Expected)
	}
	if value := got.Expected["live"]; !value.Found || value.Value != "value" {
		t.Fatalf("live value missing: %+v", got.Expected)
	}
}

func TestEnumerateTornWriteScenariosCoversEveryByte(t *testing.T) {
	scenarios := EnumerateTornWriteScenarios(4, 2, 3, 5)
	if len(scenarios) != 14 {
		t.Fatalf("got %d scenarios want 14", len(scenarios))
	}
	seen := make(map[string]bool)
	for _, scenario := range scenarios {
		if seen[scenario.ID()] {
			t.Fatalf("duplicate id %q", scenario.ID())
		}
		seen[scenario.ID()] = true
	}
	if scenarios[0].Region != "header" || scenarios[4].Region != "bucket" || scenarios[6].Region != "key" || scenarios[9].Region != "value" {
		t.Fatalf("unexpected region boundaries: %+v", scenarios)
	}
}

func TestManifestFaultScenarioIDAndRecoveryAction(t *testing.T) {
	scenario := ManifestFaultScenario{Kind: ManifestValueGC, Phase: ManifestPhasePointersInstalled, Stage: ManifestStageDirSync, Occurrence: 3}
	if got, want := scenario.ID(), "manifest/value-gc/phase=pointers-installed/stage=directory-sync/occurrence=3"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := scenario.ExpectedRecoveryAction(); got != ManifestActionRemoveSource {
		t.Fatalf("got action %q want %q", got, ManifestActionRemoveSource)
	}
	prepared := ManifestFaultScenario{Kind: ManifestMerge, Phase: ManifestPhasePrepared, Stage: ManifestStageDirSync}
	if got := prepared.ExpectedRecoveryAction(); got != ManifestActionRetainSource {
		t.Fatalf("prepared dir-sync action %q want %q", got, ManifestActionRetainSource)
	}
	tempWrite := ManifestFaultScenario{Kind: ManifestValueGC, Phase: ManifestPhaseFinalized, Stage: ManifestStageTempWrite}
	if got := tempWrite.ExpectedRecoveryAction(); got != ManifestActionIgnoreAttempt {
		t.Fatalf("temp-write action %q want %q", got, ManifestActionIgnoreAttempt)
	}
}

func TestEnumerateManifestFaultScenariosHasUniqueStableIDs(t *testing.T) {
	scenarios := EnumerateManifestFaultScenarios()
	if len(scenarios) != 28 {
		t.Fatalf("got %d scenarios want 28", len(scenarios))
	}
	seen := make(map[string]bool)
	last := ""
	for _, scenario := range scenarios {
		id := scenario.ID()
		if seen[id] {
			t.Fatalf("duplicate manifest scenario id %q", id)
		}
		if last != "" && id < last {
			t.Fatalf("manifest scenario ids are not sorted: %q before %q", last, id)
		}
		if scenario.Occurrence <= 0 {
			t.Fatalf("non-positive occurrence: %+v", scenario)
		}
		if scenario.Phase == ManifestPhasePrepared && scenario.ExpectedRecoveryAction() == ManifestActionRemoveSource {
			t.Fatalf("prepared scenario can remove source: %+v", scenario)
		}
		seen[id] = true
		last = id
	}
}

func TestEnumerateRecoveryScenariosHasUniqueStableIDs(t *testing.T) {
	scenarios := EnumerateRecoveryScenarios()
	if len(scenarios) < 6 {
		t.Fatalf("got %d scenarios", len(scenarios))
	}
	seen := make(map[string]bool)
	last := ""
	for _, scenario := range scenarios {
		id := scenario.ID()
		if seen[id] {
			t.Fatalf("duplicate scenario id %q", id)
		}
		if last != "" && id < last {
			t.Fatalf("scenario ids are not sorted: %q before %q", last, id)
		}
		seen[id] = true
		last = id
	}
}
