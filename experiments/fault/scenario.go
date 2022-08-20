package fault

import (
	"fmt"
	"sort"
)

type OperationKind string

const (
	OperationPut    OperationKind = "put"
	OperationDelete OperationKind = "delete"
)

type ScenarioOperation struct {
	Kind      OperationKind
	TxID      uint64
	Key       string
	Value     string
	Committed bool
	ExpiresAt uint64
}

type Scenario struct {
	Name       string
	FaultPoint string
	Operations []ScenarioOperation
}

type ExpectedValue struct {
	Value string
	Found bool
}

type TraceEvent struct {
	Index     int
	Operation ScenarioOperation
	Applied   bool
	Reason    string
}

type ReplayResult struct {
	ScenarioID string
	Expected   map[string]ExpectedValue
	Trace      []TraceEvent
}

type TornWriteScenario struct {
	Region string
	Offset int
	Size   int
}

type ManifestKind string

type ManifestPhase string

type ManifestMetadataStage string

type ManifestRecoveryAction string

const (
	ManifestMerge   ManifestKind = "merge"
	ManifestValueGC ManifestKind = "value-gc"

	ManifestPhasePrepared          ManifestPhase = "prepared"
	ManifestPhaseInstalled         ManifestPhase = "installed"
	ManifestPhasePointersInstalled ManifestPhase = "pointers-installed"
	ManifestPhaseSourceRemoved     ManifestPhase = "source-removed"
	ManifestPhaseFinalized         ManifestPhase = "finalized"

	ManifestStageTempWrite ManifestMetadataStage = "temp-write"
	ManifestStageFileSync  ManifestMetadataStage = "file-sync"
	ManifestStageRename    ManifestMetadataStage = "rename"
	ManifestStageDirSync   ManifestMetadataStage = "directory-sync"

	ManifestActionIgnoreAttempt ManifestRecoveryAction = "ignore-attempt"
	ManifestActionRetainSource  ManifestRecoveryAction = "retain-source"
	ManifestActionRemoveSource  ManifestRecoveryAction = "remove-source"
	ManifestActionClearManifest ManifestRecoveryAction = "clear-manifest"
)

type ManifestFaultScenario struct {
	Kind       ManifestKind
	Phase      ManifestPhase
	Stage      ManifestMetadataStage
	Occurrence int
}

func (s TornWriteScenario) ID() string {
	return fmt.Sprintf("recovery/torn-write/region=%s/offset=%d/size=%d", s.Region, s.Offset, s.Size)
}

func (s ManifestFaultScenario) ID() string {
	return fmt.Sprintf("manifest/%s/phase=%s/stage=%s/occurrence=%d", s.Kind, s.Phase, s.Stage, s.Occurrence)
}

func (s ManifestFaultScenario) ExpectedRecoveryAction() ManifestRecoveryAction {
	switch s.Stage {
	case ManifestStageTempWrite, ManifestStageFileSync:
		return ManifestActionIgnoreAttempt
	case ManifestStageRename:
		return ManifestActionRetainSource
	case ManifestStageDirSync:
		switch s.Phase {
		case ManifestPhaseInstalled, ManifestPhasePointersInstalled:
			return ManifestActionRemoveSource
		case ManifestPhaseSourceRemoved, ManifestPhaseFinalized:
			return ManifestActionClearManifest
		default:
			return ManifestActionRetainSource
		}
	default:
		return ManifestActionRetainSource
	}
}

func (s Scenario) ID() string {
	return fmt.Sprintf("recovery/%s/fault=%s", s.Name, s.FaultPoint)
}

func ReplayScenario(s Scenario, now uint64) ReplayResult {
	committed := make(map[uint64]bool)
	for _, operation := range s.Operations {
		if operation.Committed {
			committed[operation.TxID] = true
		}
	}

	result := ReplayResult{
		ScenarioID: s.ID(),
		Expected:   make(map[string]ExpectedValue),
		Trace:      make([]TraceEvent, 0, len(s.Operations)),
	}
	for i, operation := range s.Operations {
		event := TraceEvent{Index: i, Operation: operation}
		if !committed[operation.TxID] {
			event.Reason = "transaction has no commit marker"
			result.Trace = append(result.Trace, event)
			continue
		}
		event.Applied = true
		switch {
		case operation.Kind == OperationDelete:
			result.Expected[operation.Key] = ExpectedValue{}
			event.Reason = "committed tombstone"
		case operation.ExpiresAt > 0 && operation.ExpiresAt <= now:
			result.Expected[operation.Key] = ExpectedValue{}
			event.Reason = "committed value expired"
		default:
			result.Expected[operation.Key] = ExpectedValue{Value: operation.Value, Found: true}
			event.Reason = "latest committed value"
		}
		result.Trace = append(result.Trace, event)
	}
	return result
}

func EnumerateTornWriteScenarios(headerSize, bucketSize, keySize, valueSize int) []TornWriteScenario {
	regions := []struct {
		name string
		size int
	}{
		{name: "header", size: headerSize},
		{name: "bucket", size: bucketSize},
		{name: "key", size: keySize},
		{name: "value", size: valueSize},
	}
	var scenarios []TornWriteScenario
	base := 0
	for _, region := range regions {
		for offset := 0; offset < region.size; offset++ {
			scenarios = append(scenarios, TornWriteScenario{Region: region.name, Offset: base + offset, Size: base + region.size})
		}
		base += region.size
	}
	return scenarios
}

func EnumerateManifestFaultScenarios() []ManifestFaultScenario {
	writes := []struct {
		kind       ManifestKind
		phase      ManifestPhase
		occurrence int
	}{
		{kind: ManifestMerge, phase: ManifestPhasePrepared, occurrence: 1},
		{kind: ManifestMerge, phase: ManifestPhaseInstalled, occurrence: 2},
		{kind: ManifestValueGC, phase: ManifestPhasePrepared, occurrence: 1},
		{kind: ManifestValueGC, phase: ManifestPhasePrepared, occurrence: 2},
		{kind: ManifestValueGC, phase: ManifestPhasePointersInstalled, occurrence: 3},
		{kind: ManifestValueGC, phase: ManifestPhaseSourceRemoved, occurrence: 4},
		{kind: ManifestValueGC, phase: ManifestPhaseFinalized, occurrence: 5},
	}
	stages := []ManifestMetadataStage{
		ManifestStageTempWrite,
		ManifestStageFileSync,
		ManifestStageRename,
		ManifestStageDirSync,
	}
	var scenarios []ManifestFaultScenario
	for _, write := range writes {
		for _, stage := range stages {
			scenarios = append(scenarios, ManifestFaultScenario{
				Kind:       write.kind,
				Phase:      write.phase,
				Stage:      stage,
				Occurrence: write.occurrence,
			})
		}
	}
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID() < scenarios[j].ID() })
	return scenarios
}

func EnumerateRecoveryScenarios() []Scenario {
	scenarios := []Scenario{
		{
			Name:       "before-first-write",
			FaultPoint: "before-first-write",
		},
		{
			Name:       "before-commit-marker",
			FaultPoint: "before-commit-marker",
			Operations: []ScenarioOperation{
				{Kind: OperationPut, TxID: 1, Key: "partial", Value: "hidden"},
			},
		},
		{
			Name:       "after-commit-marker",
			FaultPoint: "after-commit-marker",
			Operations: []ScenarioOperation{
				{Kind: OperationPut, TxID: 1, Key: "first", Value: "v1"},
				{Kind: OperationPut, TxID: 1, Key: "second", Value: "v2", Committed: true},
			},
		},
		{
			Name:       "uncommitted-newer-overwrite",
			FaultPoint: "before-second-commit-marker",
			Operations: []ScenarioOperation{
				{Kind: OperationPut, TxID: 1, Key: "key", Value: "old", Committed: true},
				{Kind: OperationPut, TxID: 2, Key: "key", Value: "new"},
			},
		},
		{
			Name:       "committed-tombstone",
			FaultPoint: "after-delete-marker",
			Operations: []ScenarioOperation{
				{Kind: OperationPut, TxID: 1, Key: "key", Value: "old", Committed: true},
				{Kind: OperationDelete, TxID: 2, Key: "key", Committed: true},
			},
		},
		{
			Name:       "expired-value",
			FaultPoint: "after-expired-marker",
			Operations: []ScenarioOperation{
				{Kind: OperationPut, TxID: 1, Key: "expired", Value: "old", Committed: true, ExpiresAt: 10},
			},
		},
	}
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID() < scenarios[j].ID() })
	return scenarios
}
