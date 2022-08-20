package bitcask

import (
	compactionmodel "github.com/BreezeTeam/bitcask/experiments/compaction"
	"sync"
	"time"
)

type CompactionRecommendation struct {
	BudgetBytes int64
	Picker      CompactionPolicyMode
	Reason      string
	Confidence  float64
	Changed     bool
	Available   bool
}

type CompactionAuditEvent struct {
	Sequence          uint64
	Action            string
	BudgetBytes       int64
	Picker            CompactionPolicyMode
	Reason            string
	Confidence        float64
	Available         bool
	Executed          bool
	Error             string
	LogicalBytes      int64
	ObsoleteBytes     int64
	WriteAmp          float64
	SpaceAmp          float64
	ForegroundP99     time.Duration
	MergeBytesWritten uint64
}

type compactionRecommendationState struct {
	mu         sync.Mutex
	controller *compactionmodel.Controller
	last       CompactionRecommendation
	audit      []CompactionAuditEvent
	nextSeq    uint64
}

func newCompactionRecommendationState(options CompactionOptions) *compactionRecommendationState {
	config := compactionmodel.DefaultControllerConfig()
	auditCapacity := options.AuditCapacity
	if auditCapacity <= 0 {
		auditCapacity = 32
	}
	if options.MinGarbageBytes > 0 {
		config.MinGarbageBytes = options.MinGarbageBytes
	}
	if options.EmergencySpaceAmp > 0 {
		config.EmergencySpaceAmp = options.EmergencySpaceAmp
	}
	if options.HighWriteAmp > 0 {
		config.HighWriteAmp = options.HighWriteAmp
	}
	if options.BaseBudgetBytes > 0 {
		config.BaseBudgetBytes = options.BaseBudgetBytes
	}
	if options.MaxBudgetBytes > 0 {
		config.MaxBudgetBytes = options.MaxBudgetBytes
	}
	if options.ControllerStableWindows > 0 {
		config.StableWindows = options.ControllerStableWindows
	}
	if options.ControllerCooldownWindows > 0 {
		config.CooldownWindows = options.ControllerCooldownWindows
	}
	return &compactionRecommendationState{
		controller: compactionmodel.NewController(config),
		audit:      make([]CompactionAuditEvent, 0, auditCapacity),
	}
}

func (db *DB) RecommendCompaction(foregroundP99 time.Duration) CompactionRecommendation {
	if db.compactionRecommendation == nil {
		return CompactionRecommendation{}
	}
	if foregroundP99 <= 0 {
		foregroundP99 = db.CommitLatency().P99
	}
	observation := db.CompactionObservation()
	phase := compactionmodel.PhaseUnknown
	if recommendation := db.PolicyRecommendation(); recommendation.Available {
		switch recommendation.Phase {
		case "write-heavy":
			phase = compactionmodel.PhaseWriteHeavy
		case "read-heavy":
			phase = compactionmodel.PhaseReadHeavy
		case "overwrite-heavy":
			phase = compactionmodel.PhaseOverwriteHeavy
		case "mixed":
			phase = compactionmodel.PhaseMixed
		}
	}
	state := db.compactionRecommendation
	state.mu.Lock()
	defer state.mu.Unlock()
	decision := state.controller.Observe(compactionmodel.ControllerObservation{
		ForegroundP99:       foregroundP99,
		TargetP99:           db.opt.Compaction.TargetP99,
		PendingGarbageBytes: observation.ObsoleteBytes,
		DataBytes:           observation.LogicalBytes,
		WriteAmplification:  observation.WriteAmplification,
		SpaceAmplification:  observation.SpaceAmplification,
		Phase:               phase,
	})
	state.last = CompactionRecommendation{
		BudgetBytes: decision.BudgetBytes,
		Picker:      compactionPickerMode(decision.Picker),
		Reason:      decision.Reason,
		Confidence:  decision.Confidence,
		Changed:     decision.Changed,
		Available:   observation.LogicalBytes > 0,
	}
	state.appendAuditLocked(CompactionAuditEvent{
		Action:            "recommend",
		BudgetBytes:       state.last.BudgetBytes,
		Picker:            state.last.Picker,
		Reason:            state.last.Reason,
		Confidence:        state.last.Confidence,
		Available:         state.last.Available,
		LogicalBytes:      observation.LogicalBytes,
		ObsoleteBytes:     observation.ObsoleteBytes,
		WriteAmp:          observation.WriteAmplification,
		SpaceAmp:          observation.SpaceAmplification,
		ForegroundP99:     foregroundP99,
		MergeBytesWritten: observation.MergeBytesWritten,
	})
	return state.last
}

func (db *DB) MergeRecommended(foregroundP99 time.Duration) (CompactionRecommendation, error) {
	recommendation := db.RecommendCompaction(foregroundP99)
	if !recommendation.Available || recommendation.BudgetBytes <= 0 {
		db.recordCompactionExecution("noop", recommendation, nil)
		return recommendation, nil
	}
	err := db.MergeWithBudget(recommendation.BudgetBytes)
	db.recordCompactionExecution("merge", recommendation, err)
	return recommendation, err
}

func (db *DB) CompactionAudit() []CompactionAuditEvent {
	if db.compactionRecommendation == nil {
		return nil
	}
	state := db.compactionRecommendation
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]CompactionAuditEvent(nil), state.audit...)
}

func (db *DB) recordCompactionExecution(action string, recommendation CompactionRecommendation, err error) {
	if db.compactionRecommendation == nil {
		return
	}
	event := CompactionAuditEvent{
		Action:            action,
		BudgetBytes:       recommendation.BudgetBytes,
		Picker:            recommendation.Picker,
		Reason:            recommendation.Reason,
		Confidence:        recommendation.Confidence,
		Available:         recommendation.Available,
		Executed:          action == "merge" && err == nil,
		MergeBytesWritten: db.Metrics().MergeBytesWritten,
	}
	if err != nil {
		event.Error = err.Error()
	}
	state := db.compactionRecommendation
	state.mu.Lock()
	defer state.mu.Unlock()
	state.appendAuditLocked(event)
}

func (state *compactionRecommendationState) appendAuditLocked(event CompactionAuditEvent) {
	state.nextSeq++
	event.Sequence = state.nextSeq
	if len(state.audit) == cap(state.audit) {
		copy(state.audit, state.audit[1:])
		state.audit[len(state.audit)-1] = event
		return
	}
	state.audit = append(state.audit, event)
}

func compactionPickerMode(mode compactionmodel.PickerMode) CompactionPolicyMode {
	switch mode {
	case compactionmodel.PickerGarbageRatio:
		return CompactionByGarbageRatio
	case compactionmodel.PickerHotCold:
		return CompactionHotCold
	default:
		return CompactionByFileID
	}
}
