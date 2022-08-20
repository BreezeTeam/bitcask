package bitcask

import (
	autonomousmodel "github.com/BreezeTeam/bitcask/experiments/autonomous"
	"sync"
	"sync/atomic"
	"time"
)

type AutonomousObservation struct {
	Reads              uint64
	Writes             uint64
	Overwrites         uint64
	LargeValueWrites   uint64
	SyncLatency        time.Duration
	ValueLogTotalBytes int64
	ValueLogLiveBytes  int64
	ValueLogStaleBytes int64
}

type PolicyRecommendation struct {
	Phase      string
	Confidence float64
	Sync       string
	Compaction string
	Placement  string
	Reason     string
	Changed    bool
	Available  bool
}

type PolicyAuditEvent struct {
	Sequence           uint64
	Kind               string
	Phase              string
	Confidence         float64
	CompactionChanged  bool
	PlacementChanged   bool
	Reason             string
	ValueLogStaleBytes int64
}

type autonomousState struct {
	reads            uint64
	writes           uint64
	overwrites       uint64
	largeValueWrites uint64

	mu             sync.RWMutex
	detector       *autonomousmodel.Detector
	recommendation PolicyRecommendation
	lastWindow     AutonomousObservation
	lastSyncs      uint64
	lastSyncNanos  uint64
	windowSize     uint64
	largeThreshold int
	audit          []PolicyAuditEvent
	auditStart     int
	auditCount     int
	auditSequence  uint64
}

func newAutonomousState(options AutonomousOptions) *autonomousState {
	windowSize := options.WindowOperations
	if windowSize == 0 {
		windowSize = 100
	}
	largeThreshold := options.LargeValueThreshold
	if largeThreshold <= 0 {
		largeThreshold = 16 * 1024
	}
	config := autonomousmodel.DefaultConfig()
	if options.MinOperations > 0 {
		config.MinOperations = options.MinOperations
	}
	if options.ConsecutiveWindows > 0 {
		config.ConsecutiveWindows = options.ConsecutiveWindows
	}
	if options.CooldownWindows > 0 {
		config.CooldownWindows = options.CooldownWindows
	}
	auditCapacity := options.AuditCapacity
	if auditCapacity <= 0 {
		auditCapacity = 64
	}
	return &autonomousState{
		detector:       autonomousmodel.NewDetector(config),
		windowSize:     windowSize,
		largeThreshold: largeThreshold,
		audit:          make([]PolicyAuditEvent, auditCapacity),
	}
}

func (db *DB) recordAutonomousRead() {
	if db.autonomous == nil {
		return
	}
	atomic.AddUint64(&db.autonomous.reads, 1)
	db.maybeEvaluateAutonomous()
}

func (db *DB) recordAutonomousWrite(bucket string, key, value []byte) {
	if db.autonomous == nil {
		return
	}
	atomic.AddUint64(&db.autonomous.writes, 1)
	if len(value) >= db.autonomous.largeThreshold {
		atomic.AddUint64(&db.autonomous.largeValueWrites, 1)
	}
	if idx := db.BPTreeIdx[bucket]; idx != nil {
		if _, err := idx.Find(key); err == nil {
			atomic.AddUint64(&db.autonomous.overwrites, 1)
		}
	}
	db.maybeEvaluateAutonomous()
}

func (db *DB) maybeEvaluateAutonomous() {
	state := db.autonomous
	if state == nil {
		return
	}
	reads := atomic.LoadUint64(&state.reads)
	writes := atomic.LoadUint64(&state.writes)
	state.mu.RLock()
	lastTotal := state.lastWindow.Reads + state.lastWindow.Writes
	state.mu.RUnlock()
	if reads+writes-lastTotal < state.windowSize {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	reads = atomic.LoadUint64(&state.reads)
	writes = atomic.LoadUint64(&state.writes)
	if reads+writes-(state.lastWindow.Reads+state.lastWindow.Writes) < state.windowSize {
		return
	}
	current := db.autonomousObservationLocked()
	syncs := atomic.LoadUint64(&db.metrics.syncs)
	totalSyncNanos := atomic.LoadUint64(&db.metrics.totalSyncNanos)
	var syncLatency time.Duration
	if deltaSyncs := syncs - state.lastSyncs; deltaSyncs > 0 {
		syncLatency = time.Duration((totalSyncNanos - state.lastSyncNanos) / deltaSyncs)
	}
	delta := AutonomousObservation{
		Reads:            current.Reads - state.lastWindow.Reads,
		Writes:           current.Writes - state.lastWindow.Writes,
		Overwrites:       current.Overwrites - state.lastWindow.Overwrites,
		LargeValueWrites: current.LargeValueWrites - state.lastWindow.LargeValueWrites,
		SyncLatency:      syncLatency,
	}
	recommendation := state.detector.Observe(autonomousmodel.Observation{
		Reads:            delta.Reads,
		Writes:           delta.Writes,
		Overwrites:       delta.Overwrites,
		LargeValueWrites: delta.LargeValueWrites,
		SyncLatency:      delta.SyncLatency,
	})
	state.recommendation = PolicyRecommendation{
		Phase:      string(recommendation.Phase),
		Confidence: recommendation.Confidence,
		Sync:       string(recommendation.Sync),
		Compaction: string(recommendation.Compaction),
		Placement:  string(recommendation.Placement),
		Reason:     recommendation.Reason,
		Changed:    recommendation.Changed,
		Available:  recommendation.Phase != autonomousmodel.PhaseUnknown,
	}
	state.appendAuditLocked(PolicyAuditEvent{
		Kind:               "recommendation",
		Phase:              state.recommendation.Phase,
		Confidence:         state.recommendation.Confidence,
		Reason:             state.recommendation.Reason,
		ValueLogStaleBytes: current.ValueLogStaleBytes,
	})
	state.lastWindow = current
	state.lastSyncs = syncs
	state.lastSyncNanos = totalSyncNanos
}

func (db *DB) AutonomousObservation() AutonomousObservation {
	if db.autonomous == nil {
		return AutonomousObservation{}
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.autonomousObservationLocked()
}

func (db *DB) autonomousObservationLocked() AutonomousObservation {
	syncs := atomic.LoadUint64(&db.metrics.syncs)
	totalSyncNanos := atomic.LoadUint64(&db.metrics.totalSyncNanos)
	var syncLatency time.Duration
	if syncs > 0 {
		syncLatency = time.Duration(totalSyncNanos / syncs)
	}
	observation := AutonomousObservation{
		Reads:            atomic.LoadUint64(&db.autonomous.reads),
		Writes:           atomic.LoadUint64(&db.autonomous.writes),
		Overwrites:       atomic.LoadUint64(&db.autonomous.overwrites),
		LargeValueWrites: atomic.LoadUint64(&db.autonomous.largeValueWrites),
		SyncLatency:      syncLatency,
	}
	for _, segment := range db.valueLogStatsLocked() {
		observation.ValueLogTotalBytes += segment.TotalBytes
		observation.ValueLogLiveBytes += segment.LiveBytes
		observation.ValueLogStaleBytes += segment.StaleBytes
	}
	return observation
}

func (db *DB) PolicyRecommendation() PolicyRecommendation {
	if db.autonomous == nil {
		return PolicyRecommendation{}
	}
	db.autonomous.mu.RLock()
	defer db.autonomous.mu.RUnlock()
	return db.autonomous.recommendation
}

func (s *autonomousState) appendAuditLocked(event PolicyAuditEvent) {
	if len(s.audit) == 0 {
		return
	}
	s.auditSequence++
	event.Sequence = s.auditSequence
	index := (s.auditStart + s.auditCount) % len(s.audit)
	if s.auditCount == len(s.audit) {
		index = s.auditStart
		s.auditStart = (s.auditStart + 1) % len(s.audit)
	} else {
		s.auditCount++
	}
	s.audit[index] = event
}

func (db *DB) PolicyAudit() []PolicyAuditEvent {
	if db.autonomous == nil {
		return nil
	}
	db.autonomous.mu.RLock()
	defer db.autonomous.mu.RUnlock()
	result := make([]PolicyAuditEvent, db.autonomous.auditCount)
	for i := 0; i < db.autonomous.auditCount; i++ {
		result[i] = db.autonomous.audit[(db.autonomous.auditStart+i)%len(db.autonomous.audit)]
	}
	return result
}
