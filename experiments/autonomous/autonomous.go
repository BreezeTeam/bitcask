package autonomous

import "time"

type Phase string

const (
	PhaseUnknown              Phase = "unknown"
	PhaseMixed                Phase = "mixed"
	PhaseWriteHeavy           Phase = "write-heavy"
	PhaseReadHeavy            Phase = "read-heavy"
	PhaseOverwriteHeavy       Phase = "overwrite-heavy"
	PhaseLargeValueHeavy      Phase = "large-value-heavy"
	PhaseSyncLatencySensitive Phase = "sync-latency-sensitive"
)

type SyncPolicy string

const (
	SyncNoChange SyncPolicy = "no-change"
	SyncAdaptive SyncPolicy = "adaptive"
	SyncGroup    SyncPolicy = "group"
)

type CompactionPolicy string

const (
	CompactionNoChange     CompactionPolicy = "no-change"
	CompactionFileID       CompactionPolicy = "file-id"
	CompactionGarbageRatio CompactionPolicy = "garbage-ratio"
	CompactionHotCold      CompactionPolicy = "hot-cold"
)

type PlacementPolicy string

const (
	PlacementNoChange  PlacementPolicy = "no-change"
	PlacementInline    PlacementPolicy = "inline"
	PlacementValueLog  PlacementPolicy = "value-log"
	PlacementLifecycle PlacementPolicy = "lifecycle"
)

type Observation struct {
	Reads            uint64
	Writes           uint64
	Overwrites       uint64
	LargeValueWrites uint64
	SyncLatency      time.Duration
}

type Config struct {
	MinOperations        uint64
	WriteRatio           float64
	ReadRatio            float64
	OverwriteRatio       float64
	LargeValueRatio      float64
	SyncLatencyThreshold time.Duration
	ConsecutiveWindows   int
	CooldownWindows      uint64
}

type Analysis struct {
	Phase      Phase
	Confidence float64
	Reason     string
}

type Recommendation struct {
	Phase      Phase
	Confidence float64
	Sync       SyncPolicy
	Compaction CompactionPolicy
	Placement  PlacementPolicy
	Reason     string
	Changed    bool
}

type Detector struct {
	config         Config
	current        Phase
	candidate      Phase
	candidateCount int
	window         uint64
	lastChange     uint64
}

func DefaultConfig() Config {
	return Config{
		MinOperations:        100,
		WriteRatio:           0.70,
		ReadRatio:            0.70,
		OverwriteRatio:       0.50,
		LargeValueRatio:      0.50,
		SyncLatencyThreshold: 5 * time.Millisecond,
		ConsecutiveWindows:   2,
		CooldownWindows:      2,
	}
}

func NewDetector(config Config) *Detector {
	defaults := DefaultConfig()
	if config.MinOperations == 0 {
		config.MinOperations = defaults.MinOperations
	}
	if config.WriteRatio <= 0 {
		config.WriteRatio = defaults.WriteRatio
	}
	if config.ReadRatio <= 0 {
		config.ReadRatio = defaults.ReadRatio
	}
	if config.OverwriteRatio <= 0 {
		config.OverwriteRatio = defaults.OverwriteRatio
	}
	if config.LargeValueRatio <= 0 {
		config.LargeValueRatio = defaults.LargeValueRatio
	}
	if config.SyncLatencyThreshold <= 0 {
		config.SyncLatencyThreshold = defaults.SyncLatencyThreshold
	}
	if config.ConsecutiveWindows <= 0 {
		config.ConsecutiveWindows = defaults.ConsecutiveWindows
	}
	return &Detector{config: config, current: PhaseUnknown}
}

func (d *Detector) Analyze(obs Observation) Analysis {
	operations := obs.Reads + obs.Writes
	if operations < d.config.MinOperations {
		return Analysis{Phase: PhaseUnknown, Reason: "insufficient operations"}
	}

	writeRatio := ratio(obs.Writes, operations)
	readRatio := ratio(obs.Reads, operations)
	overwriteRatio := ratio(obs.Overwrites, obs.Writes)
	largeValueRatio := ratio(obs.LargeValueWrites, obs.Writes)

	if obs.Writes > 0 && obs.SyncLatency >= d.config.SyncLatencyThreshold && writeRatio >= 0.5 {
		return Analysis{Phase: PhaseSyncLatencySensitive, Confidence: boundedRatio(obs.SyncLatency, d.config.SyncLatencyThreshold), Reason: "sync latency exceeds threshold"}
	}
	if obs.Writes > 0 && overwriteRatio >= d.config.OverwriteRatio {
		return Analysis{Phase: PhaseOverwriteHeavy, Confidence: overwriteRatio, Reason: "overwrites dominate writes"}
	}
	if obs.Writes > 0 && largeValueRatio >= d.config.LargeValueRatio {
		return Analysis{Phase: PhaseLargeValueHeavy, Confidence: largeValueRatio, Reason: "large values dominate writes"}
	}
	if writeRatio >= d.config.WriteRatio {
		return Analysis{Phase: PhaseWriteHeavy, Confidence: writeRatio, Reason: "writes dominate operations"}
	}
	if readRatio >= d.config.ReadRatio {
		return Analysis{Phase: PhaseReadHeavy, Confidence: readRatio, Reason: "reads dominate operations"}
	}
	return Analysis{Phase: PhaseMixed, Confidence: 1 - abs(writeRatio-readRatio), Reason: "no operation class dominates"}
}

func (d *Detector) Observe(obs Observation) Recommendation {
	d.window++
	analysis := d.Analyze(obs)
	if analysis.Phase == PhaseUnknown {
		d.candidate = PhaseUnknown
		d.candidateCount = 0
		return recommend(d.current, analysis.Confidence, analysis.Reason, false)
	}

	if analysis.Phase == d.current {
		d.candidate = PhaseUnknown
		d.candidateCount = 0
		return recommend(d.current, analysis.Confidence, analysis.Reason, false)
	}
	if analysis.Phase != d.candidate {
		d.candidate = analysis.Phase
		d.candidateCount = 1
	} else {
		d.candidateCount++
	}

	cooldownComplete := d.lastChange == 0 || d.window-d.lastChange >= d.config.CooldownWindows
	changed := d.candidateCount >= d.config.ConsecutiveWindows && cooldownComplete
	if changed {
		d.current = d.candidate
		d.candidate = PhaseUnknown
		d.candidateCount = 0
		d.lastChange = d.window
	}
	return recommend(d.current, analysis.Confidence, analysis.Reason, changed)
}

func recommend(phase Phase, confidence float64, reason string, changed bool) Recommendation {
	rec := Recommendation{Phase: phase, Confidence: confidence, Reason: reason, Changed: changed}
	switch phase {
	case PhaseWriteHeavy:
		rec.Sync = SyncGroup
		rec.Compaction = CompactionGarbageRatio
		rec.Placement = PlacementInline
	case PhaseReadHeavy:
		rec.Sync = SyncNoChange
		rec.Compaction = CompactionHotCold
		rec.Placement = PlacementInline
	case PhaseOverwriteHeavy:
		rec.Sync = SyncGroup
		rec.Compaction = CompactionGarbageRatio
		rec.Placement = PlacementLifecycle
	case PhaseLargeValueHeavy:
		rec.Sync = SyncAdaptive
		rec.Compaction = CompactionHotCold
		rec.Placement = PlacementValueLog
	case PhaseSyncLatencySensitive:
		rec.Sync = SyncAdaptive
		rec.Compaction = CompactionFileID
		rec.Placement = PlacementNoChange
	case PhaseMixed:
		rec.Sync = SyncAdaptive
		rec.Compaction = CompactionHotCold
		rec.Placement = PlacementLifecycle
	default:
		rec.Sync = SyncNoChange
		rec.Compaction = CompactionNoChange
		rec.Placement = PlacementNoChange
	}
	return rec
}

func ratio(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total)
}

func boundedRatio(value, threshold time.Duration) float64 {
	if threshold <= 0 || value >= threshold*2 {
		return 1
	}
	return float64(value) / float64(threshold*2)
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
