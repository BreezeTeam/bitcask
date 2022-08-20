package compaction

import "time"

type WorkloadPhase string

const (
	PhaseUnknown        WorkloadPhase = "unknown"
	PhaseWriteHeavy     WorkloadPhase = "write-heavy"
	PhaseReadHeavy      WorkloadPhase = "read-heavy"
	PhaseOverwriteHeavy WorkloadPhase = "overwrite-heavy"
	PhaseMixed          WorkloadPhase = "mixed"
)

type PickerMode string

const (
	PickerFileID       PickerMode = "file-id"
	PickerGarbageRatio PickerMode = "garbage-ratio"
	PickerHotCold      PickerMode = "hot-cold"
)

type ControllerObservation struct {
	ForegroundP99       time.Duration
	TargetP99           time.Duration
	PendingGarbageBytes int64
	DataBytes           int64
	WriteAmplification  float64
	SpaceAmplification  float64
	CompactionBytesSec  int64
	Phase               WorkloadPhase
}

type ControllerConfig struct {
	MinGarbageBytes   int64
	EmergencySpaceAmp float64
	HighWriteAmp      float64
	BaseBudgetBytes   int64
	MaxBudgetBytes    int64
	StableWindows     int
	CooldownWindows   uint64
}

type ControlDecision struct {
	BudgetBytes int64
	Picker      PickerMode
	Reason      string
	Confidence  float64
	Changed     bool
}

type Controller struct {
	config         ControllerConfig
	current        ControlDecision
	candidate      ControlDecision
	candidateCount int
	window         uint64
	lastChange     uint64
}

func DefaultControllerConfig() ControllerConfig {
	return ControllerConfig{
		MinGarbageBytes:   64 * 1024 * 1024,
		EmergencySpaceAmp: 2.0,
		HighWriteAmp:      3.0,
		BaseBudgetBytes:   16 * 1024 * 1024,
		MaxBudgetBytes:    256 * 1024 * 1024,
		StableWindows:     2,
		CooldownWindows:   2,
	}
}

func NewController(config ControllerConfig) *Controller {
	defaults := DefaultControllerConfig()
	if config.MinGarbageBytes <= 0 {
		config.MinGarbageBytes = defaults.MinGarbageBytes
	}
	if config.EmergencySpaceAmp <= 0 {
		config.EmergencySpaceAmp = defaults.EmergencySpaceAmp
	}
	if config.HighWriteAmp <= 0 {
		config.HighWriteAmp = defaults.HighWriteAmp
	}
	if config.BaseBudgetBytes <= 0 {
		config.BaseBudgetBytes = defaults.BaseBudgetBytes
	}
	if config.MaxBudgetBytes <= 0 {
		config.MaxBudgetBytes = defaults.MaxBudgetBytes
	}
	if config.MaxBudgetBytes < config.BaseBudgetBytes {
		config.MaxBudgetBytes = config.BaseBudgetBytes
	}
	if config.StableWindows <= 0 {
		config.StableWindows = defaults.StableWindows
	}
	return &Controller{config: config, current: ControlDecision{Picker: PickerFileID}}
}

func (c *Controller) Evaluate(obs ControllerObservation) ControlDecision {
	decision := ControlDecision{Picker: pickerForPhase(obs.Phase)}
	latencyRatio := durationRatio(obs.ForegroundP99, obs.TargetP99)
	garbageRatio := byteRatio(obs.PendingGarbageBytes, obs.DataBytes)

	switch {
	case obs.SpaceAmplification >= c.config.EmergencySpaceAmp:
		decision.BudgetBytes = c.clampBudget(c.config.BaseBudgetBytes * 2)
		decision.Picker = PickerGarbageRatio
		decision.Reason = "emergency space amplification"
		decision.Confidence = clamp01(obs.SpaceAmplification / c.config.EmergencySpaceAmp / 2)
	case obs.TargetP99 > 0 && obs.ForegroundP99 > obs.TargetP99:
		decision.Reason = "foreground p99 exceeds target"
		decision.Confidence = clamp01(latencyRatio / 2)
	case obs.PendingGarbageBytes < c.config.MinGarbageBytes:
		decision.Reason = "garbage below minimum work threshold"
		decision.Confidence = clamp01(1 - garbageRatio)
	case obs.WriteAmplification >= c.config.HighWriteAmp:
		decision.BudgetBytes = c.clampBudget(c.config.BaseBudgetBytes / 2)
		decision.Reason = "high write amplification limits rewrite budget"
		decision.Confidence = clamp01(obs.WriteAmplification / c.config.HighWriteAmp / 2)
	default:
		budget := c.config.BaseBudgetBytes
		if garbageRatio >= 0.5 {
			budget *= 2
		}
		if obs.Phase == PhaseWriteHeavy {
			budget /= 2
		}
		decision.BudgetBytes = c.clampBudget(budget)
		decision.Reason = "garbage pressure within latency budget"
		decision.Confidence = clamp01(garbageRatio + 0.25)
	}
	return decision
}

func (c *Controller) Observe(obs ControllerObservation) ControlDecision {
	c.window++
	next := c.Evaluate(obs)
	if sameControl(next, c.current) {
		c.candidateCount = 0
		c.candidate = ControlDecision{}
		c.current.Reason = next.Reason
		c.current.Confidence = next.Confidence
		current := c.current
		current.Changed = false
		return current
	}
	if !sameControl(next, c.candidate) {
		c.candidate = next
		c.candidateCount = 1
	} else {
		c.candidateCount++
	}
	cooldownComplete := c.lastChange == 0 || c.window-c.lastChange >= c.config.CooldownWindows
	if c.candidateCount >= c.config.StableWindows && cooldownComplete {
		next.BudgetBytes = c.boundedStep(c.current.BudgetBytes, next.BudgetBytes)
		next.Changed = true
		c.current = next
		c.candidate = ControlDecision{}
		c.candidateCount = 0
		c.lastChange = c.window
		return c.current
	}
	current := c.current
	current.Changed = false
	return current
}

func (c *Controller) boundedStep(current, target int64) int64 {
	if current == target {
		return target
	}
	step := c.config.BaseBudgetBytes
	if target > current && target-current > step {
		return current + step
	}
	if current > target && current-target > step {
		return current - step
	}
	return target
}

func (c *Controller) clampBudget(budget int64) int64 {
	if budget < 0 {
		return 0
	}
	if budget > c.config.MaxBudgetBytes {
		return c.config.MaxBudgetBytes
	}
	return budget
}

func pickerForPhase(phase WorkloadPhase) PickerMode {
	switch phase {
	case PhaseReadHeavy:
		return PickerHotCold
	case PhaseOverwriteHeavy:
		return PickerGarbageRatio
	default:
		return PickerFileID
	}
}

func sameControl(a, b ControlDecision) bool {
	return a.BudgetBytes == b.BudgetBytes && a.Picker == b.Picker
}

func byteRatio(part, total int64) float64 {
	if total <= 0 || part <= 0 {
		return 0
	}
	return float64(part) / float64(total)
}

func durationRatio(value, target time.Duration) float64 {
	if target <= 0 || value <= 0 {
		return 0
	}
	return float64(value) / float64(target)
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}
