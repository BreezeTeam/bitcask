package kvseparation

import "errors"

type Placement string

const (
	PlacementInline   Placement = "inline"
	PlacementValueLog Placement = "value-log"
	PlacementColdTier Placement = "cold-tier"
)

type LifecycleObservation struct {
	ValueSize       int
	ReadCount       uint64
	UpdateCount     uint64
	AgeWindows      uint64
	OverwriteWindow uint64
}

type LifecycleConfig struct {
	SizeThreshold      int
	MinObservations    uint64
	HotReadThreshold   uint64
	FrequentUpdates    uint64
	ColdAgeWindows     uint64
	ColdValueThreshold int
	StableWindows      int
}

type PlacementDecision struct {
	Placement Placement
	Reason    string
	Changed   bool
}

type LifecyclePolicy struct {
	config         LifecycleConfig
	current        Placement
	candidate      Placement
	candidateCount int
}

var ErrNotFound = errors.New("key not found")

type Pointer struct {
	Offset int64
	Size   int
}

type ValueLog struct {
	values []byte
}

type Store struct {
	index map[string]Pointer
	vlog  ValueLog
}

type LogStats struct {
	TotalBytes int
	LiveBytes  int
	StaleBytes int
}

func DefaultLifecycleConfig() LifecycleConfig {
	return LifecycleConfig{
		SizeThreshold:      1024,
		MinObservations:    4,
		HotReadThreshold:   8,
		FrequentUpdates:    4,
		ColdAgeWindows:     8,
		ColdValueThreshold: 64 * 1024,
		StableWindows:      2,
	}
}

func NewLifecyclePolicy(config LifecycleConfig) *LifecyclePolicy {
	defaults := DefaultLifecycleConfig()
	if config.SizeThreshold <= 0 {
		config.SizeThreshold = defaults.SizeThreshold
	}
	if config.MinObservations == 0 {
		config.MinObservations = defaults.MinObservations
	}
	if config.HotReadThreshold == 0 {
		config.HotReadThreshold = defaults.HotReadThreshold
	}
	if config.FrequentUpdates == 0 {
		config.FrequentUpdates = defaults.FrequentUpdates
	}
	if config.ColdAgeWindows == 0 {
		config.ColdAgeWindows = defaults.ColdAgeWindows
	}
	if config.ColdValueThreshold <= 0 {
		config.ColdValueThreshold = defaults.ColdValueThreshold
	}
	if config.StableWindows <= 0 {
		config.StableWindows = defaults.StableWindows
	}
	return &LifecyclePolicy{config: config}
}

func (p *LifecyclePolicy) Decide(obs LifecycleObservation) PlacementDecision {
	placement, reason := p.classify(obs)
	if p.current == "" {
		p.current = placement
		return PlacementDecision{Placement: placement, Reason: reason, Changed: true}
	}
	if placement == p.current {
		p.candidate = ""
		p.candidateCount = 0
		return PlacementDecision{Placement: p.current, Reason: reason}
	}
	if placement != p.candidate {
		p.candidate = placement
		p.candidateCount = 1
	} else {
		p.candidateCount++
	}
	changed := p.candidateCount >= p.config.StableWindows
	if changed {
		p.current = p.candidate
		p.candidate = ""
		p.candidateCount = 0
	}
	return PlacementDecision{Placement: p.current, Reason: reason, Changed: changed}
}

func (p *LifecyclePolicy) classify(obs LifecycleObservation) (Placement, string) {
	observations := obs.ReadCount + obs.UpdateCount
	if observations < p.config.MinObservations {
		if obs.ValueSize >= p.config.SizeThreshold {
			return PlacementValueLog, "insufficient history; size threshold fallback"
		}
		return PlacementInline, "insufficient history; size threshold fallback"
	}
	if obs.ValueSize < p.config.SizeThreshold {
		return PlacementInline, "small value avoids pointer fixed cost"
	}
	if obs.UpdateCount >= p.config.FrequentUpdates || (obs.OverwriteWindow > 0 && obs.OverwriteWindow <= 2) {
		return PlacementInline, "frequent updates avoid value-log garbage"
	}
	if obs.ReadCount >= p.config.HotReadThreshold {
		return PlacementInline, "hot reads avoid value-log indirection"
	}
	if obs.ValueSize >= p.config.ColdValueThreshold && obs.AgeWindows >= p.config.ColdAgeWindows {
		return PlacementColdTier, "large old value is a cold-tier candidate"
	}
	return PlacementValueLog, "large stable value reduces main-log rewrite bytes"
}

func NewStore() *Store {
	return &Store{index: make(map[string]Pointer)}
}

func (s *Store) Put(key, value []byte) Pointer {
	ptr := s.vlog.Append(value)
	s.index[string(key)] = ptr
	return ptr
}

func (s *Store) Get(key []byte) ([]byte, error) {
	ptr, ok := s.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return s.vlog.Read(ptr)
}

func (s *Store) LivePointers() map[string]Pointer {
	pointers := make(map[string]Pointer, len(s.index))
	for key, ptr := range s.index {
		pointers[key] = ptr
	}
	return pointers
}

func (s *Store) Stats() LogStats {
	stats := LogStats{TotalBytes: len(s.vlog.values)}
	for _, ptr := range s.index {
		stats.LiveBytes += ptr.Size
	}
	stats.StaleBytes = stats.TotalBytes - stats.LiveBytes
	return stats
}

func (v *ValueLog) Append(value []byte) Pointer {
	ptr := Pointer{Offset: int64(len(v.values)), Size: len(value)}
	v.values = append(v.values, value...)
	return ptr
}

func (v *ValueLog) Read(ptr Pointer) ([]byte, error) {
	end := ptr.Offset + int64(ptr.Size)
	if ptr.Offset < 0 || end > int64(len(v.values)) {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v.values[ptr.Offset:end]...), nil
}
