package adaptivesync

import "time"

type Observation struct {
	DirtyBytes      int64
	Commits         int
	LastSyncLatency time.Duration
	SinceLastSync   time.Duration
}

type Decision struct {
	ShouldSync bool
	Delay      time.Duration
}

type Policy struct {
	DirtyBytesLimit   int64
	MinDelay          time.Duration
	MaxDelay          time.Duration
	TargetSyncLatency time.Duration
}

func (p Policy) Decide(obs Observation) Decision {
	if p.DirtyBytesLimit > 0 && obs.DirtyBytes >= p.DirtyBytesLimit {
		return Decision{ShouldSync: true}
	}
	if p.MaxDelay > 0 && obs.SinceLastSync >= p.MaxDelay {
		return Decision{ShouldSync: true}
	}
	if p.TargetSyncLatency > 0 && obs.LastSyncLatency > p.TargetSyncLatency && obs.DirtyBytes < p.DirtyBytesLimit {
		return Decision{Delay: p.boundedDelay(p.MinDelay)}
	}
	if p.MinDelay > 0 && obs.SinceLastSync < p.MinDelay {
		return Decision{Delay: p.MinDelay - obs.SinceLastSync}
	}
	return Decision{ShouldSync: true}
}

func (p Policy) boundedDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}
