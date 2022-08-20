package bitcask

import (
	"sync/atomic"
	"time"
)

var commitLatencyBounds = [...]time.Duration{
	10 * time.Microsecond,
	25 * time.Microsecond,
	50 * time.Microsecond,
	100 * time.Microsecond,
	250 * time.Microsecond,
	500 * time.Microsecond,
	time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
}

type CommitLatencySnapshot struct {
	Count uint64
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

type commitLatencyHistogram struct {
	buckets  [len(commitLatencyBounds) + 1]uint64
	maxNanos uint64
}

func (h *commitLatencyHistogram) observe(duration time.Duration) {
	index := len(commitLatencyBounds)
	for i, bound := range commitLatencyBounds {
		if duration <= bound {
			index = i
			break
		}
	}
	atomic.AddUint64(&h.buckets[index], 1)
	value := uint64(duration)
	for {
		current := atomic.LoadUint64(&h.maxNanos)
		if value <= current || atomic.CompareAndSwapUint64(&h.maxNanos, current, value) {
			break
		}
	}
}

func (h *commitLatencyHistogram) snapshot() CommitLatencySnapshot {
	counts := make([]uint64, len(h.buckets))
	var total uint64
	for i := range h.buckets {
		counts[i] = atomic.LoadUint64(&h.buckets[i])
		total += counts[i]
	}
	return CommitLatencySnapshot{
		Count: total,
		P50:   histogramPercentile(counts, total, 50),
		P95:   histogramPercentile(counts, total, 95),
		P99:   histogramPercentile(counts, total, 99),
		Max:   time.Duration(atomic.LoadUint64(&h.maxNanos)),
	}
}

func histogramPercentile(counts []uint64, total uint64, percentile uint64) time.Duration {
	if total == 0 {
		return 0
	}
	target := (total*percentile + 99) / 100
	var cumulative uint64
	for i, count := range counts {
		cumulative += count
		if cumulative < target {
			continue
		}
		if i < len(commitLatencyBounds) {
			return commitLatencyBounds[i]
		}
		return time.Second
	}
	return time.Second
}

func (db *DB) CommitLatency() CommitLatencySnapshot {
	return db.commitLatency.snapshot()
}
