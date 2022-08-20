package bitcask

import (
	"sync"
	"sync/atomic"
)

type lifecycleKeyStats struct {
	reads       uint64
	updates     uint64
	firstWindow uint64
	lastUpdate  uint64
}

type KVPlacementMetrics struct {
	InlineDecisions   uint64
	ValueLogDecisions uint64
	ThresholdFallback uint64
}

type lifecyclePlacement struct {
	mu     sync.Mutex
	window uint64
	keys   map[string]*lifecycleKeyStats

	inlineDecisions   uint64
	valueLogDecisions uint64
	thresholdFallback uint64
}

func newLifecyclePlacement() *lifecyclePlacement {
	return &lifecyclePlacement{keys: make(map[string]*lifecycleKeyStats)}
}

func (db *DB) lifecycleRecordUpdate(bucket string, key []byte) *lifecycleKeyStats {
	if db.lifecycle == nil {
		return nil
	}
	db.lifecycle.mu.Lock()
	defer db.lifecycle.mu.Unlock()
	db.lifecycle.window++
	identity := entryIndexKey([]byte(bucket), key)
	stats := db.lifecycle.keys[identity]
	if stats == nil {
		stats = &lifecycleKeyStats{firstWindow: db.lifecycle.window}
		db.lifecycle.keys[identity] = stats
	}
	stats.updates++
	stats.lastUpdate = db.lifecycle.window
	return stats
}

func (db *DB) lifecycleRecordRead(bucket string, key []byte) {
	if db.lifecycle == nil {
		return
	}
	db.lifecycle.mu.Lock()
	defer db.lifecycle.mu.Unlock()
	identity := entryIndexKey([]byte(bucket), key)
	stats := db.lifecycle.keys[identity]
	if stats == nil {
		stats = &lifecycleKeyStats{firstWindow: db.lifecycle.window}
		db.lifecycle.keys[identity] = stats
	}
	stats.reads++
}

func (db *DB) shouldSeparateLifecycle(bucket string, key, value []byte) bool {
	stats := db.lifecycleRecordUpdate(bucket, key)
	opt := db.opt.KVSeparation
	if stats == nil {
		return len(value) >= opt.Threshold
	}
	minObservations := opt.LifecycleMinObservations
	if minObservations == 0 {
		minObservations = 4
	}
	hotReads := opt.LifecycleHotReads
	if hotReads == 0 {
		hotReads = 8
	}
	frequentUpdates := opt.LifecycleFrequentUpdates
	if frequentUpdates == 0 {
		frequentUpdates = 4
	}
	observations := stats.reads + stats.updates
	_ = opt.LifecycleColdAge
	_ = opt.LifecycleColdValueSize
	if observations < minObservations {
		atomic.AddUint64(&db.lifecycle.thresholdFallback, 1)
		separate := len(value) >= opt.Threshold
		db.recordPlacementDecision(separate)
		return separate
	}
	if len(value) < opt.Threshold || stats.reads >= hotReads || stats.updates >= frequentUpdates {
		db.recordPlacementDecision(false)
		return false
	}
	atomic.AddUint64(&db.lifecycle.valueLogDecisions, 1)
	return true
}

func (db *DB) recordPlacementDecision(separate bool) {
	if separate {
		atomic.AddUint64(&db.lifecycle.valueLogDecisions, 1)
		return
	}
	atomic.AddUint64(&db.lifecycle.inlineDecisions, 1)
}

func (db *DB) KVPlacementMetrics() KVPlacementMetrics {
	if db.lifecycle == nil {
		return KVPlacementMetrics{}
	}
	return KVPlacementMetrics{
		InlineDecisions:   atomic.LoadUint64(&db.lifecycle.inlineDecisions),
		ValueLogDecisions: atomic.LoadUint64(&db.lifecycle.valueLogDecisions),
		ThresholdFallback: atomic.LoadUint64(&db.lifecycle.thresholdFallback),
	}
}
