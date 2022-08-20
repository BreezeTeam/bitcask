package bitcask

import "sync/atomic"

type MetricsSnapshot struct {
	Commits                uint64
	EntriesWritten         uint64
	BytesWritten           uint64
	Syncs                  uint64
	SyncErrors             uint64
	TotalSyncNanos         uint64
	Rotations              uint64
	MergeRuns              uint64
	MergeBytesRead         uint64
	MergeBytesWritten      uint64
	RecoveryEntries        uint64
	RecoveryCommittedTx    uint64
	GroupEpochs            uint64
	GroupWaiters           uint64
	GroupMaxSize           uint64
	GroupLastEpoch         uint64
	GroupSize1             uint64
	GroupSize2             uint64
	GroupSize3To4          uint64
	GroupSize5To8          uint64
	GroupSize9Plus         uint64
	DurableFrontier        uint64
	DurabilityRetries      uint64
	DurabilityRetryOK      uint64
	DurabilityRetryAfter1  uint64
	DurabilityRetryAfter2  uint64
	DurabilityRetryAfter3  uint64
	DurabilityRetryAfter4P uint64
}

type metrics struct {
	commits                uint64
	entriesWritten         uint64
	bytesWritten           uint64
	syncs                  uint64
	syncErrors             uint64
	totalSyncNanos         uint64
	rotations              uint64
	mergeRuns              uint64
	mergeBytesRead         uint64
	mergeBytesWritten      uint64
	recoveryEntries        uint64
	recoveryCommittedTx    uint64
	groupEpochs            uint64
	groupWaiters           uint64
	groupMaxSize           uint64
	groupLastEpoch         uint64
	groupSize1             uint64
	groupSize2             uint64
	groupSize3To4          uint64
	groupSize5To8          uint64
	groupSize9Plus         uint64
	durableFrontier        uint64
	durabilityRetries      uint64
	durabilityRetryOK      uint64
	durabilityRetryAfter1  uint64
	durabilityRetryAfter2  uint64
	durabilityRetryAfter3  uint64
	durabilityRetryAfter4P uint64
}

func (db *DB) recordDurabilityRetrySuccess(failures int) {
	atomic.AddUint64(&db.metrics.durabilityRetryOK, 1)
	switch {
	case failures <= 1:
		atomic.AddUint64(&db.metrics.durabilityRetryAfter1, 1)
	case failures == 2:
		atomic.AddUint64(&db.metrics.durabilityRetryAfter2, 1)
	case failures == 3:
		atomic.AddUint64(&db.metrics.durabilityRetryAfter3, 1)
	default:
		atomic.AddUint64(&db.metrics.durabilityRetryAfter4P, 1)
	}
}

func (db *DB) recordGroupEpoch(epochID, frontier uint64, waiters int) {
	atomic.AddUint64(&db.metrics.groupEpochs, 1)
	atomic.AddUint64(&db.metrics.groupWaiters, uint64(waiters))
	atomic.StoreUint64(&db.metrics.groupLastEpoch, epochID)
	atomic.StoreUint64(&db.metrics.durableFrontier, frontier)
	groupSize := uint64(waiters)
	switch {
	case groupSize <= 1:
		atomic.AddUint64(&db.metrics.groupSize1, 1)
	case groupSize == 2:
		atomic.AddUint64(&db.metrics.groupSize2, 1)
	case groupSize <= 4:
		atomic.AddUint64(&db.metrics.groupSize3To4, 1)
	case groupSize <= 8:
		atomic.AddUint64(&db.metrics.groupSize5To8, 1)
	default:
		atomic.AddUint64(&db.metrics.groupSize9Plus, 1)
	}
	for {
		current := atomic.LoadUint64(&db.metrics.groupMaxSize)
		if groupSize <= current || atomic.CompareAndSwapUint64(&db.metrics.groupMaxSize, current, groupSize) {
			break
		}
	}
}

func (db *DB) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Commits:                atomic.LoadUint64(&db.metrics.commits),
		EntriesWritten:         atomic.LoadUint64(&db.metrics.entriesWritten),
		BytesWritten:           atomic.LoadUint64(&db.metrics.bytesWritten),
		Syncs:                  atomic.LoadUint64(&db.metrics.syncs),
		SyncErrors:             atomic.LoadUint64(&db.metrics.syncErrors),
		TotalSyncNanos:         atomic.LoadUint64(&db.metrics.totalSyncNanos),
		Rotations:              atomic.LoadUint64(&db.metrics.rotations),
		MergeRuns:              atomic.LoadUint64(&db.metrics.mergeRuns),
		MergeBytesRead:         atomic.LoadUint64(&db.metrics.mergeBytesRead),
		MergeBytesWritten:      atomic.LoadUint64(&db.metrics.mergeBytesWritten),
		RecoveryEntries:        atomic.LoadUint64(&db.metrics.recoveryEntries),
		RecoveryCommittedTx:    atomic.LoadUint64(&db.metrics.recoveryCommittedTx),
		GroupEpochs:            atomic.LoadUint64(&db.metrics.groupEpochs),
		GroupWaiters:           atomic.LoadUint64(&db.metrics.groupWaiters),
		GroupMaxSize:           atomic.LoadUint64(&db.metrics.groupMaxSize),
		GroupLastEpoch:         atomic.LoadUint64(&db.metrics.groupLastEpoch),
		GroupSize1:             atomic.LoadUint64(&db.metrics.groupSize1),
		GroupSize2:             atomic.LoadUint64(&db.metrics.groupSize2),
		GroupSize3To4:          atomic.LoadUint64(&db.metrics.groupSize3To4),
		GroupSize5To8:          atomic.LoadUint64(&db.metrics.groupSize5To8),
		GroupSize9Plus:         atomic.LoadUint64(&db.metrics.groupSize9Plus),
		DurableFrontier:        atomic.LoadUint64(&db.metrics.durableFrontier),
		DurabilityRetries:      atomic.LoadUint64(&db.metrics.durabilityRetries),
		DurabilityRetryOK:      atomic.LoadUint64(&db.metrics.durabilityRetryOK),
		DurabilityRetryAfter1:  atomic.LoadUint64(&db.metrics.durabilityRetryAfter1),
		DurabilityRetryAfter2:  atomic.LoadUint64(&db.metrics.durabilityRetryAfter2),
		DurabilityRetryAfter3:  atomic.LoadUint64(&db.metrics.durabilityRetryAfter3),
		DurabilityRetryAfter4P: atomic.LoadUint64(&db.metrics.durabilityRetryAfter4P),
	}
}
