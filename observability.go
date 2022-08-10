package bitcask

import "sync/atomic"

type MetricsSnapshot struct {
	Commits             uint64
	EntriesWritten      uint64
	BytesWritten        uint64
	Syncs               uint64
	SyncErrors          uint64
	TotalSyncNanos      uint64
	Rotations           uint64
	MergeRuns           uint64
	MergeBytesRead      uint64
	MergeBytesWritten   uint64
	RecoveryEntries     uint64
	RecoveryCommittedTx uint64
}

type metrics struct {
	commits             uint64
	entriesWritten      uint64
	bytesWritten        uint64
	syncs               uint64
	syncErrors          uint64
	totalSyncNanos      uint64
	rotations           uint64
	mergeRuns           uint64
	mergeBytesRead      uint64
	mergeBytesWritten   uint64
	recoveryEntries     uint64
	recoveryCommittedTx uint64
}

func (db *DB) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Commits:             atomic.LoadUint64(&db.metrics.commits),
		EntriesWritten:      atomic.LoadUint64(&db.metrics.entriesWritten),
		BytesWritten:        atomic.LoadUint64(&db.metrics.bytesWritten),
		Syncs:               atomic.LoadUint64(&db.metrics.syncs),
		SyncErrors:          atomic.LoadUint64(&db.metrics.syncErrors),
		TotalSyncNanos:      atomic.LoadUint64(&db.metrics.totalSyncNanos),
		Rotations:           atomic.LoadUint64(&db.metrics.rotations),
		MergeRuns:           atomic.LoadUint64(&db.metrics.mergeRuns),
		MergeBytesRead:      atomic.LoadUint64(&db.metrics.mergeBytesRead),
		MergeBytesWritten:   atomic.LoadUint64(&db.metrics.mergeBytesWritten),
		RecoveryEntries:     atomic.LoadUint64(&db.metrics.recoveryEntries),
		RecoveryCommittedTx: atomic.LoadUint64(&db.metrics.recoveryCommittedTx),
	}
}
