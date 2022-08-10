package bitcask

import (
	"sync/atomic"
	"time"
)

func (db *DB) effectiveSyncPolicy() SyncPolicyMode {
	mode := db.opt.SyncPolicy.Mode
	if mode != SyncPolicyDefault {
		return mode
	}
	if db.opt.SyncEnable {
		return SyncPolicyEveryCommit
	}
	return SyncPolicyNone
}

func (tx *Tx) syncAfterTransaction(bytesWritten int64) error {
	db := tx.db
	db.dirtyBytes += bytesWritten
	db.dirtyCommits++

	switch db.effectiveSyncPolicy() {
	case SyncPolicyNone:
		return nil
	case SyncPolicyEveryCommit, SyncPolicyGroupCommit:
		return db.syncActiveFile()
	case SyncPolicyAdaptive:
		if db.shouldAdaptiveSync() {
			return db.syncActiveFile()
		}
		return nil
	default:
		return nil
	}
}

func (db *DB) shouldAdaptiveSync() bool {
	policy := db.opt.SyncPolicy
	if policy.DirtyBytesLimit > 0 && db.dirtyBytes >= policy.DirtyBytesLimit {
		return true
	}
	if policy.AdaptiveMaxDelay > 0 && time.Since(db.lastSyncAt) >= policy.AdaptiveMaxDelay {
		return true
	}
	if policy.AdaptiveMinDelay > 0 && time.Since(db.lastSyncAt) < policy.AdaptiveMinDelay {
		return false
	}
	if policy.TargetSyncLatency > 0 && db.lastSyncLatency > policy.TargetSyncLatency && policy.DirtyBytesLimit > 0 && db.dirtyBytes < policy.DirtyBytesLimit {
		return false
	}
	return true
}

func (db *DB) syncActiveFile() error {
	start := time.Now()
	err := db.ActiveFile.Sync()
	latency := time.Since(start)
	db.lastSyncLatency = latency
	atomic.AddUint64(&db.metrics.syncs, 1)
	atomic.AddUint64(&db.metrics.totalSyncNanos, uint64(latency.Nanoseconds()))
	if err != nil {
		atomic.AddUint64(&db.metrics.syncErrors, 1)
		return err
	}
	db.lastSyncAt = time.Now()
	db.dirtyBytes = 0
	db.dirtyCommits = 0
	return nil
}
