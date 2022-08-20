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
		return db.syncDurabilityResources()
	case SyncPolicyAdaptive:
		if db.shouldAdaptiveSync() {
			return db.syncDurabilityResources()
		}
		if db.adaptiveSync != nil {
			db.adaptiveSync.notify()
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
	if policy.DirtyCommitsLimit > 0 && db.dirtyCommits >= policy.DirtyCommitsLimit {
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

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDBClosed
	}
	return db.syncDurabilityResources()
}

func (db *DB) syncDurabilityResources() error {
	db.durabilityMu.Lock()
	defer db.durabilityMu.Unlock()
	start := time.Now()
	wasRetry := db.durabilityFailed
	if wasRetry {
		atomic.AddUint64(&db.metrics.durabilityRetries, 1)
	}
	var err error
	if db.valueLog != nil {
		err = db.injectSemanticFault(FaultPointBeforeValueSync)
		if err == nil {
			err = db.valueLog.Sync()
		}
	}
	if err == nil {
		err = db.injectSemanticFault(FaultPointBeforeMainSync)
	}
	if err == nil {
		for _, dataFile := range db.pendingDurabilityFiles {
			if err = dataFile.Sync(); err != nil {
				break
			}
		}
	}
	if err == nil {
		err = db.ActiveFile.Sync()
	}
	latency := time.Since(start)
	db.lastSyncLatency = latency
	atomic.AddUint64(&db.metrics.syncs, 1)
	atomic.AddUint64(&db.metrics.totalSyncNanos, uint64(latency.Nanoseconds()))
	if err != nil {
		db.durabilityFailed = true
		db.durabilityFailureStreak++
		atomic.AddUint64(&db.metrics.syncErrors, 1)
		return err
	}
	if wasRetry {
		db.recordDurabilityRetrySuccess(db.durabilityFailureStreak)
	}
	db.durabilityFailed = false
	db.durabilityFailureStreak = 0
	for _, dataFile := range db.pendingDurabilityFiles {
		if closeErr := dataFile.Close(); closeErr != nil {
			atomic.AddUint64(&db.metrics.syncErrors, 1)
			return closeErr
		}
	}
	db.pendingDurabilityFiles = nil
	db.lastSyncAt = time.Now()
	db.dirtyBytes = 0
	db.dirtyCommits = 0
	return nil
}
