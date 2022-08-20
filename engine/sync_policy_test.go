package bitcask

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSyncPolicyCompatibilityWithSyncEnable(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	if got := db.effectiveSyncPolicy(); got != SyncPolicyNone {
		t.Fatalf("got %v want %v", got, SyncPolicyNone)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	opt = newTestOptions(t)
	opt.SyncEnable = true
	db = openTestDB(t, opt)
	defer db.Close()
	if got := db.effectiveSyncPolicy(); got != SyncPolicyEveryCommit {
		t.Fatalf("got %v want %v", got, SyncPolicyEveryCommit)
	}
}

func TestSyncPolicyExplicitModeOverridesSyncEnable(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncEnable = true
	opt.SyncPolicy.Mode = SyncPolicyNone
	db := openTestDB(t, opt)
	defer db.Close()
	if got := db.effectiveSyncPolicy(); got != SyncPolicyNone {
		t.Fatalf("got %v want %v", got, SyncPolicyNone)
	}
}

func TestCommitSyncsOncePerTransaction(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncEnable = true
	db := openTestDB(t, opt)
	defer db.Close()

	if err := db.PutBatch("sync", []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, Persistent); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, "sync", []byte("k1"), []byte("v1"))
	assertValue(t, db, "sync", []byte("k2"), []byte("v2"))
}

func TestAdaptiveSyncPersistsAfterReopen(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1
	opt.SyncPolicy.AdaptiveMaxDelay = time.Second
	db := openTestDB(t, opt)

	if err := db.PutBatch("adaptive", []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, Persistent); err != nil {
		t.Fatal(err)
	}
	if db.dirtyBytes != 0 {
		t.Fatalf("dirty bytes got %d want 0 after adaptive sync", db.dirtyBytes)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "adaptive", []byte("k1"), []byte("v1"))
	assertValue(t, db, "adaptive", []byte("k2"), []byte("v2"))
}

func TestAdaptiveSyncLoopFlushesAtMaxDelayWithoutNewWrite(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = 20 * time.Millisecond
	db := openTestDB(t, opt)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		return tx.Put("adaptive", []byte("key"), []byte("value"), Persistent)
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for db.Metrics().Syncs == 0 {
		if time.Now().After(deadline) {
			t.Fatal("adaptive loop did not flush by max delay")
		}
		time.Sleep(time.Millisecond)
	}
	db.mu.RLock()
	dirtyBytes, dirtyCommits := db.dirtyBytes, db.dirtyCommits
	db.mu.RUnlock()
	if dirtyBytes != 0 || dirtyCommits != 0 {
		t.Fatalf("dirty state bytes=%d commits=%d", dirtyBytes, dirtyCommits)
	}
}

func TestAdaptiveSyncDirtyCommitLimit(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.DirtyCommitsLimit = 2
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = 2 * time.Hour
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "adaptive", []byte("first"), []byte("value"), Persistent)
	if db.Metrics().Syncs != 0 {
		t.Fatal("first commit unexpectedly synced")
	}
	putValue(t, db, "adaptive", []byte("second"), []byte("value"), Persistent)
	if db.Metrics().Syncs != 1 {
		t.Fatalf("syncs got %d want 1", db.Metrics().Syncs)
	}
}

func TestAdaptiveSyncCloseFlushesSeparatedValue(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = 2 * time.Hour
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	value := []byte("large-separated-value-for-adaptive-close")
	putValue(t, db, "adaptive", []byte("key"), value, Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "adaptive", []byte("key"), value)
}

func TestAdaptiveSyncCanDelayBelowThreshold(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = 2 * time.Hour
	db := openTestDB(t, opt)
	defer db.Close()

	if err := db.PutBatch("adaptive", []KV{{Key: []byte("k"), Value: []byte("v")}}, Persistent); err != nil {
		t.Fatal(err)
	}
	if db.dirtyBytes == 0 {
		t.Fatalf("expected adaptive policy to delay sync and retain dirty bytes")
	}
}

func TestGroupCommitSharesSyncAcrossWriters(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxDelay = 100 * time.Millisecond
	opt.SyncPolicy.GroupMaxWrites = 8
	db := openTestDB(t, opt)
	defer db.Close()

	const writers = 32
	start := make(chan struct{})
	errs := make(chan error, writers)
	var ready sync.WaitGroup
	ready.Add(writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			ready.Done()
			<-start
			errs <- db.Update(func(tx *Tx) error {
				return tx.Put("group", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
			})
		}()
	}
	ready.Wait()
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	metrics := db.Metrics()
	if metrics.Commits != writers {
		t.Fatalf("commits got %d want %d", metrics.Commits, writers)
	}
	if metrics.Syncs >= metrics.Commits {
		t.Fatalf("syncs %d did not coalesce %d commits", metrics.Syncs, metrics.Commits)
	}
}

func TestGroupCommitMetricsTrackEpochFrontier(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxWrites = 4
	opt.SyncPolicy.GroupMaxDelay = time.Second
	db := openTestDB(t, opt)
	defer db.Close()
	const writers = 8
	start := make(chan struct{})
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			<-start
			errs <- db.Update(func(tx *Tx) error {
				return tx.Put("frontier", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
			})
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	metrics := db.Metrics()
	if metrics.GroupEpochs == 0 || metrics.GroupWaiters != writers || metrics.GroupMaxSize == 0 || metrics.GroupMaxSize > 4 {
		t.Fatalf("unexpected group metrics: %+v", metrics)
	}
	if metrics.GroupLastEpoch != metrics.GroupEpochs || metrics.DurableFrontier != writers {
		t.Fatalf("unexpected epoch/frontier: %+v", metrics)
	}
}

func TestGroupCommitFailureDoesNotAdvanceDurableFrontier(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxWrites = 1
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SyncFailAfter = 0
	db := openTestDB(t, opt)
	defer db.Close()
	err := db.Update(func(tx *Tx) error {
		return tx.Put("frontier", []byte("key"), []byte("value"), Persistent)
	})
	if !errors.Is(err, ErrFaultInjectedSync) {
		t.Fatalf("got %v want sync error", err)
	}
	metrics := db.Metrics()
	if metrics.GroupEpochs != 0 || metrics.DurableFrontier != 0 {
		t.Fatalf("failed epoch advanced durability: %+v", metrics)
	}
}

func TestGroupCommitMetricsTrackSizeDistribution(t *testing.T) {
	db := &DB{}
	for i, waiters := range []int{1, 2, 3, 4, 5, 8, 9, 16} {
		db.recordGroupEpoch(uint64(i+1), uint64(i+1), waiters)
	}
	metrics := db.Metrics()
	if metrics.GroupSize1 != 1 || metrics.GroupSize2 != 1 || metrics.GroupSize3To4 != 2 || metrics.GroupSize5To8 != 2 || metrics.GroupSize9Plus != 2 {
		t.Fatalf("unexpected group distribution: %+v", metrics)
	}
	if metrics.GroupEpochs != 8 || metrics.GroupWaiters != 48 || metrics.GroupMaxSize != 16 {
		t.Fatalf("unexpected aggregate group metrics: %+v", metrics)
	}
}

func BenchmarkGroupEpochMetrics(b *testing.B) {
	db := &DB{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.recordGroupEpoch(uint64(i+1), uint64((i+1)*8), i%16+1)
	}
}

func BenchmarkDurabilityRetryMetrics(b *testing.B) {
	db := &DB{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.recordDurabilityRetrySuccess(i%5 + 1)
	}
}

func TestGroupCommitSyncErrorFansOutAndUnlocksWriters(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxDelay = time.Second
	opt.SyncPolicy.GroupMaxWrites = 4
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SyncFailAfter = 0
	db := openTestDB(t, opt)
	defer db.Close()

	const writers = 4
	start := make(chan struct{})
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			<-start
			errs <- db.Update(func(tx *Tx) error {
				return tx.Put("group", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
			})
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-errs; !errors.Is(err, ErrFaultInjectedSync) {
			t.Fatalf("writer %d got %v want %v", i, err, ErrFaultInjectedSync)
		}
	}
}

func TestGroupCommitCloseFlushesPendingWriter(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxDelay = time.Hour
	opt.SyncPolicy.GroupMaxWrites = 100
	db := openTestDB(t, opt)

	writerDone := make(chan error, 1)
	go func() {
		writerDone <- db.Update(func(tx *Tx) error {
			return tx.Put("group", []byte("pending"), []byte("value"), Persistent)
		})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		db.groupCommit.mu.Lock()
		pending := len(db.groupCommit.pending)
		db.groupCommit.mu.Unlock()
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not enter pending group")
		}
		time.Sleep(time.Millisecond)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "group", []byte("pending"), []byte("value"))
}

func TestGroupCommitRotationUsesEpochSync(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxWrites = 1
	db := openTestDB(t, opt)
	defer db.Close()
	value := []byte("01234567890123456789012345678901")
	if err := db.PutBatch("rotate", []KV{
		{Key: []byte("first"), Value: value},
		{Key: []byte("second"), Value: value},
	}, Persistent); err != nil {
		t.Fatal(err)
	}
	metrics := db.Metrics()
	if metrics.Syncs != 1 || metrics.GroupEpochs != 1 {
		t.Fatalf("rotation forced extra sync: %+v", metrics)
	}
	if len(db.pendingDurabilityFiles) != 0 {
		t.Fatalf("retained files not released: %d", len(db.pendingDurabilityFiles))
	}
}

func TestRotationSyncFailureRetainsFileForRetry(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeMainSync
	opt.FaultInjection.SemanticFailAfter = 0
	db := openTestDB(t, opt)
	value := []byte("01234567890123456789012345678901")
	err := db.PutBatch("rotate", []KV{
		{Key: []byte("first"), Value: value},
		{Key: []byte("second"), Value: value},
	}, Persistent)
	if !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want semantic sync error", err)
	}
	if len(db.pendingDurabilityFiles) == 0 {
		t.Fatal("failed sync discarded rotated file")
	}
	db.opt.FaultInjection.SemanticPoint = ""
	if err := db.syncDurabilityResources(); err != nil {
		t.Fatal(err)
	}
	if len(db.pendingDurabilityFiles) != 0 {
		t.Fatal("successful retry did not release rotated file")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "rotate", []byte("first"), value)
	assertValue(t, db, "rotate", []byte("second"), value)
}

func BenchmarkGroupCommitRotation(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	opt.SyncPolicy.GroupMaxWrites = 1
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := []byte("01234567890123456789012345678901")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		items := []KV{
			{Key: []byte(fmt.Sprintf("first-%09d", i)), Value: value},
			{Key: []byte(fmt.Sprintf("second-%09d", i)), Value: value},
		}
		if err := db.PutBatch("rotate", items, Persistent); err != nil {
			b.Fatal(err)
		}
	}
	metrics := db.Metrics()
	if metrics.Commits > 0 {
		b.ReportMetric(float64(metrics.Syncs)/float64(metrics.Commits), "syncs/commit")
	}
}

func TestDurabilityRetryMetricsTrackFailureStreakDistribution(t *testing.T) {
	db := &DB{}
	for _, failures := range []int{1, 2, 3, 4, 7} {
		db.recordDurabilityRetrySuccess(failures)
	}
	metrics := db.Metrics()
	if metrics.DurabilityRetryOK != 5 || metrics.DurabilityRetryAfter1 != 1 || metrics.DurabilityRetryAfter2 != 1 || metrics.DurabilityRetryAfter3 != 1 || metrics.DurabilityRetryAfter4P != 2 {
		t.Fatalf("unexpected retry distribution: %+v", metrics)
	}
}

func TestFlushRetryMetricsTrackRecoveredFailureStreak(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeMainSync
	opt.FaultInjection.SemanticFailAfter = 0
	db := openTestDB(t, opt)
	value := []byte("01234567890123456789012345678901")
	err := db.PutBatch("flush-streak", []KV{
		{Key: []byte("first"), Value: value},
		{Key: []byte("second"), Value: value},
	}, Persistent)
	if !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want semantic fault", err)
	}
	if err := db.Flush(); !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("retry got %v want semantic fault", err)
	}
	db.opt.FaultInjection.SemanticPoint = ""
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	metrics := db.Metrics()
	if metrics.DurabilityRetries != 2 || metrics.DurabilityRetryOK != 1 || metrics.DurabilityRetryAfter2 != 1 {
		t.Fatalf("unexpected recovered retry streak metrics: %+v", metrics)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "flush-streak", []byte("first"), value)
	assertValue(t, db, "flush-streak", []byte("second"), value)
}

func TestFlushRetriesFailedDurabilityResources(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeMainSync
	opt.FaultInjection.SemanticFailAfter = 0
	db := openTestDB(t, opt)
	value := []byte("01234567890123456789012345678901")
	err := db.PutBatch("flush", []KV{
		{Key: []byte("first"), Value: value},
		{Key: []byte("second"), Value: value},
	}, Persistent)
	if !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want semantic fault", err)
	}
	db.opt.FaultInjection.SemanticPoint = ""
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	metrics := db.Metrics()
	if metrics.DurabilityRetries != 1 || metrics.DurabilityRetryOK != 1 {
		t.Fatalf("unexpected retry metrics: %+v", metrics)
	}
	if len(db.pendingDurabilityFiles) != 0 || db.durabilityFailed {
		t.Fatalf("retry did not clear state files=%d failed=%t", len(db.pendingDurabilityFiles), db.durabilityFailed)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "flush", []byte("first"), value)
	assertValue(t, db, "flush", []byte("second"), value)
}

func TestFlushPersistsDelayedAdaptiveSeparatedValue(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyAdaptive
	opt.SyncPolicy.DirtyBytesLimit = 1024 * 1024
	opt.SyncPolicy.AdaptiveMinDelay = time.Hour
	opt.SyncPolicy.AdaptiveMaxDelay = 2 * time.Hour
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	value := []byte("large-separated-value-for-explicit-flush")
	putValue(t, db, "flush", []byte("key"), value, Persistent)
	if db.dirtyCommits == 0 {
		t.Fatal("expected delayed dirty commit")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if db.dirtyCommits != 0 || db.dirtyBytes != 0 {
		t.Fatalf("flush left dirty state commits=%d bytes=%d", db.dirtyCommits, db.dirtyBytes)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "flush", []byte("key"), value)
}

func TestFlushRejectsClosedDB(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("got %v want %v", err, ErrDBClosed)
	}
}

func BenchmarkExplicitFlush(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyNone
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := []byte("value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error { return tx.Put("flush", []byte(fmt.Sprintf("key-%09d", i)), value, Persistent) }); err != nil {
			b.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDurabilityPolicyPersistsTransactionAcrossRotation(t *testing.T) {
	for _, mode := range []SyncPolicyMode{SyncPolicyEveryCommit, SyncPolicyGroupCommit} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			opt := newTestOptions(t)
			opt.SegmentSize = 128
			opt.SyncPolicy.Mode = mode
			opt.SyncPolicy.GroupMaxWrites = 1
			db := openTestDB(t, opt)
			value := []byte("01234567890123456789012345678901")
			if err := db.PutBatch("rotate", []KV{
				{Key: []byte("first"), Value: value},
				{Key: []byte("second"), Value: value},
			}, Persistent); err != nil {
				t.Fatal(err)
			}
			if db.Stats().DataFileCount < 2 {
				t.Fatal("expected transaction to rotate data files")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db = openTestDB(t, opt)
			defer db.Close()
			assertValue(t, db, "rotate", []byte("first"), value)
			assertValue(t, db, "rotate", []byte("second"), value)
		})
	}
}

func TestDurabilityPolicyPersistsSeparatedValue(t *testing.T) {
	for _, mode := range []SyncPolicyMode{SyncPolicyEveryCommit, SyncPolicyGroupCommit} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			opt := newTestOptions(t)
			opt.SegmentSize = 1024 * 1024
			opt.SyncPolicy.Mode = mode
			opt.SyncPolicy.GroupMaxWrites = 1
			opt.KVSeparation.Enable = true
			opt.KVSeparation.Threshold = 16
			db := openTestDB(t, opt)
			value := []byte("large-separated-value-that-must-be-durable")
			putValue(t, db, "durable", []byte("key"), value, Persistent)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db = openTestDB(t, opt)
			defer db.Close()
			assertValue(t, db, "durable", []byte("key"), value)
		})
	}
}

func TestGroupCommitPersistsCommittedTransaction(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyGroupCommit
	db := openTestDB(t, opt)

	if err := db.PutBatch("group", []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, Persistent); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "group", []byte("k1"), []byte("v1"))
	assertValue(t, db, "group", []byte("k2"), []byte("v2"))
}
