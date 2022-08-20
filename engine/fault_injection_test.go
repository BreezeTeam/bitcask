package bitcask

import (
	"errors"
	"testing"
	"time"
)

func TestFaultInjectionWriteFailsBeforeCommitMarker(t *testing.T) {
	opt := newTestOptions(t)
	opt.FaultInjection.Enable = true
	opt.FaultInjection.WriteFailAfter = 1
	db := openTestDB(t, opt)
	defer db.Close()

	err := db.PutBatch("fault", []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, Persistent)
	if !errors.Is(err, ErrFaultInjectedWrite) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedWrite)
	}

	assertNotFound(t, db, "fault", []byte("k1"))
	assertNotFound(t, db, "fault", []byte("k2"))
}

func TestFaultInjectionSyncFailureReturnsCommitError(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SyncFailAfter = 0
	db := openTestDB(t, opt)
	defer db.Close()

	err := db.Update(func(tx *Tx) error {
		return tx.Put("fault", []byte("k"), []byte("v"), Persistent)
	})
	if !errors.Is(err, ErrFaultInjectedSync) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedSync)
	}
	if got := db.Metrics().SyncErrors; got != 1 {
		t.Fatalf("sync errors got %d want 1", got)
	}
}

func TestDirectCommitFailureClosesTransaction(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Options)
		wantErr   error
	}{
		{
			name: "write",
			configure: func(opt *Options) {
				opt.FaultInjection.WriteFailAfter = 0
			},
			wantErr: ErrFaultInjectedWrite,
		},
		{
			name: "short-write",
			configure: func(opt *Options) {
				opt.FaultInjection.ShortWriteAfter = 0
			},
			wantErr: ErrFaultInjectedShortWrite,
		},
		{
			name: "sync",
			configure: func(opt *Options) {
				opt.SyncPolicy.Mode = SyncPolicyEveryCommit
				opt.FaultInjection.SyncFailAfter = 0
			},
			wantErr: ErrFaultInjectedSync,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := newTestOptions(t)
			opt.FaultInjection.Enable = true
			tt.configure(&opt)
			db := openTestDB(t, opt)
			defer db.Close()

			tx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("fault", []byte("failed"), []byte("value"), Persistent); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("commit got %v want %v", err, tt.wantErr)
			}
			if err := tx.Commit(); !errors.Is(err, ErrTxClosed) {
				t.Fatalf("second commit got %v want %v", err, ErrTxClosed)
			}
			if err := tx.Rollback(); !errors.Is(err, ErrTxClosed) {
				t.Fatalf("rollback got %v want %v", err, ErrTxClosed)
			}

			done := make(chan error, 1)
			go func() {
				next, err := db.Begin(true)
				if err == nil {
					err = next.Rollback()
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("subsequent transaction: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("subsequent transaction blocked after failed commit")
			}
		})
	}
}

func TestManagedCommitFailurePreservesOriginalError(t *testing.T) {
	opt := newTestOptions(t)
	opt.FaultInjection.Enable = true
	opt.FaultInjection.WriteFailAfter = 0
	db := openTestDB(t, opt)
	defer db.Close()

	err := db.Update(func(tx *Tx) error {
		return tx.Put("fault", []byte("k"), []byte("v"), Persistent)
	})
	if !errors.Is(err, ErrFaultInjectedWrite) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedWrite)
	}
}

func TestSemanticFaultBeforeCommitMarkerKeepsTransactionUncommitted(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeCommitMarker
	opt.FaultInjection.SemanticFailAfter = 0
	db := openTestDB(t, opt)
	err := db.PutBatch("semantic", []KV{
		{Key: []byte("first"), Value: []byte("value-1")},
		{Key: []byte("second"), Value: []byte("value-2")},
	}, Persistent)
	if !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedSemantic)
	}
	assertNotFound(t, db, "semantic", []byte("first"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertNotFound(t, db, "semantic", []byte("first"))
	assertNotFound(t, db, "semantic", []byte("second"))
}

func TestSemanticFaultDurabilityPoints(t *testing.T) {
	tests := []struct {
		name      string
		point     string
		separated bool
	}{
		{name: "main-sync", point: FaultPointBeforeMainSync},
		{name: "value-append", point: FaultPointBeforeValueAppend, separated: true},
		{name: "value-sync", point: FaultPointBeforeValueSync, separated: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := newTestOptions(t)
			opt.SegmentSize = 1024 * 1024
			opt.SyncPolicy.Mode = SyncPolicyEveryCommit
			opt.FaultInjection.Enable = true
			opt.FaultInjection.SemanticPoint = tt.point
			opt.FaultInjection.SemanticFailAfter = 0
			opt.KVSeparation.Enable = tt.separated
			opt.KVSeparation.Threshold = 16
			db := openTestDB(t, opt)
			defer db.Close()
			err := db.Update(func(tx *Tx) error {
				return tx.Put("semantic", []byte("key"), []byte("large-value-for-semantic-fault"), Persistent)
			})
			if !errors.Is(err, ErrFaultInjectedSemantic) {
				t.Fatalf("got %v want %v", err, ErrFaultInjectedSemantic)
			}
		})
	}
}

func TestSemanticFaultOccurrenceIsDeterministic(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeEntry
	opt.FaultInjection.SemanticFailAfter = 1
	db := openTestDB(t, opt)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		return tx.Put("semantic", []byte("first"), []byte("value"), Persistent)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.Put("semantic", []byte("second"), []byte("value"), Persistent)
	}); !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want second occurrence failure", err)
	}
}

func BenchmarkSemanticFaultPointDisabled(b *testing.B) {
	opt := DefaultOptions
	db := &DB{opt: opt}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.injectSemanticFault(FaultPointBeforeEntry)
	}
}

func BenchmarkSemanticFaultPointEnabledNoFault(b *testing.B) {
	opt := DefaultOptions
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeEntry
	opt.FaultInjection.SemanticFailAfter = int64(b.N + 1)
	opt.faultState = &faultInjectionState{}
	db := &DB{opt: opt}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.injectSemanticFault(FaultPointBeforeEntry)
	}
}

func TestFaultInjectionRecoveryIgnoresUncommittedTail(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	if err := db.Update(func(tx *Tx) error {
		return tx.Put("fault", []byte("committed"), []byte("v1"), Persistent)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	uncommitted := newTestEntry("fault", []byte("tail"), []byte("v2"), 100, UnCommitted)
	appendEntryToDataFile(t, opt, uncommitted)

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "fault", []byte("committed"), []byte("v1"))
	assertNotFound(t, db, "fault", []byte("tail"))
}

func TestFaultInjectionCorruptEntryFailsOpen(t *testing.T) {
	opt := newTestOptions(t)
	opt.FaultInjection.Enable = true
	opt.FaultInjection.CorruptAfterWrite = true
	db := openTestDB(t, opt)

	err := db.Update(func(tx *Tx) error {
		return tx.Put("fault", []byte("k"), []byte("v"), Persistent)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(opt)
	if err == nil {
		t.Fatalf("expected open to fail on corrupt entry")
	}
}

func appendEntryToDataFile(t *testing.T, opt Options, entry *Entry) {
	t.Helper()
	dataFile, err := NewDataFile(opt.Dir, 0, opt.SegmentSize, opt.RWMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := dataFile.setActiveFileWriteOff(); err != nil {
		t.Fatal(err)
	}
	if _, err := dataFile.WriteAt(entry.Encode(), dataFile.writeOff); err != nil {
		t.Fatal(err)
	}
	if err := dataFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFaultInjectionMergeDoesNotLoseLiveData(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	payload := []byte("01234567890123456789012345678901")
	putValue(t, db, "merge", []byte("k1"), payload, Persistent)
	putValue(t, db, "merge", []byte("k2"), payload, Persistent)
	putValue(t, db, "merge", []byte("k1"), []byte("live"), Persistent)
	deleteKey(t, db, "merge", []byte("k2"))

	if db.Stats().DataFileCount < 2 {
		t.Fatalf("expected at least 2 data files before merge")
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, "merge", []byte("k1"), []byte("live"))
	assertNotFound(t, db, "merge", []byte("k2"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	opt.FaultInjection.Enable = true
	opt.FaultInjection.ReadCorruptAfter = 0
	if _, err := Open(opt); err == nil {
		t.Fatalf("expected injected recovery read corruption")
	}
}
