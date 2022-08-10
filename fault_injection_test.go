package bitcask

import (
	"errors"
	"testing"
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
