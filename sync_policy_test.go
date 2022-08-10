package bitcask

import (
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
