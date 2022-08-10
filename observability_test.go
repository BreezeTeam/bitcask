package bitcask

import "testing"

func TestMetricsCountsCommittedEntries(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	if err := db.PutBatch("metrics", []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}, Persistent); err != nil {
		t.Fatal(err)
	}

	metrics := db.Metrics()
	if metrics.Commits != 1 {
		t.Fatalf("commits got %d want 1", metrics.Commits)
	}
	if metrics.EntriesWritten != 2 {
		t.Fatalf("entries got %d want 2", metrics.EntriesWritten)
	}
	if metrics.BytesWritten == 0 {
		t.Fatalf("expected bytes written to be recorded")
	}
}

func TestMetricsCountsSyncs(t *testing.T) {
	opt := newTestOptions(t)
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "metrics", []byte("k"), []byte("v"), Persistent)
	metrics := db.Metrics()
	if metrics.Syncs != 1 {
		t.Fatalf("syncs got %d want 1", metrics.Syncs)
	}
	if metrics.TotalSyncNanos == 0 {
		t.Fatalf("expected sync latency to be recorded")
	}
}

func TestMetricsSnapshotIsStable(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "metrics", []byte("k"), []byte("v"), Persistent)
	first := db.Metrics()
	second := db.Metrics()
	if first != second {
		t.Fatalf("metrics snapshot changed without writes: first=%#v second=%#v", first, second)
	}
}

func TestMetricsCountsRotations(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()

	value := []byte("01234567890123456789012345678901")
	putValue(t, db, "metrics", []byte("k1"), value, Persistent)
	putValue(t, db, "metrics", []byte("k2"), value, Persistent)

	metrics := db.Metrics()
	if metrics.Rotations == 0 {
		t.Fatalf("expected at least one rotation")
	}
}

func TestMetricsCountsRecovery(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	putValue(t, db, "metrics", []byte("k"), []byte("v"), Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	metrics := db.Metrics()
	if metrics.RecoveryEntries == 0 {
		t.Fatalf("expected recovery entries to be recorded")
	}
	if metrics.RecoveryCommittedTx == 0 {
		t.Fatalf("expected recovery committed tx count to be recorded")
	}
}
