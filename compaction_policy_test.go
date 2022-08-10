package bitcask

import "testing"

func TestPickMergeFileIDsByFileIDKeepsAllFiles(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	got := db.pickMergeFileIDs([]int{2, 0, 1})
	want := []int{0, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestPickMergeFileIDsByGarbageRatioCanSelectSingleCandidate(t *testing.T) {
	opt := newTestOptions(t)
	opt.Compaction.Mode = CompactionByGarbageRatio
	opt.Compaction.MinGarbageRatio = 0.1
	db := openTestDB(t, opt)
	defer db.Close()

	got := db.pickMergeFileIDs([]int{0, 1, 2})
	if len(got) != 1 {
		t.Fatalf("got %v want single candidate", got)
	}
}

func TestDBMergePolicyPreservesLiveData(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.Mode = CompactionByGarbageRatio
	opt.Compaction.MinGarbageRatio = 0.1
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "merge", []byte("k1"), []byte("v1"), Persistent)
	putValue(t, db, "merge", []byte("k2"), []byte("v2"), Persistent)
	putValue(t, db, "merge", []byte("k3"), []byte("v3"), Persistent)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, "merge", []byte("k3"), []byte("v3"))
}

func TestDBMergePolicyRemovesDeletedAndExpired(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.Mode = CompactionByGarbageRatio
	opt.Compaction.MinGarbageRatio = 0.1
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "merge", []byte("deleted"), []byte("v1"), Persistent)
	deleteKey(t, db, "merge", []byte("deleted"))
	putValueWithTimestamp(t, db, "merge", []byte("expired"), []byte("v2"), 1, uint64(1))
	putValue(t, db, "merge", []byte("live"), []byte("v3"), Persistent)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	assertNotFound(t, db, "merge", []byte("deleted"))
	assertNotFound(t, db, "merge", []byte("expired"))
	assertValue(t, db, "merge", []byte("live"), []byte("v3"))
}
