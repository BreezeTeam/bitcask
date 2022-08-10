package bitcask

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func newTestOptions(t *testing.T) Options {
	t.Helper()
	opt := DefaultOptions
	opt.Dir = t.TempDir()
	return opt
}

func openTestDB(t *testing.T, opt Options) *DB {
	t.Helper()
	db, err := Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func putValue(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		return tx.Put(bucket, key, value, ttl)
	}); err != nil {
		t.Fatal(err)
	}
}

func putValueWithTimestamp(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32, timestamp uint64) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		return tx.PutWithTimestamp(bucket, key, value, ttl, timestamp)
	}); err != nil {
		t.Fatal(err)
	}
}

func deleteKey(t *testing.T, db *DB, bucket string, key []byte) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		return tx.Delete(bucket, key)
	}); err != nil {
		t.Fatal(err)
	}
}

func writeEntry(t *testing.T, opt Options, entry *Entry) {
	t.Helper()
	writeEntries(t, opt, entry)
}

func writeEntries(t *testing.T, opt Options, entries ...*Entry) {
	t.Helper()
	dataFile, err := NewDataFile(opt.Dir, 0, opt.SegmentSize, opt.RWMode)
	if err != nil {
		t.Fatal(err)
	}
	var off int64
	for _, entry := range entries {
		if _, err := dataFile.WriteAt(entry.Encode(), off); err != nil {
			t.Fatal(err)
		}
		off += entry.Size()
	}
	if err := dataFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func newTestEntry(bucket string, key, value []byte, txID uint64, status uint16) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(value)),
			Timestamp:  uint64(time.Now().Unix()),
			Flag:       DataSetFlag,
			TTL:        Persistent,
			Bucket:     []byte(bucket),
			BucketSize: uint32(len(bucket)),
			Status:     status,
			Ds:         DataStructureBPTree,
			TxID:       txID,
		},
	}
}

func assertValue(t *testing.T, db *DB, bucket string, key, want []byte) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		entry, err := tx.Get(bucket, key)
		if err != nil {
			return err
		}
		if got := string(entry.Value); got != string(want) {
			t.Fatalf("got %s want %s", got, string(want))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertNotFound(t *testing.T, db *DB, bucket string, key []byte) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		_, err := tx.Get(bucket, key)
		if err == nil {
			t.Fatalf("expected key %q in bucket %q to be missing", string(key), bucket)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEntryEncodeToMatchesEncode(t *testing.T) {
	entry := newTestEntry("bucket", []byte("key"), []byte("value"), 1, Committed)
	encoded := entry.Encode()
	buf := make([]byte, 0, entry.Size())
	reused := entry.EncodeTo(buf)
	if string(reused) != string(encoded) {
		t.Fatalf("EncodeTo bytes differ from Encode")
	}
}

func TestDB_Basic(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")

	putValue(t, db, bucket, key, val, Persistent)
	assertValue(t, db, bucket, key, val)

	deleteKey(t, db, bucket, key)
	assertNotFound(t, db, bucket, key)

	val = []byte("val001")
	putValue(t, db, bucket, key, val, Persistent)
	assertValue(t, db, bucket, key, val)
}

func TestDB_TempDirsAreIsolated(t *testing.T) {
	opt1 := newTestOptions(t)
	db1 := openTestDB(t, opt1)
	defer db1.Close()

	opt2 := newTestOptions(t)
	db2 := openTestDB(t, opt2)
	defer db2.Close()

	bucket := "bucket"
	key := []byte("same-key")

	putValue(t, db1, bucket, key, []byte("db1"), Persistent)
	putValue(t, db2, bucket, key, []byte("db2"), Persistent)

	assertValue(t, db1, bucket, key, []byte("db1"))
	assertValue(t, db2, bucket, key, []byte("db2"))
}

func TestDB_CloseReopenPersistence(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)

	bucket := "bucket"
	key := []byte("key")
	putValue(t, db, bucket, key, []byte("v1"), Persistent)
	putValue(t, db, bucket, key, []byte("v2"), Persistent)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, bucket, key, []byte("v2"))
}

func TestDB_CloseRejectsFurtherTransactions(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error { return nil }); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("got %v want %v", err, ErrDBClosed)
	}
}

func TestDB_PutBatchPersistsAllKeys(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	items := []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	if err := db.PutBatch("batch", items, Persistent); err != nil {
		t.Fatal(err)
	}

	for _, item := range items {
		assertValue(t, db, "batch", item.Key, item.Value)
	}
}

func TestDB_PutBatchRejectsEmptyKey(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	err := db.PutBatch("batch", []KV{{Key: []byte("ok"), Value: []byte("v")}, {Key: nil, Value: []byte("bad")}}, Persistent)
	if !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("got %v want %v", err, ErrKeyEmpty)
	}
	assertNotFound(t, db, "batch", []byte("ok"))
}

func TestDB_PutBatchPersistsAfterReopen(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)

	items := []KV{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	if err := db.PutBatch("batch", items, Persistent); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	for _, item := range items {
		assertValue(t, db, "batch", item.Key, item.Value)
	}
}

func TestTx_PutBatchRejectsReadOnlyTx(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		err := tx.PutBatch("batch", []KV{{Key: []byte("k"), Value: []byte("v")}}, Persistent)
		if !errors.Is(err, ErrTxNotWritable) {
			t.Fatalf("got %v want %v", err, ErrTxNotWritable)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_DeletePersistsAfterReopen(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)

	bucket := "bucket"
	key := []byte("key")
	putValue(t, db, bucket, key, []byte("value"), Persistent)
	deleteKey(t, db, bucket, key)
	assertNotFound(t, db, bucket, key)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertNotFound(t, db, bucket, key)
}

func TestDB_TTL(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	bucket := "ttl"
	oldTimestamp := uint64(time.Now().Add(-10 * time.Second).Unix())
	nowTimestamp := uint64(time.Now().Unix())

	putValueWithTimestamp(t, db, bucket, []byte("expired"), []byte("expired-value"), 1, oldTimestamp)
	assertNotFound(t, db, bucket, []byte("expired"))

	putValueWithTimestamp(t, db, bucket, []byte("persistent"), []byte("persistent-value"), Persistent, oldTimestamp)
	assertValue(t, db, bucket, []byte("persistent"), []byte("persistent-value"))

	putValueWithTimestamp(t, db, bucket, []byte("live"), []byte("live-value"), 60, nowTimestamp)
	assertValue(t, db, bucket, []byte("live"), []byte("live-value"))
}

func TestDB_RecoveryIgnoresUncommittedEntry(t *testing.T) {
	opt := newTestOptions(t)
	bucket := "recovery"
	key := []byte("uncommitted")
	writeEntry(t, opt, newTestEntry(bucket, key, []byte("value"), 1, UnCommitted))

	db := openTestDB(t, opt)
	defer db.Close()
	assertNotFound(t, db, bucket, key)
}

func TestDB_RecoveryUsesCommitMarkerForWholeTransaction(t *testing.T) {
	opt := newTestOptions(t)
	bucket := "recovery"
	key1 := []byte("key1")
	key2 := []byte("key2")
	writeEntries(t, opt,
		newTestEntry(bucket, key1, []byte("value1"), 1, UnCommitted),
		newTestEntry(bucket, key2, []byte("value2"), 1, Committed),
	)

	db := openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, bucket, key1, []byte("value1"))
	assertValue(t, db, bucket, key2, []byte("value2"))
}

func TestDB_OpenFailsOnCorruptedEntry(t *testing.T) {
	opt := newTestOptions(t)
	entry := newTestEntry("recovery", []byte("corrupt"), []byte("value"), 1, Committed)
	writeEntry(t, opt, entry)

	file, err := os.OpenFile(getDataFilePath(opt.Dir, 0), os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(DataEntryHeaderSize)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if db, err := Open(opt); err == nil {
		db.Close()
		t.Fatal("expected Open to fail on corrupted entry")
	}
}

func TestTx_RangeAndPrefixFilterInvisibleRecords(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()

	bucket := "scan"
	putValue(t, db, bucket, []byte("a1"), []byte("v-a1"), Persistent)
	putValue(t, db, bucket, []byte("a2"), []byte("v-a2"), Persistent)
	putValue(t, db, bucket, []byte("a3"), []byte("v-a3"), Persistent)
	putValue(t, db, bucket, []byte("b1"), []byte("v-b1"), Persistent)
	deleteKey(t, db, bucket, []byte("a2"))
	putValueWithTimestamp(t, db, bucket, []byte("a4"), []byte("v-a4"), 1, uint64(time.Now().Add(-10*time.Second).Unix()))

	if err := db.View(func(tx *Tx) error {
		kvs, err := tx.Range(bucket, []byte("a1"), []byte("a4"))
		if err != nil {
			return err
		}
		assertKVs(t, kvs, []KV{{Key: []byte("a1"), Value: []byte("v-a1")}, {Key: []byte("a3"), Value: []byte("v-a3")}})

		kvs, _, err = tx.Prefix(bucket, []byte("a"), 0, ScanNoLimit)
		if err != nil {
			return err
		}
		assertKVs(t, kvs, []KV{{Key: []byte("a1"), Value: []byte("v-a1")}, {Key: []byte("a3"), Value: []byte("v-a3")}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertKVs(t *testing.T, got, want []KV) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d kvs want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("got kv[%d]=%q/%q want %q/%q", i, string(got[i].Key), string(got[i].Value), string(want[i].Key), string(want[i].Value))
		}
	}
}

func TestDB_MergeRejectsClosedDB(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Merge(); !errors.Is(err, ErrDBClosed) {
		t.Fatalf("got %v want %v", err, ErrDBClosed)
	}
}

func TestDB_Merge_For_string(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 100
	db := openTestDB(t, opt)

	bucket := "test_merge"
	liveKey := []byte("key_" + fmt.Sprintf("%07d", 1))
	deletedKey := []byte("key_" + fmt.Sprintf("%07d", 2))
	expiredKey := []byte("key_" + fmt.Sprintf("%07d", 3))

	putValue(t, db, bucket, liveKey, []byte("old-value-old-value-old-value"), Persistent)
	putValue(t, db, bucket, liveKey, []byte("new-value-new-value-new-value"), Persistent)
	putValue(t, db, bucket, deletedKey, []byte("deleted-value-deleted-value"), Persistent)
	deleteKey(t, db, bucket, deletedKey)
	putValueWithTimestamp(t, db, bucket, expiredKey, []byte("expired-value-expired-value"), 1, uint64(time.Now().Add(-10*time.Second).Unix()))

	assertValue(t, db, bucket, liveKey, []byte("new-value-new-value-new-value"))
	assertNotFound(t, db, bucket, deletedKey)
	assertNotFound(t, db, bucket, expiredKey)

	beforeMerge := db.Stats()
	if beforeMerge.DataFileCount < 2 {
		t.Fatalf("merge test needs multiple data files, got %d", beforeMerge.DataFileCount)
	}
	if beforeMerge.ValidKeyCount != 1 {
		t.Fatalf("got %d valid keys before merge, want 1", beforeMerge.ValidKeyCount)
	}

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}

	afterMerge := db.Stats()
	if afterMerge.IsMerging {
		t.Fatal("merge flag should be cleared after merge")
	}
	if afterMerge.ValidKeyCount != 1 {
		t.Fatalf("got %d valid keys after merge, want 1", afterMerge.ValidKeyCount)
	}

	assertValue(t, db, bucket, liveKey, []byte("new-value-new-value-new-value"))
	assertNotFound(t, db, bucket, deletedKey)
	assertNotFound(t, db, bucket, expiredKey)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, bucket, liveKey, []byte("new-value-new-value-new-value"))
	assertNotFound(t, db, bucket, deletedKey)
	assertNotFound(t, db, bucket, expiredKey)
}

/////////////////
// getMaxFileIDAndFileIDs getMaxFileIDAndFileIDs2 基准测试
/////////////////

func BenchmarkDB_getMaxFileIDAndFileIDs(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getMaxFileIDAndFileIDs()
	}
}

func BenchmarkDB_getMaxFileIDAndFileIDs2(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getMaxFileIDAndFileIDs2()
	}
}
