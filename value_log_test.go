package bitcask

import (
	"bytes"
	"testing"
)

func TestValuePointerEncodeDecode(t *testing.T) {
	ptr := valuePointer{FileID: 1, Offset: 42, Size: 1024, CRC: 7}
	got, err := decodeValuePointer(encodeValuePointer(ptr))
	if err != nil {
		t.Fatal(err)
	}
	if got != ptr {
		t.Fatalf("got %#v want %#v", got, ptr)
	}
}

func TestKVSeparationSmallValueInline(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 1024
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "kvsep", []byte("small"), []byte("small-value"), Persistent)
	record, err := db.getRecordFromKey([]byte("kvsep"), []byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	if record.H.Meta.Ds != DataStructureBPTree {
		t.Fatalf("got ds %d want inline bptree", record.H.Meta.Ds)
	}
	assertValue(t, db, "kvsep", []byte("small"), []byte("small-value"))
}

func TestKVSeparationLargeValueRoundTrip(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	defer db.Close()

	value := bytes.Repeat([]byte("x"), 1024)
	putValue(t, db, "kvsep", []byte("large"), value, Persistent)
	record, err := db.getRecordFromKey([]byte("kvsep"), []byte("large"))
	if err != nil {
		t.Fatal(err)
	}
	if record.H.Meta.Ds != DataStructureValuePointer {
		t.Fatalf("got ds %d want value pointer", record.H.Meta.Ds)
	}
	assertValue(t, db, "kvsep", []byte("large"), value)
}

func TestKVSeparationPersistsAfterReopen(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)

	value := bytes.Repeat([]byte("v"), 2048)
	putValue(t, db, "kvsep", []byte("large"), value, Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "kvsep", []byte("large"), value)
}

func TestKVSeparationMergePreservesLargeValue(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	defer db.Close()

	value := bytes.Repeat([]byte("m"), 2048)
	putValue(t, db, "kvsep", []byte("large"), value, Persistent)
	putValue(t, db, "kvsep", []byte("other"), []byte("small"), Persistent)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, "kvsep", []byte("large"), value)
}

func TestKVSeparationCorruptValueLogReturnsError(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)

	value := bytes.Repeat([]byte("c"), 1024)
	putValue(t, db, "kvsep", []byte("large"), value, Persistent)
	if err := db.valueLog.file.Truncate(1); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		_, err := tx.Get("kvsep", []byte("large"))
		if err == nil {
			t.Fatalf("expected corrupt value log error")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
}
