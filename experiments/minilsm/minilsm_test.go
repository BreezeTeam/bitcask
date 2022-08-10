package minilsm

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDBPutGetCopiesData(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("key")
	value := []byte("value")
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'

	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "value" {
		t.Fatalf("got %q want %q", got, "value")
	}
	got[0] = 'Y'

	got, err = db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "value" {
		t.Fatalf("got %q want %q", got, "value")
	}
}

func TestDBReplaysWALOnOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "2" {
		t.Fatalf("got %q want %q", got, "2")
	}
}

func TestDBFlushPersistsSSTableAndClearsWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal size got %d want 0", info.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "1" {
		t.Fatalf("got %q want %q", got, "1")
	}
	got, err = db.Get([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "2" {
		t.Fatalf("got %q want %q", got, "2")
	}
}

func TestDBWALOverridesFlushedSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key"), []byte("sst")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("key"), []byte("wal")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "wal" {
		t.Fatalf("got %q want %q", got, "wal")
	}
}

func TestDBGetMissingKey(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v want %v", err, ErrNotFound)
	}
}
