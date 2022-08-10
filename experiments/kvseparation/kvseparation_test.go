package kvseparation

import (
	"errors"
	"testing"
)

func TestStoreIndexesPointersIntoValueLog(t *testing.T) {
	store := NewStore()
	first := store.Put([]byte("key"), []byte("old"))
	second := store.Put([]byte("key"), []byte("new-value"))

	if first == second {
		t.Fatal("expected overwrite to create a new value-log pointer")
	}
	got, err := store.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new-value" {
		t.Fatalf("got %q want %q", got, "new-value")
	}
	pointers := store.LivePointers()
	if pointers["key"] != second {
		t.Fatalf("live pointer got %#v want %#v", pointers["key"], second)
	}
}

func TestStoreMissingKey(t *testing.T) {
	store := NewStore()
	if _, err := store.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v want %v", err, ErrNotFound)
	}
}
