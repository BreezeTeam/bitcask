package raftlog

import (
	"errors"
	"testing"
)

func TestLogAppendAndReadCopiesCommand(t *testing.T) {
	var log Log
	command := []byte("set a 1")
	if err := log.Append(Entry{Index: 1, Term: 1, Command: command}); err != nil {
		t.Fatal(err)
	}
	command[0] = 'X'

	entry, ok := log.Entry(1)
	if !ok {
		t.Fatal("expected entry")
	}
	if string(entry.Command) != "set a 1" {
		t.Fatalf("got %q want %q", entry.Command, "set a 1")
	}
	entry.Command[0] = 'Y'

	entry, ok = log.Entry(1)
	if !ok {
		t.Fatal("expected entry")
	}
	if string(entry.Command) != "set a 1" {
		t.Fatalf("got %q want %q", entry.Command, "set a 1")
	}
}

func TestLogRejectsGap(t *testing.T) {
	var log Log
	if err := log.Append(Entry{Index: 2, Term: 1, Command: []byte("gap")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v want %v", err, ErrConflict)
	}
}

func TestLogTruncatesConflictingSuffix(t *testing.T) {
	var log Log
	if err := log.Append(
		Entry{Index: 1, Term: 1, Command: []byte("a")},
		Entry{Index: 2, Term: 1, Command: []byte("b")},
		Entry{Index: 3, Term: 1, Command: []byte("c")},
	); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(Entry{Index: 2, Term: 2, Command: []byte("new-b")}); err != nil {
		t.Fatal(err)
	}
	if log.LastIndex() != 2 {
		t.Fatalf("got last index %d want 2", log.LastIndex())
	}
	entry, ok := log.Entry(2)
	if !ok {
		t.Fatal("expected entry")
	}
	if entry.Term != 2 || string(entry.Command) != "new-b" {
		t.Fatalf("unexpected entry: %#v", entry)
	}
}
