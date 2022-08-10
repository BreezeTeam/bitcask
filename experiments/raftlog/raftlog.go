package raftlog

import "errors"

var ErrConflict = errors.New("log conflict")

type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

type Log struct {
	entries []Entry
}

func (l *Log) Append(entries ...Entry) error {
	for _, entry := range entries {
		if entry.Index == 0 {
			return ErrConflict
		}
		if int(entry.Index) <= len(l.entries) {
			existing := l.entries[entry.Index-1]
			if existing.Term != entry.Term {
				l.entries = l.entries[:entry.Index-1]
			} else {
				continue
			}
		}
		if entry.Index != uint64(len(l.entries)+1) {
			return ErrConflict
		}
		l.entries = append(l.entries, Entry{Index: entry.Index, Term: entry.Term, Command: append([]byte(nil), entry.Command...)})
	}
	return nil
}

func (l *Log) LastIndex() uint64 {
	return uint64(len(l.entries))
}

func (l *Log) Entry(index uint64) (Entry, bool) {
	if index == 0 || index > uint64(len(l.entries)) {
		return Entry{}, false
	}
	entry := l.entries[index-1]
	entry.Command = append([]byte(nil), entry.Command...)
	return entry, true
}
