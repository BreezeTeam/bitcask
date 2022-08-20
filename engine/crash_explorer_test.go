package bitcask

import (
	"errors"
	"os"
	"testing"
	"time"

	faultmodel "github.com/BreezeTeam/bitcask/experiments/fault"
)

func TestCrashExplorerRecoveryScenarios(t *testing.T) {
	const logicalNow = uint64(100)
	for _, scenario := range faultmodel.EnumerateRecoveryScenarios() {
		scenario := scenario
		t.Run(scenario.ID(), func(t *testing.T) {
			opt := newTestOptions(t)
			entries := make([]*Entry, 0, len(scenario.Operations))
			for _, operation := range scenario.Operations {
				entry := newTestEntry("crash", []byte(operation.Key), []byte(operation.Value), operation.TxID, UnCommitted)
				if operation.Committed {
					entry.Meta.Status = Committed
				}
				if operation.Kind == faultmodel.OperationDelete {
					entry.Meta.Flag = DataDeleteFlag
					entry.Value = nil
					entry.Meta.ValueSize = 0
				}
				if operation.ExpiresAt > 0 {
					entry.Meta.TTL = 1
					if operation.ExpiresAt <= logicalNow {
						entry.Meta.Timestamp = uint64(time.Now().Add(-2 * time.Second).Unix())
					} else {
						entry.Meta.Timestamp = uint64(time.Now().Unix())
						entry.Meta.TTL = uint32(time.Hour / time.Second)
					}
				}
				entries = append(entries, entry)
			}
			if len(entries) > 0 {
				writeEntries(t, opt, entries...)
			}

			db := openTestDB(t, opt)
			defer db.Close()
			replayed := faultmodel.ReplayScenario(scenario, logicalNow)
			for key, expected := range replayed.Expected {
				if expected.Found {
					assertValue(t, db, "crash", []byte(key), []byte(expected.Value))
					continue
				}
				assertNotFound(t, db, "crash", []byte(key))
			}
			for _, operation := range scenario.Operations {
				if _, ok := replayed.Expected[operation.Key]; !ok {
					assertNotFound(t, db, "crash", []byte(operation.Key))
				}
			}
		})
	}
}

func TestCrashExplorerTornEntryBoundariesFailSafely(t *testing.T) {
	entry := newTestEntry("bucket", []byte("key"), []byte("value"), 1, Committed)
	encoded := entry.Encode()
	scenarios := faultmodel.EnumerateTornWriteScenarios(int(DataEntryHeaderSize), len(entry.Meta.Bucket), len(entry.Key), len(entry.Value))
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.ID(), func(t *testing.T) {
			opt := newTestOptions(t)
			if err := os.WriteFile(getDataFilePath(opt.Dir, 0), encoded[:scenario.Offset], 0644); err != nil {
				t.Fatal(err)
			}
			db, err := Open(opt)
			if err == nil {
				defer db.Close()
				assertNotFound(t, db, "bucket", []byte("key"))
			}
		})
	}
}

func TestCrashExplorerTornCommitMarkerKeepsEarlierEntryHidden(t *testing.T) {
	first := newTestEntry("bucket", []byte("first"), []byte("value-1"), 1, UnCommitted)
	marker := newTestEntry("bucket", []byte("second"), []byte("value-2"), 1, Committed)
	prefix := first.Encode()
	encodedMarker := marker.Encode()
	for offset := 0; offset < len(encodedMarker); offset++ {
		t.Run(faultmodel.TornWriteScenario{Region: "commit-marker", Offset: offset, Size: len(encodedMarker)}.ID(), func(t *testing.T) {
			opt := newTestOptions(t)
			log := append(append([]byte(nil), prefix...), encodedMarker[:offset]...)
			if err := os.WriteFile(getDataFilePath(opt.Dir, 0), log, 0644); err != nil {
				t.Fatal(err)
			}
			db, err := Open(opt)
			if err == nil {
				defer db.Close()
				assertNotFound(t, db, "bucket", []byte("first"))
				assertNotFound(t, db, "bucket", []byte("second"))
			}
		})
	}
}

func TestCrashExplorerCorruptEntryFailsSafely(t *testing.T) {
	opt := newTestOptions(t)
	opt.FaultInjection.Enable = true
	opt.FaultInjection.CorruptAfterWrite = true
	db := openTestDB(t, opt)
	if err := db.Update(func(tx *Tx) error {
		return tx.Put("crash", []byte("key"), []byte("value"), Persistent)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(opt); err == nil {
		t.Fatal("corrupt/recovery-entry-value expected Open error")
	}
}

func BenchmarkRecoveryTornCommitMarkers(b *testing.B) {
	first := newTestEntry("bucket", []byte("first"), []byte("value-1"), 1, UnCommitted).Encode()
	marker := newTestEntry("bucket", []byte("second"), []byte("value-2"), 1, Committed).Encode()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		opt := DefaultOptions
		opt.Dir = b.TempDir()
		log := append(append([]byte(nil), first...), marker[:i%len(marker)]...)
		if err := os.WriteFile(getDataFilePath(opt.Dir, 0), log, 0644); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		db, err := Open(opt)
		if err == nil {
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkRecoveryManyPartialTransactions(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	dataFile, err := NewDataFile(opt.Dir, 0, opt.SegmentSize, opt.RWMode)
	if err != nil {
		b.Fatal(err)
	}
	var off int64
	for i := 0; i < 1000; i++ {
		committed := newTestEntry("recovery", []byte("committed"), []byte("value"), uint64(i*2+1), Committed)
		partial := newTestEntry("recovery", []byte("partial"), []byte("hidden"), uint64(i*2+2), UnCommitted)
		for _, entry := range []*Entry{committed, partial} {
			encoded := entry.Encode()
			if _, err := dataFile.WriteAt(encoded, off); err != nil {
				b.Fatal(err)
			}
			off += entry.Size()
		}
	}
	if err := dataFile.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil && !errors.Is(err, ErrDBClosed) {
			b.Fatal(err)
		}
	}
}
