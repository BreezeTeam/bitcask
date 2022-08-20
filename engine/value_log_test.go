package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
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

func TestKVSeparationSegmentRotationAndReopen(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	values := make(map[string][]byte)
	for i := 0; i < 6; i++ {
		key := []byte{byte('a' + i)}
		value := bytes.Repeat([]byte{byte('A' + i)}, 40)
		values[string(key)] = value
		putValue(t, db, "segments", key, value, Persistent)
	}
	if db.valueLog.activeID < 5 {
		t.Fatalf("active value-log id got %d want at least 5", db.valueLog.activeID)
	}
	for key := range values {
		record, err := db.getRecordFromKey([]byte("segments"), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		ptr, err := decodeValuePointer(record.E.Value)
		if err != nil {
			t.Fatal(err)
		}
		if key != "a" && ptr.FileID == 0 {
			t.Fatalf("key %q unexpectedly points to legacy segment", key)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	for key, value := range values {
		assertValue(t, db, "segments", []byte(key), value)
	}
}

func TestKVSeparationLegacyValueLogRemainsReadable(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("legacy"), 16)
	putValue(t, db, "legacy", []byte("key"), value, Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	opt.KVSeparation.ValueLogSegmentSize = 64
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "legacy", []byte("key"), value)
}

func TestKVSeparationMissingSegmentReturnsError(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 32
	db := openTestDB(t, opt)
	putValue(t, db, "missing", []byte("first"), bytes.Repeat([]byte("a"), 32), Persistent)
	putValue(t, db, "missing", []byte("second"), bytes.Repeat([]byte("b"), 32), Persistent)
	path := db.valueLog.path(1)
	if err := db.valueLog.files[1].Close(); err != nil {
		t.Fatal(err)
	}
	delete(db.valueLog.files, 1)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		_, err := tx.Get("missing", []byte("second"))
		if !errors.Is(err, ErrValuePointer) {
			t.Fatalf("got %v want %v", err, ErrValuePointer)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestValueLogStatsTrackLiveAndStaleSegments(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			opt := newTestOptions(t)
			opt.SegmentSize = 1024 * 1024
			opt.EntryIdxMode = mode
			opt.KVSeparation.Enable = true
			opt.KVSeparation.Threshold = 16
			opt.KVSeparation.ValueLogSegmentSize = 64
			db := openTestDB(t, opt)
			defer db.Close()

			putValue(t, db, "stats", []byte("key"), bytes.Repeat([]byte("a"), 40), Persistent)
			putValue(t, db, "stats", []byte("key"), bytes.Repeat([]byte("b"), 40), Persistent)
			putValue(t, db, "stats", []byte("live"), bytes.Repeat([]byte("c"), 40), Persistent)
			stats := db.ValueLogStats()
			if len(stats) != 3 {
				t.Fatalf("got %+v want three segments", stats)
			}
			if stats[0].LiveBytes != 0 || stats[0].StaleBytes != 40 {
				t.Fatalf("legacy stale stats: %+v", stats[0])
			}
			if stats[1].LiveBytes != 40 || stats[1].StaleBytes != 0 {
				t.Fatalf("segment 1 stats: %+v", stats[1])
			}
			if !stats[2].Active || stats[2].LiveBytes != 40 {
				t.Fatalf("active stats: %+v", stats[2])
			}
			candidate, ok := db.PickValueLogGCCandidate(0.5)
			if !ok || candidate.FileID != 0 {
				t.Fatalf("candidate got %+v ok=%t", candidate, ok)
			}
		})
	}
}

func TestValueLogStatsPersistAfterReopen(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	putValue(t, db, "stats", []byte("key"), bytes.Repeat([]byte("a"), 40), Persistent)
	putValue(t, db, "stats", []byte("key"), bytes.Repeat([]byte("b"), 40), Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	stats := db.ValueLogStats()
	if len(stats) != 2 || stats[0].StaleBytes != 40 || stats[1].LiveBytes != 40 {
		t.Fatalf("unexpected reopened stats: %+v", stats)
	}
}

func TestValueLogGCCandidateExcludesActiveSegment(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "stats", []byte("key"), bytes.Repeat([]byte("a"), 40), Persistent)
	if candidate, ok := db.PickValueLogGCCandidate(0); ok || candidate.FileID != 0 {
		t.Fatalf("active segment selected: %+v ok=%t", candidate, ok)
	}
}

func TestLifecyclePlacementRequiresKVSeparation(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.LifecycleEnable = true
	if _, err := Open(opt); err == nil {
		t.Fatal("expected lifecycle placement without value log to fail")
	}
}

func TestLifecyclePlacementFallsBackToThreshold(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.LifecycleEnable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "lifecycle", []byte("key"), bytes.Repeat([]byte("x"), 40), Persistent)
	record, err := db.getRecordFromKey([]byte("lifecycle"), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if record.H.Meta.Ds != DataStructureValuePointer {
		t.Fatalf("got ds %d want value pointer", record.H.Meta.Ds)
	}
	metrics := db.KVPlacementMetrics()
	if metrics.ThresholdFallback != 1 || metrics.ValueLogDecisions != 1 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestLifecyclePlacementKeepsFrequentlyUpdatedLargeValueInline(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.LifecycleEnable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.LifecycleMinObservations = 2
	opt.KVSeparation.LifecycleFrequentUpdates = 2
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("x"), 40)
	putValue(t, db, "lifecycle", []byte("key"), value, Persistent)
	putValue(t, db, "lifecycle", []byte("key"), value, Persistent)
	record, err := db.getRecordFromKey([]byte("lifecycle"), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if record.H.Meta.Ds != DataStructureBPTree {
		t.Fatalf("got ds %d want inline", record.H.Meta.Ds)
	}
	assertValue(t, db, "lifecycle", []byte("key"), value)
}

func TestLifecyclePlacementKeepsHotLargeValueInline(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.LifecycleEnable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.LifecycleMinObservations = 3
	opt.KVSeparation.LifecycleHotReads = 2
	opt.KVSeparation.LifecycleFrequentUpdates = 100
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("x"), 40)
	putValue(t, db, "lifecycle", []byte("key"), value, Persistent)
	assertValue(t, db, "lifecycle", []byte("key"), value)
	assertValue(t, db, "lifecycle", []byte("key"), value)
	putValue(t, db, "lifecycle", []byte("key"), value, Persistent)
	record, err := db.getRecordFromKey([]byte("lifecycle"), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if record.H.Meta.Ds != DataStructureBPTree {
		t.Fatalf("got ds %d want inline", record.H.Meta.Ds)
	}
}

func TestLifecyclePlacementHistoryResetsSafelyOnReopen(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.LifecycleEnable = true
	opt.KVSeparation.Threshold = 16
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("x"), 40)
	putValue(t, db, "lifecycle", []byte("key"), value, Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "lifecycle", []byte("key"), value)
	if metrics := db.KVPlacementMetrics(); metrics != (KVPlacementMetrics{}) {
		t.Fatalf("history did not reset: %+v", metrics)
	}
}

func BenchmarkLifecyclePlacementPut(b *testing.B) {
	for _, lifecycle := range []bool{false, true} {
		lifecycle := lifecycle
		b.Run(fmt.Sprintf("lifecycle=%t", lifecycle), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 128 * 1024 * 1024
			opt.KVSeparation.Enable = true
			opt.KVSeparation.LifecycleEnable = lifecycle
			opt.KVSeparation.Threshold = 16
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			value := bytes.Repeat([]byte("v"), 1024)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%09d", i))
				if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestValueLogGCPreservesLiveValuesAndReclaimsSource(t *testing.T) {
	for _, mode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		mode := mode
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			opt := newTestOptions(t)
			opt.SegmentSize = 1024 * 1024
			opt.EntryIdxMode = mode
			opt.SyncPolicy.Mode = SyncPolicyEveryCommit
			opt.KVSeparation.Enable = true
			opt.KVSeparation.Threshold = 16
			opt.KVSeparation.ValueLogSegmentSize = 96
			db := openTestDB(t, opt)

			old := bytes.Repeat([]byte("o"), 40)
			live := bytes.Repeat([]byte("l"), 40)
			other := bytes.Repeat([]byte("x"), 40)
			putValue(t, db, "gc", []byte("key"), old, Persistent)
			putValue(t, db, "gc", []byte("live-source"), other, Persistent)
			putValue(t, db, "gc", []byte("key"), live, Persistent)
			putValue(t, db, "gc", []byte("rotate"), other, Persistent)

			candidate, ok := db.PickValueLogGCCandidate(0.4)
			if !ok || candidate.FileID != 0 {
				t.Fatalf("unexpected candidate: %+v ok=%t stats=%+v", candidate, ok, db.ValueLogStats())
			}
			result, err := db.ValueLogGC(0.4)
			if err != nil {
				t.Fatal(err)
			}
			if result.SourceFileID != 0 || result.ValuesCopied != 1 || result.BytesReclaimed != 80 {
				t.Fatalf("unexpected GC result: %+v", result)
			}
			if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); !os.IsNotExist(err) {
				t.Fatalf("source segment still exists: %v", err)
			}
			assertValue(t, db, "gc", []byte("key"), live)
			assertValue(t, db, "gc", []byte("live-source"), other)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db = openTestDB(t, opt)
			defer db.Close()
			assertValue(t, db, "gc", []byte("key"), live)
			assertValue(t, db, "gc", []byte("live-source"), other)
			if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrValueLogGCNoCandidate) {
				t.Fatalf("repeated GC got %v want %v", err, ErrValueLogGCNoCandidate)
			}
		})
	}
}

func TestValueLogGCManifestPreparedRetainsReferencedSource(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("x"), 40)
	putValue(t, db, "manifest", []byte("key"), value, Persistent)
	if err := db.writeValueLogGCManifest(valueLogGCManifest{Phase: valueLogGCPrepared, SourceFileID: 0}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "manifest", []byte("key"), value)
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); err != nil {
		t.Fatalf("referenced source removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogGCManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest not cleared: %v", err)
	}
}

func TestValueLogGCManifestInstalledRemovesUnreferencedSource(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	putValue(t, db, "manifest", []byte("key"), bytes.Repeat([]byte("a"), 40), Persistent)
	putValue(t, db, "manifest", []byte("key"), bytes.Repeat([]byte("b"), 40), Persistent)
	if err := db.writeValueLogGCManifest(valueLogGCManifest{Phase: valueLogGCPointersInstalled, SourceFileID: 0, FirstReplacementFileID: db.valueLog.activeID, LastReplacementFileID: db.valueLog.activeID}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "manifest", []byte("key"), bytes.Repeat([]byte("b"), 40))
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); !os.IsNotExist(err) {
		t.Fatalf("unreferenced source not removed: %v", err)
	}
}

func TestValueLogGCManifestV2RoundTrip(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	db := openTestDB(t, opt)
	defer db.Close()

	for _, phase := range []valueLogGCPhase{valueLogGCPrepared, valueLogGCPointersInstalled, valueLogGCSourceRemoved, valueLogGCFinalized} {
		want := valueLogGCManifest{Phase: phase, SourceFileID: 3, FirstReplacementFileID: 7, LastReplacementFileID: 9}
		if err := db.writeValueLogGCManifest(want); err != nil {
			t.Fatal(err)
		}
		got, err := readValueLogGCManifest(opt.Dir)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("phase %d got %+v want %+v", phase, got, want)
		}
	}
}

func TestValueLogGCManifestV1Compatibility(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("x"), 40)
	putValue(t, db, "manifest-v1", []byte("key"), value, Persistent)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, valueLogGCManifestV1Size)
	binary.LittleEndian.PutUint32(buf[0:4], valueLogGCManifestMagic)
	binary.LittleEndian.PutUint16(buf[4:6], valueLogGCManifestV1Version)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(valueLogGCPrepared))
	binary.LittleEndian.PutUint64(buf[8:16], 0)
	binary.LittleEndian.PutUint32(buf[16:20], crc32.ChecksumIEEE(buf[:16]))
	binary.LittleEndian.PutUint16(buf[20:22], valueLogGCManifestV1Size)
	if err := os.WriteFile(filepath.Join(opt.Dir, valueLogGCManifestName), buf, 0644); err != nil {
		t.Fatal(err)
	}
	manifest, err := readValueLogGCManifest(opt.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if !manifest.legacy || manifest.SourceFileID != 0 || manifest.Phase != valueLogGCPrepared {
		t.Fatalf("unexpected v1 manifest: %+v", manifest)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "manifest-v1", []byte("key"), value)
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); err != nil {
		t.Fatalf("legacy recovery removed referenced source: %v", err)
	}
}

func TestValueLogGCManifestSourceRemovedAndFinalizedRecovery(t *testing.T) {
	for _, phase := range []valueLogGCPhase{valueLogGCSourceRemoved, valueLogGCFinalized} {
		t.Run(fmt.Sprintf("phase=%d", phase), func(t *testing.T) {
			opt := newTestOptions(t)
			opt.KVSeparation.Enable = true
			db := openTestDB(t, opt)
			if err := db.writeValueLogGCManifest(valueLogGCManifest{Phase: phase, SourceFileID: 99}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTestDB(t, opt)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(opt.Dir, valueLogGCManifestName)); !os.IsNotExist(err) {
				t.Fatalf("manifest not cleared: %v", err)
			}
		})
	}
}

func TestValueLogGCManifestMissingReplacementFailsOpen(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	putValue(t, db, "manifest", []byte("key"), bytes.Repeat([]byte("a"), 40), Persistent)
	putValue(t, db, "manifest", []byte("key"), bytes.Repeat([]byte("b"), 40), Persistent)
	if err := db.writeValueLogGCManifest(valueLogGCManifest{Phase: valueLogGCPointersInstalled, SourceFileID: 0, FirstReplacementFileID: 9, LastReplacementFileID: 9}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opt); err == nil {
		t.Fatal("expected missing replacement segment to fail open")
	}
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); err != nil {
		t.Fatalf("source removed after replacement validation failure: %v", err)
	}
}

func TestValueLogGCManifestCorruptionFailsOpen(t *testing.T) {
	opt := newTestOptions(t)
	opt.KVSeparation.Enable = true
	db := openTestDB(t, opt)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opt.Dir, valueLogGCManifestName), []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opt); err == nil {
		t.Fatal("expected corrupt GC manifest to fail open")
	}
}

func TestValueLogGCMetadataFaultMatrixRecovers(t *testing.T) {
	stages := []metadataStage{
		metadataStageTempWrite,
		metadataStageFileSync,
		metadataStageRename,
		metadataStageDirSync,
	}
	for occurrence := 1; occurrence <= 5; occurrence++ {
		for _, stage := range stages {
			name := fmt.Sprintf("occurrence=%d/stage=%s", occurrence, stage)
			t.Run(name, func(t *testing.T) {
				opt := newTestOptions(t)
				opt.SegmentSize = 1024 * 1024
				opt.SyncPolicy.Mode = SyncPolicyEveryCommit
				opt.KVSeparation.Enable = true
				opt.KVSeparation.Threshold = 16
				opt.KVSeparation.ValueLogSegmentSize = 96
				db := openTestDB(t, opt)
				old := bytes.Repeat([]byte("o"), 40)
				live := bytes.Repeat([]byte("l"), 40)
				putValue(t, db, "gc-matrix", []byte("key"), old, Persistent)
				putValue(t, db, "gc-matrix", []byte("source-live"), old, Persistent)
				putValue(t, db, "gc-matrix", []byte("key"), live, Persistent)
				putValue(t, db, "gc-matrix", []byte("rotate"), live, Persistent)
				db.opt.FaultInjection.Enable = true
				db.opt.FaultInjection.MetadataStage = string(stage)
				db.opt.FaultInjection.MetadataFailAfter = int64(occurrence - 1)
				db.opt.faultState = &faultInjectionState{}
				if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedMetadata) {
					t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
				}
				db.opt.FaultInjection.Enable = false
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}

				db = openTestDB(t, opt)
				assertValue(t, db, "gc-matrix", []byte("key"), live)
				assertValue(t, db, "gc-matrix", []byte("source-live"), old)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				_, sourceErr := os.Stat(filepath.Join(opt.Dir, valueLogFileName))
				if occurrence <= 2 && sourceErr != nil {
					t.Fatalf("source removed before pointers committed: %v", sourceErr)
				}
				if occurrence >= 3 && !os.IsNotExist(sourceErr) {
					t.Fatalf("unreferenced source remains: %v", sourceErr)
				}
				if _, err := os.Stat(filepath.Join(opt.Dir, valueLogGCManifestName)); !os.IsNotExist(err) {
					t.Fatalf("manifest not cleared: %v", err)
				}
			})
		}
	}
}

func TestValueLogGCManifestDeleteDirectorySyncFaultRecovers(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 96
	db := openTestDB(t, opt)
	old := bytes.Repeat([]byte("o"), 40)
	live := bytes.Repeat([]byte("l"), 40)
	putValue(t, db, "gc-delete-sync", []byte("key"), old, Persistent)
	putValue(t, db, "gc-delete-sync", []byte("source-live"), old, Persistent)
	putValue(t, db, "gc-delete-sync", []byte("key"), live, Persistent)
	putValue(t, db, "gc-delete-sync", []byte("rotate"), live, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.MetadataStage = string(metadataStageManifestDeleteDirSync)
	db.opt.FaultInjection.MetadataFailAfter = 0
	db.opt.faultState = &faultInjectionState{}
	if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedMetadata) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "gc-delete-sync", []byte("key"), live)
	assertValue(t, db, "gc-delete-sync", []byte("source-live"), old)
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); !os.IsNotExist(err) {
		t.Fatalf("source remains after completed GC: %v", err)
	}
}

func TestValueLogGCMetadataDirectorySyncFaultRecovers(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 96
	db := openTestDB(t, opt)
	old := bytes.Repeat([]byte("o"), 40)
	live := bytes.Repeat([]byte("l"), 40)
	putValue(t, db, "gc-metadata", []byte("key"), old, Persistent)
	putValue(t, db, "gc-metadata", []byte("source-live"), old, Persistent)
	putValue(t, db, "gc-metadata", []byte("key"), live, Persistent)
	putValue(t, db, "gc-metadata", []byte("rotate"), live, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.MetadataStage = string(metadataStageDirSync)
	db.opt.FaultInjection.MetadataFailAfter = 2
	db.opt.faultState = &faultInjectionState{}
	if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedMetadata) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "gc-metadata", []byte("key"), live)
	assertValue(t, db, "gc-metadata", []byte("source-live"), old)
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); !os.IsNotExist(err) {
		t.Fatalf("source remains after installed recovery: %v", err)
	}
}

func TestValueLogGCSemanticCrashAfterPointersRecovers(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 96
	db := openTestDB(t, opt)
	old := bytes.Repeat([]byte("o"), 40)
	live := bytes.Repeat([]byte("l"), 40)
	putValue(t, db, "gc-crash", []byte("key"), old, Persistent)
	putValue(t, db, "gc-crash", []byte("source-live"), old, Persistent)
	putValue(t, db, "gc-crash", []byte("key"), live, Persistent)
	putValue(t, db, "gc-crash", []byte("rotate"), live, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.SemanticPoint = FaultPointGCPointersInstalled
	db.opt.FaultInjection.SemanticFailAfter = 0
	db.opt.faultState = &faultInjectionState{}
	if _, err := db.ValueLogGC(0.4); !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedSemantic)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "gc-crash", []byte("key"), live)
	assertValue(t, db, "gc-crash", []byte("source-live"), old)
	if _, err := os.Stat(filepath.Join(opt.Dir, valueLogFileName)); !os.IsNotExist(err) {
		t.Fatalf("recovery did not remove source: %v", err)
	}
}

func TestValueLogGCNoCandidateLeavesSegmentsUntouched(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "gc", []byte("key"), bytes.Repeat([]byte("x"), 40), Persistent)
	before := db.ValueLogStats()
	if _, err := db.ValueLogGC(0.5); !errors.Is(err, ErrValueLogGCNoCandidate) {
		t.Fatalf("got %v want %v", err, ErrValueLogGCNoCandidate)
	}
	after := db.ValueLogStats()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("stats changed before=%+v after=%+v", before, after)
	}
}

func BenchmarkValueLogGC(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		opt := DefaultOptions
		opt.Dir = b.TempDir()
		opt.SegmentSize = 16 * 1024 * 1024
		opt.SyncPolicy.Mode = SyncPolicyEveryCommit
		opt.KVSeparation.Enable = true
		opt.KVSeparation.Threshold = 16
		opt.KVSeparation.ValueLogSegmentSize = 64 * 1024
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		value := bytes.Repeat([]byte("v"), 1024)
		for keyID := 0; keyID < 64; keyID++ {
			key := []byte(fmt.Sprintf("key-%03d", keyID))
			if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
				b.Fatal(err)
			}
		}
		for keyID := 0; keyID < 32; keyID++ {
			key := []byte(fmt.Sprintf("key-%03d", keyID))
			if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		result, err := db.ValueLogGC(0.4)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		b.ReportMetric(float64(result.LiveBytes), "live-B")
		b.ReportMetric(float64(result.BytesReclaimed), "reclaimed-B")
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueLogStatsCore(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64 * 1024
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 1024)
	for i := 0; i < 1024; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.ValueLogStats()
	}
}

func TestValueLogSyncClearsOnlyAfterSuccess(t *testing.T) {
	valueLog, err := openValueLog(t.TempDir(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer valueLog.Close()
	if _, err := valueLog.Append(bytes.Repeat([]byte("a"), 32)); err != nil {
		t.Fatal(err)
	}
	if _, err := valueLog.Append(bytes.Repeat([]byte("b"), 32)); err != nil {
		t.Fatal(err)
	}
	if len(valueLog.dirty) != 2 {
		t.Fatalf("dirty segments got %d want 2", len(valueLog.dirty))
	}
	if err := valueLog.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(valueLog.dirty) != 0 {
		t.Fatalf("dirty segments not cleared: %+v", valueLog.dirty)
	}
	if _, err := valueLog.Append(bytes.Repeat([]byte("c"), 32)); err != nil {
		t.Fatal(err)
	}
	if len(valueLog.dirty) != 1 {
		t.Fatalf("new append dirtied %d segments want 1", len(valueLog.dirty))
	}
}

func TestTouchedValueSegmentsRetainedAfterMainSyncFailure(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.SyncPolicy.Mode = SyncPolicyEveryCommit
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 32
	opt.FaultInjection.Enable = true
	opt.FaultInjection.SemanticPoint = FaultPointBeforeMainSync
	opt.FaultInjection.SemanticFailAfter = 0
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("x"), 32)
	err := db.Update(func(tx *Tx) error {
		if err := tx.Put("touched", []byte("first"), value, Persistent); err != nil {
			return err
		}
		return tx.Put("touched", []byte("second"), value, Persistent)
	})
	if !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want semantic fault", err)
	}
	// Value segments completed their ordered sync before the injected main-log fault.
	if len(db.valueLog.dirty) != 0 {
		t.Fatalf("successfully synced value segments remained dirty: %+v", db.valueLog.dirty)
	}
	db.opt.FaultInjection.SemanticPoint = ""
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "touched", []byte("first"), value)
	assertValue(t, db, "touched", []byte("second"), value)
}

func BenchmarkTouchedValueLogSync(b *testing.B) {
	valueLog, err := openValueLog(b.TempDir(), 64*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer valueLog.Close()
	value := make([]byte, 4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := valueLog.Append(value); err != nil {
			b.Fatal(err)
		}
		if err := valueLog.Sync(); err != nil {
			b.Fatal(err)
		}
	}
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
	if err := db.valueLog.files[0].Truncate(1); err != nil {
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
