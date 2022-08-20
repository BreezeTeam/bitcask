package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	opt.SegmentSize = 128
	opt.Compaction.Mode = CompactionByGarbageRatio
	opt.Compaction.MinGarbageRatio = 0.1
	db := openTestDB(t, opt)
	defer db.Close()

	value := bytes.Repeat([]byte("x"), 32)
	putValue(t, db, "merge", []byte("key"), value, Persistent)
	putValue(t, db, "merge", []byte("key"), value, Persistent)
	putValue(t, db, "merge", []byte("live"), value, Persistent)
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	if len(fileIDs) < 2 {
		t.Fatalf("got files %v want at least two", fileIDs)
	}
	got := db.pickMergeFileIDs(fileIDs)
	if len(got) != 1 {
		t.Fatalf("got %v want single candidate", got)
	}
}

func TestMergeSegmentsMeasureLogicalAndLiveBytes(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()

	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "stats", []byte("key"), value, Persistent)
	putValue(t, db, "stats", []byte("key"), value, Persistent)
	putValue(t, db, "stats", []byte("live"), value, Persistent)
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("got %+v want at least two segments", segments)
	}

	var totalSize, totalLive int64
	var garbageFound bool
	for _, segment := range segments {
		if segment.size < 0 || segment.liveBytes < 0 || segment.liveBytes > segment.size {
			t.Fatalf("invalid segment stats: %+v", segment)
		}
		totalSize += segment.size
		totalLive += segment.liveBytes
		if segment.size > segment.liveBytes {
			garbageFound = true
		}
	}
	if totalSize == 0 || totalLive == 0 || !garbageFound {
		t.Fatalf("unexpected aggregate stats size=%d live=%d garbage=%t: %+v", totalSize, totalLive, garbageFound, segments)
	}
}

func TestMergeSegmentsExcludeDeletedAndExpiredRecords(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()

	putValue(t, db, "stats", []byte("deleted"), []byte("value"), Persistent)
	deleteKey(t, db, "stats", []byte("deleted"))
	putValueWithTimestamp(t, db, "stats", []byte("expired"), []byte("value"), 1, 1)
	putValue(t, db, "stats", []byte("live"), []byte("value"), Persistent)
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		t.Fatal(err)
	}

	liveRecord, err := db.getRecordFromKey([]byte("stats"), []byte("live"))
	if err != nil {
		t.Fatal(err)
	}
	wantLive := int64(uint32(DataEntryHeaderSize) + liveRecord.H.Meta.KeySize + liveRecord.H.Meta.ValueSize + liveRecord.H.Meta.BucketSize)
	var gotLive int64
	for _, segment := range segments {
		gotLive += segment.liveBytes
	}
	if gotLive != wantLive {
		t.Fatalf("live bytes got %d want %d: %+v", gotLive, wantLive, segments)
	}
}

func TestCompactionObservationTracksReadHotnessAndAmplification(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "observe", []byte("old"), value, Persistent)
	putValue(t, db, "observe", []byte("hot"), value, Persistent)
	putValue(t, db, "observe", []byte("rotate"), value, Persistent)

	record, err := db.getRecordFromKey([]byte("observe"), []byte("hot"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		assertValue(t, db, "observe", []byte("hot"), value)
	}
	observation := db.CompactionObservation()
	if observation.LogicalBytes == 0 || observation.LiveBytes == 0 || observation.PhysicalBytes == 0 {
		t.Fatalf("missing measured bytes: %+v", observation)
	}
	if observation.SegmentReads[record.H.FileID] != 5 {
		t.Fatalf("reads got %+v want file %d count 5", observation.SegmentReads, record.H.FileID)
	}
	if observation.WriteAmplification < 1 || observation.SpaceAmplification < 1 {
		t.Fatalf("invalid amplification: %+v", observation)
	}
	observation.SegmentReads[record.H.FileID] = 999
	if got := db.CompactionObservation().SegmentReads[record.H.FileID]; got != 5 {
		t.Fatalf("snapshot mutation leaked, got %d", got)
	}
}

func TestMergeSegmentsIncludeMeasuredReadCount(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "hot", []byte("key"), value, Persistent)
	putValue(t, db, "hot", []byte("rotate"), value, Persistent)
	record, err := db.getRecordFromKey([]byte("hot"), []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		assertValue(t, db, "hot", []byte("key"), value)
	}
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range segments {
		if int64(segment.id) == record.H.FileID && segment.readCount != 3 {
			t.Fatalf("read count got %d want 3: %+v", segment.readCount, segments)
		}
	}
}

func TestCompactionRecommendationDisabledByDefault(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()
	if got := db.RecommendCompaction(time.Millisecond); got != (CompactionRecommendation{}) {
		t.Fatalf("unexpected recommendation: %+v", got)
	}
}

func TestCompactionRecommendationThrottlesAboveTarget(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = 10 * time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 100
	opt.Compaction.ControllerStableWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "recommend", []byte("key"), value, Persistent)
	putValue(t, db, "recommend", []byte("key"), value, Persistent)
	got := db.RecommendCompaction(20 * time.Millisecond)
	if !got.Available || got.BudgetBytes != 0 || got.Reason != "foreground p99 exceeds target" {
		t.Fatalf("unexpected recommendation: %+v", got)
	}
}

func TestCompactionRecommendationAllowsEmergencySpaceWork(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = 10 * time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 1.1
	opt.Compaction.ControllerStableWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "recommend", []byte("key"), []byte("value"), Persistent)
	got := db.RecommendCompaction(20 * time.Millisecond)
	if !got.Available || got.BudgetBytes == 0 || got.Picker != CompactionByGarbageRatio || got.Reason != "emergency space amplification" {
		t.Fatalf("unexpected recommendation: %+v", got)
	}
}

func BenchmarkCompactionRecommendation(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 4 * 1024
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = 10 * time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.ControllerStableWindows = 1
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < 512; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.RecommendCompaction(5 * time.Millisecond)
	}
}

func BenchmarkCompactionObservation(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 4 * 1024
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < 512; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.CompactionObservation()
	}
}

func BenchmarkMergeSegmentsMeasuredStats(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 4 * 1024
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < 512; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
			b.Fatal(err)
		}
	}
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.mergeSegments(fileIDs); err != nil {
			b.Fatal(err)
		}
	}
}

func TestBudgetMergeFileIDsRespectsLogicalBytes(t *testing.T) {
	segments := []mergeSegment{{id: 1, size: 40}, {id: 2, size: 70}, {id: 3, size: 30}}
	got := budgetMergeFileIDs(segments, 75)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("got %v want [1 3]", got)
	}
	if got := budgetMergeFileIDs(segments, 20); len(got) != 0 {
		t.Fatalf("got %v want empty", got)
	}
}

func TestMergeWithBudgetPreservesLiveData(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "budget", []byte("key"), value, Persistent)
	putValue(t, db, "budget", []byte("key"), bytes.Repeat([]byte("n"), 32), Persistent)
	putValue(t, db, "budget", []byte("other"), value, Persistent)
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		t.Fatal(err)
	}
	var budget int64
	for _, segment := range segments {
		if segment.size > 0 {
			budget = segment.size
			break
		}
	}
	if err := db.MergeWithBudget(budget); err != nil {
		t.Fatal(err)
	}
	assertValue(t, db, "budget", []byte("key"), bytes.Repeat([]byte("n"), 32))
	assertValue(t, db, "budget", []byte("other"), value)
}

func TestMergeWithBudgetRejectsNonPositiveBudget(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()
	if err := db.MergeWithBudget(0); err == nil {
		t.Fatal("expected invalid merge budget")
	}
}

func TestMergeRecommendedHonorsLatencyThrottle(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 100
	opt.Compaction.ControllerStableWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "budget", []byte("key"), value, Persistent)
	putValue(t, db, "budget", []byte("key"), value, Persistent)
	before := db.Metrics().MergeRuns
	recommendation, err := db.MergeRecommended(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if recommendation.BudgetBytes != 0 || db.Metrics().MergeRuns != before {
		t.Fatalf("throttled recommendation executed merge: %+v", recommendation)
	}
}

func TestCompactionAuditRecordsRecommendationAndExecution(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 100
	opt.Compaction.ControllerStableWindows = 1
	opt.Compaction.AuditCapacity = 2
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "audit", []byte("key"), value, Persistent)
	putValue(t, db, "audit", []byte("key"), value, Persistent)

	if got := db.CompactionAudit(); len(got) != 0 {
		t.Fatalf("unexpected audit before recommendation: %+v", got)
	}
	recommendation, err := db.MergeRecommended(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if recommendation.BudgetBytes != 0 {
		t.Fatalf("expected throttled recommendation: %+v", recommendation)
	}
	audit := db.CompactionAudit()
	if len(audit) != 2 || audit[0].Action != "recommend" || audit[1].Action != "noop" {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	if audit[0].Sequence != 1 || audit[1].Sequence != 2 || !audit[0].Available || audit[0].LogicalBytes == 0 || audit[0].ObsoleteBytes == 0 {
		t.Fatalf("missing recommendation snapshot: %+v", audit)
	}
	audit[0].Action = "mutated"
	if got := db.CompactionAudit()[0].Action; got != "recommend" {
		t.Fatalf("audit snapshot mutation leaked: %s", got)
	}

	_ = db.RecommendCompaction(20 * time.Millisecond)
	audit = db.CompactionAudit()
	if len(audit) != 2 || audit[0].Sequence != 2 || audit[1].Sequence != 3 {
		t.Fatalf("audit ring did not evict oldest event: %+v", audit)
	}
}

func TestCompactionAuditRecordsSuccessfulMerge(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = 10 * time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.EmergencySpaceAmp = 1.1
	opt.Compaction.ControllerStableWindows = 1
	opt.Compaction.AuditCapacity = 4
	db := openTestDB(t, opt)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "audit-merge", []byte("key"), value, Persistent)
	putValue(t, db, "audit-merge", []byte("key"), bytes.Repeat([]byte("n"), 32), Persistent)
	putValue(t, db, "audit-merge", []byte("other"), value, Persistent)

	before := db.Metrics().MergeRuns
	recommendation, err := db.MergeRecommended(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !recommendation.Available || recommendation.BudgetBytes <= 0 || db.Metrics().MergeRuns <= before {
		t.Fatalf("expected executed merge recommendation=%+v metrics=%+v", recommendation, db.Metrics())
	}
	audit := db.CompactionAudit()
	last := audit[len(audit)-1]
	if last.Action != "merge" || !last.Executed || last.Error != "" || last.MergeBytesWritten == 0 {
		t.Fatalf("missing successful merge audit event: %+v", audit)
	}
	assertValue(t, db, "audit-merge", []byte("key"), bytes.Repeat([]byte("n"), 32))
	assertValue(t, db, "audit-merge", []byte("other"), value)
}

func BenchmarkCompactionAuditSnapshot(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 4 * 1024
	opt.Compaction.EnableSLORecommendation = true
	opt.Compaction.TargetP99 = time.Millisecond
	opt.Compaction.MinGarbageBytes = 1
	opt.Compaction.ControllerStableWindows = 1
	opt.Compaction.AuditCapacity = 64
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 128)
	for i := 0; i < 128; i++ {
		key := []byte{byte(i)}
		if err := db.Update(func(tx *Tx) error { return tx.Put("bench", key, value, Persistent) }); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 64; i++ {
		_ = db.RecommendCompaction(2 * time.Millisecond)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.CompactionAudit()
	}
}

func BenchmarkBudgetMergeFileIDs(b *testing.B) {
	segments := make([]mergeSegment, 1024)
	for i := range segments {
		segments[i] = mergeSegment{id: i, size: int64(1024 + i%4096)}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = budgetMergeFileIDs(segments, 1024*1024)
	}
}

func TestMergeManifestCorruptionFailsOpen(t *testing.T) {
	opt := newTestOptions(t)
	if err := os.WriteFile(filepath.Join(opt.Dir, mergeManifestName), []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(opt); err == nil {
		t.Fatal("expected corrupt merge manifest to fail open")
	}
}

func TestMergeManifestInstalledRemovesSource(t *testing.T) {
	opt := newTestOptions(t)
	source := newTestEntry("merge", []byte("old"), []byte("value"), 1, Committed)
	target := newTestEntry("merge", []byte("new"), []byte("value"), 2, Committed)
	writeEntries(t, opt, source)
	targetFile, err := NewDataFile(opt.Dir, 1, opt.SegmentSize, opt.RWMode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := targetFile.WriteAt(target.Encode(), 0); err != nil {
		t.Fatal(err)
	}
	if err := targetFile.Close(); err != nil {
		t.Fatal(err)
	}
	dummy := &DB{opt: opt}
	if err := dummy.writeMergeManifest(mergeManifest{Phase: mergeManifestInstalled, SourceFileID: 0, FirstTargetFileID: 1, LastTargetFileID: 1}); err != nil {
		t.Fatal(err)
	}
	db := openTestDB(t, opt)
	defer db.Close()
	assertNotFound(t, db, "merge", []byte("old"))
	assertValue(t, db, "merge", []byte("new"), []byte("value"))
	if _, err := os.Stat(getDataFilePath(opt.Dir, 0)); !os.IsNotExist(err) {
		t.Fatalf("source still exists: %v", err)
	}
}

func TestMergeManifestInstalledEmptyTargetRemovesSource(t *testing.T) {
	opt := newTestOptions(t)
	writeEntry(t, opt, newTestEntry("merge", []byte("obsolete"), []byte("value"), 1, Committed))
	dummy := &DB{opt: opt}
	if err := dummy.writeMergeManifest(mergeManifest{
		Phase:             mergeManifestInstalled,
		SourceFileID:      0,
		FirstTargetFileID: 1,
		LastTargetFileID:  0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := recoverMergeManifest(opt); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(getDataFilePath(opt.Dir, 0)); !os.IsNotExist(err) {
		t.Fatalf("empty-target installed source still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(opt.Dir, mergeManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest not cleared: %v", err)
	}
	db := openTestDB(t, opt)
	defer db.Close()
}

func TestMergeManifestMissingTargetRetainsSource(t *testing.T) {
	opt := newTestOptions(t)
	writeEntry(t, opt, newTestEntry("merge", []byte("old"), []byte("value"), 1, Committed))
	dummy := &DB{opt: opt}
	if err := dummy.writeMergeManifest(mergeManifest{Phase: mergeManifestInstalled, SourceFileID: 0, FirstTargetFileID: 1, LastTargetFileID: 1}); err != nil {
		t.Fatal(err)
	}
	db := openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "merge", []byte("old"), []byte("value"))
	if _, err := os.Stat(getDataFilePath(opt.Dir, 0)); err != nil {
		t.Fatalf("source removed without target: %v", err)
	}
}

func TestMergeSemanticInstalledRecoveryPreservesLiveData(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("v"), 32)
	putValue(t, db, "merge", []byte("key"), value, Persistent)
	putValue(t, db, "merge", []byte("other"), value, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.SemanticPoint = FaultPointMergeInstalled
	db.opt.FaultInjection.SemanticFailAfter = 0
	db.opt.faultState = &faultInjectionState{}
	if err := db.Merge(); !errors.Is(err, ErrFaultInjectedSemantic) {
		t.Fatalf("got %v want semantic fault", err)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "merge", []byte("key"), value)
	assertValue(t, db, "merge", []byte("other"), value)
}

func TestMergeMetadataFaultMatrixPreservesData(t *testing.T) {
	for occurrence := 1; occurrence <= 2; occurrence++ {
		for _, stage := range []metadataStage{
			metadataStageTempWrite,
			metadataStageFileSync,
			metadataStageRename,
			metadataStageDirSync,
		} {
			t.Run(fmt.Sprintf("occurrence=%d/stage=%s", occurrence, stage), func(t *testing.T) {
				opt := newTestOptions(t)
				opt.SegmentSize = 128
				db := openTestDB(t, opt)
				value := bytes.Repeat([]byte("m"), 32)
				putValue(t, db, "merge-matrix", []byte("key"), value, Persistent)
				putValue(t, db, "merge-matrix", []byte("other"), value, Persistent)
				db.opt.FaultInjection.Enable = true
				db.opt.FaultInjection.MetadataStage = string(stage)
				db.opt.FaultInjection.MetadataFailAfter = int64(occurrence - 1)
				db.opt.faultState = &faultInjectionState{}
				if err := db.Merge(); !errors.Is(err, ErrFaultInjectedMetadata) {
					t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
				}
				db.opt.FaultInjection.Enable = false
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = openTestDB(t, opt)
				assertValue(t, db, "merge-matrix", []byte("key"), value)
				assertValue(t, db, "merge-matrix", []byte("other"), value)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(opt.Dir, mergeManifestName)); !os.IsNotExist(err) {
					t.Fatalf("manifest not cleared: %v", err)
				}
			})
		}
	}
}

func TestMergeManifestDeleteDirectorySyncFaultRecovers(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("d"), 32)
	putValue(t, db, "merge-delete-sync", []byte("key"), value, Persistent)
	putValue(t, db, "merge-delete-sync", []byte("other"), value, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.MetadataStage = string(metadataStageManifestDeleteDirSync)
	db.opt.FaultInjection.MetadataFailAfter = 0
	db.opt.faultState = &faultInjectionState{}
	if err := db.Merge(); !errors.Is(err, ErrFaultInjectedMetadata) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "merge-delete-sync", []byte("key"), value)
	assertValue(t, db, "merge-delete-sync", []byte("other"), value)
	if _, err := os.Stat(filepath.Join(opt.Dir, mergeManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest not cleared: %v", err)
	}
}

func TestMergeMetadataDirectorySyncFaultRecovers(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 128
	db := openTestDB(t, opt)
	value := bytes.Repeat([]byte("m"), 32)
	putValue(t, db, "merge-metadata", []byte("key"), value, Persistent)
	putValue(t, db, "merge-metadata", []byte("other"), value, Persistent)
	db.opt.FaultInjection.Enable = true
	db.opt.FaultInjection.MetadataStage = string(metadataStageDirSync)
	db.opt.FaultInjection.MetadataFailAfter = 1
	db.opt.faultState = &faultInjectionState{}
	if err := db.Merge(); !errors.Is(err, ErrFaultInjectedMetadata) {
		t.Fatalf("got %v want %v", err, ErrFaultInjectedMetadata)
	}
	db.opt.FaultInjection.Enable = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTestDB(t, opt)
	defer db.Close()
	assertValue(t, db, "merge-metadata", []byte("key"), value)
	assertValue(t, db, "merge-metadata", []byte("other"), value)
	if _, err := os.Stat(filepath.Join(opt.Dir, mergeManifestName)); !os.IsNotExist(err) {
		t.Fatalf("manifest not cleared: %v", err)
	}
}

func BenchmarkMergeManifestRecovery(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		opt := DefaultOptions
		opt.Dir = b.TempDir()
		writeEntriesBench := func(id int64, entry *Entry) {
			file, err := NewDataFile(opt.Dir, id, opt.SegmentSize, opt.RWMode)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := file.WriteAt(entry.Encode(), 0); err != nil {
				b.Fatal(err)
			}
			if err := file.Close(); err != nil {
				b.Fatal(err)
			}
		}
		writeEntriesBench(0, newTestEntry("merge", []byte("old"), []byte("value"), 1, Committed))
		writeEntriesBench(1, newTestEntry("merge", []byte("new"), []byte("value"), 2, Committed))
		dummy := &DB{opt: opt}
		if err := dummy.writeMergeManifest(mergeManifest{Phase: mergeManifestInstalled, SourceFileID: 0, FirstTargetFileID: 1, LastTargetFileID: 1}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		db, err := Open(opt)
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
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
