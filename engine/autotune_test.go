package bitcask

import (
	"errors"
	"fmt"
	"testing"
)

func TestAutoTuneApplyDisabled(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer db.Close()
	if _, err := db.ApplyPolicyRecommendation(); !errors.Is(err, ErrAutonomousApplyDisabled) {
		t.Fatalf("got %v want %v", err, ErrAutonomousApplyDisabled)
	}
}

func TestAutoTuneRequiresAvailableRecommendation(t *testing.T) {
	opt := newTestOptions(t)
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyCompaction = true
	db := openTestDB(t, opt)
	defer db.Close()
	got, err := db.ApplyPolicyRecommendation()
	if err != nil {
		t.Fatal(err)
	}
	if got.CompactionChanged || got.Reason != "recommendation unavailable" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestAutoTuneAppliesStableCompactionRecommendation(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyCompaction = true
	opt.Autonomous.MinConfidence = 0.7
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	for i := 0; i < 10; i++ {
		putValue(t, db, "auto", []byte("same"), []byte(fmt.Sprintf("value-%02d", i)), Persistent)
	}
	got, err := db.ApplyPolicyRecommendation()
	if err != nil {
		t.Fatal(err)
	}
	if !got.CompactionChanged || got.CompactionMode != CompactionByGarbageRatio {
		t.Fatalf("unexpected applied result: %+v recommendation=%+v", got, db.PolicyRecommendation())
	}
	reset := db.ResetAutonomousPolicies()
	if !reset.CompactionChanged || db.opt.Compaction.Mode != opt.Compaction.Mode {
		t.Fatalf("reset failed: %+v", reset)
	}
}

func TestAutoTuneDoesNotEnableUnopenedValueLog(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyKVPlacement = true
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	for i := 0; i < 10; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("key-%02d", i)), make([]byte, 32*1024), Persistent)
	}
	got, err := db.ApplyPolicyRecommendation()
	if err != nil {
		t.Fatal(err)
	}
	if got.PlacementChanged || db.valueLog != nil || db.opt.KVSeparation.LifecycleEnable {
		t.Fatalf("autotune opened unavailable infrastructure: %+v", got)
	}
}

func TestAutoTuneConfidenceFloor(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyCompaction = true
	opt.Autonomous.MinConfidence = 0.99
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	for i := 0; i < 7; i++ {
		putValue(t, db, "auto", []byte(fmt.Sprintf("write-%02d", i)), []byte("value"), Persistent)
	}
	for i := 0; i < 3; i++ {
		assertValue(t, db, "auto", []byte(fmt.Sprintf("write-%02d", i)), []byte("value"))
	}
	got, err := db.ApplyPolicyRecommendation()
	if err != nil {
		t.Fatal(err)
	}
	if got.CompactionChanged || got.Reason != "recommendation below confidence threshold" {
		t.Fatalf("confidence floor not enforced: %+v recommendation=%+v", got, db.PolicyRecommendation())
	}
}

func TestPolicyAuditIsBoundedAndImmutable(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyCompaction = true
	opt.Autonomous.AuditCapacity = 3
	opt.Autonomous.WindowOperations = 2
	opt.Autonomous.MinOperations = 2
	opt.Autonomous.ConsecutiveWindows = 1
	db := openTestDB(t, opt)
	defer db.Close()
	for i := 0; i < 8; i++ {
		putValue(t, db, "audit", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
	}
	if _, err := db.ApplyPolicyRecommendation(); err != nil {
		t.Fatal(err)
	}
	audit := db.PolicyAudit()
	if len(audit) != 3 {
		t.Fatalf("audit len got %d want 3: %+v", len(audit), audit)
	}
	if !(audit[0].Sequence < audit[1].Sequence && audit[1].Sequence < audit[2].Sequence) {
		t.Fatalf("audit sequence not monotonic: %+v", audit)
	}
	audit[0].Reason = "mutated"
	if db.PolicyAudit()[0].Reason == "mutated" {
		t.Fatal("audit snapshot mutation leaked")
	}
}

func TestAutonomousObservationIncludesValueLogPressure(t *testing.T) {
	opt := newTestOptions(t)
	opt.SegmentSize = 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.WindowOperations = 100
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16
	opt.KVSeparation.ValueLogSegmentSize = 64
	db := openTestDB(t, opt)
	defer db.Close()
	putValue(t, db, "pressure", []byte("key"), make([]byte, 40), Persistent)
	putValue(t, db, "pressure", []byte("key"), make([]byte, 40), Persistent)
	got := db.AutonomousObservation()
	if got.ValueLogTotalBytes != 80 || got.ValueLogLiveBytes != 40 || got.ValueLogStaleBytes != 40 {
		t.Fatalf("unexpected value-log pressure: %+v", got)
	}
}

func BenchmarkPolicyAuditSnapshot(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.AuditCapacity = 64
	opt.Autonomous.WindowOperations = 1
	opt.Autonomous.MinOperations = 1
	opt.Autonomous.ConsecutiveWindows = 1
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 64; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("bench", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.PolicyAudit()
	}
}

func BenchmarkApplyPolicyRecommendation(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.Autonomous.EnableRecommendations = true
	opt.Autonomous.ApplyCompaction = true
	opt.Autonomous.WindowOperations = 10
	opt.Autonomous.MinOperations = 10
	opt.Autonomous.ConsecutiveWindows = 1
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 10; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("bench", []byte(fmt.Sprintf("key-%02d", i)), []byte("value"), Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.ApplyPolicyRecommendation(); err != nil {
			b.Fatal(err)
		}
	}
}
