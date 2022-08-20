package bitcask

import (
	"fmt"
	"testing"
)

type phaseWorkloadResult struct {
	Recommendation PolicyRecommendation
	AuditEvents    int
	Latency        CommitLatencySnapshot
	CompactionMode CompactionPolicyMode
}

func runAutonomousPhases(tb testing.TB, autonomous bool, operations int) phaseWorkloadResult {
	tb.Helper()
	opt := DefaultOptions
	opt.Dir = tb.TempDir()
	opt.SegmentSize = 128 * 1024 * 1024
	opt.KVSeparation.Enable = true
	opt.KVSeparation.Threshold = 16 * 1024
	opt.Autonomous.EnableRecommendations = autonomous
	opt.Autonomous.ApplyCompaction = autonomous
	opt.Autonomous.ApplyKVPlacement = autonomous
	opt.Autonomous.WindowOperations = uint64(operations)
	opt.Autonomous.MinOperations = uint64(operations)
	opt.Autonomous.ConsecutiveWindows = 1
	opt.Autonomous.CooldownWindows = 1
	db, err := Open(opt)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()

	value := []byte("value")
	for i := 0; i < operations; i++ {
		key := []byte(fmt.Sprintf("write-%04d", i))
		if err := db.Update(func(tx *Tx) error { return tx.Put("phase", key, value, Persistent) }); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i < operations; i++ {
		key := []byte(fmt.Sprintf("write-%04d", i%operations))
		if err := db.View(func(tx *Tx) error { _, err := tx.Get("phase", key); return err }); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i < operations; i++ {
		if err := db.Update(func(tx *Tx) error { return tx.Put("phase", []byte("hot"), value, Persistent) }); err != nil {
			tb.Fatal(err)
		}
	}
	large := make([]byte, 32*1024)
	for i := 0; i < operations; i++ {
		key := []byte(fmt.Sprintf("large-%04d", i))
		if err := db.Update(func(tx *Tx) error { return tx.Put("phase", key, large, Persistent) }); err != nil {
			tb.Fatal(err)
		}
	}
	if autonomous {
		if _, err := db.ApplyPolicyRecommendation(); err != nil {
			tb.Fatal(err)
		}
	}
	return phaseWorkloadResult{
		Recommendation: db.PolicyRecommendation(),
		AuditEvents:    len(db.PolicyAudit()),
		Latency:        db.CommitLatency(),
		CompactionMode: db.opt.Compaction.Mode,
	}
}

func BenchmarkAutonomousPhaseTransitions(b *testing.B) {
	for _, autonomous := range []bool{false, true} {
		autonomous := autonomous
		b.Run(fmt.Sprintf("autonomous=%t", autonomous), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result := runAutonomousPhases(b, autonomous, 32)
				b.ReportMetric(float64(result.Latency.P50.Nanoseconds()), "commit-p50-ns")
				b.ReportMetric(float64(result.Latency.P99.Nanoseconds()), "commit-p99-ns")
				b.ReportMetric(float64(result.AuditEvents), "audit-events")
			}
		})
	}
}
