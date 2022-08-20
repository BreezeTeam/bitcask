package compaction

import "testing"

func TestOverwriteHotsetAndAmplification(t *testing.T) {
	operations := OverwriteHotset(10, 20, 100)
	report := AnalyzeWorkload(operations, 500, 2000)
	if report.Operations != 30 || report.Writes != 30 || report.Reads != 0 {
		t.Fatalf("unexpected counts: %+v", report)
	}
	if report.LogicalWriteBytes != 3000 || report.LiveBytes != 1000 || report.ObsoleteBytes != 2000 {
		t.Fatalf("unexpected bytes: %+v", report)
	}
	if report.WriteAmplification != float64(3500)/3000 || report.SpaceAmplification != 2 {
		t.Fatalf("unexpected amplification: %+v", report)
	}
}

func TestColdGarbageIsDeterministic(t *testing.T) {
	first := ColdGarbage(10, 4, 32)
	second := ColdGarbage(10, 4, 32)
	if len(first) != 14 || len(second) != len(first) {
		t.Fatalf("unexpected workload lengths %d %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("workload differs at %d: %+v %+v", i, first[i], second[i])
		}
	}
}

func TestMixedWorkloadCounts(t *testing.T) {
	report := AnalyzeWorkload(Mixed(10, 100, 30, 64), 0, 640)
	if report.Writes != 40 || report.Reads != 70 || report.LiveBytes != 640 {
		t.Fatalf("unexpected mixed report: %+v", report)
	}
}

func BenchmarkAnalyzeCompactionWorkload(b *testing.B) {
	operations := OverwriteHotset(1024, 8192, 128)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = AnalyzeWorkload(operations, 64*1024, 4*1024*1024)
	}
}
