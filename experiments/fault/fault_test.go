package fault

import "testing"

func TestReplayInjectsWriteFaultAfterLimit(t *testing.T) {
	events := Replay(Schedule{WriteFailAfter: 1, SyncFailAfter: -1, ReadCorruptAfter: -1}, []Point{PointWrite, PointWrite, PointWrite})
	if events[0].Fault {
		t.Fatalf("first write should not fault")
	}
	if !events[1].Fault || !events[2].Fault {
		t.Fatalf("writes after limit should fault")
	}
}

func TestReplaySeparatesPointCounters(t *testing.T) {
	events := Replay(Schedule{WriteFailAfter: 0, SyncFailAfter: 1, ReadCorruptAfter: -1}, []Point{PointWrite, PointSync, PointSync, PointRead})
	if !events[0].Fault {
		t.Fatalf("first write should fault")
	}
	if events[1].Fault {
		t.Fatalf("first sync should not fault")
	}
	if !events[2].Fault {
		t.Fatalf("second sync should fault")
	}
	if events[3].Fault {
		t.Fatalf("read faults are disabled")
	}
}
