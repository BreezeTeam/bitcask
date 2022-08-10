package compaction

import "testing"

func TestPickByGarbageRatio(t *testing.T) {
	pick := PickByGarbageRatio([]Segment{
		{ID: 1, Size: 100, LiveBytes: 80},
		{ID: 2, Size: 100, LiveBytes: 20},
		{ID: 3, Size: 100, LiveBytes: 60},
	}, 0.5)

	if len(pick.Segments) != 1 || pick.Segments[0].ID != 2 {
		t.Fatalf("got %#v want segment 2", pick)
	}
	if pick.Score != 0.8 {
		t.Fatalf("got score %v want 0.8", pick.Score)
	}
}

func TestPickByGarbageRatioReturnsEmptyWhenNoSegmentQualifies(t *testing.T) {
	pick := PickByGarbageRatio([]Segment{{ID: 1, Size: 100, LiveBytes: 90}}, 0.5)
	if len(pick.Segments) != 0 || pick.Score != 0 {
		t.Fatalf("got %#v want empty pick", pick)
	}
}

func TestPickHotColdPrefersColdGarbage(t *testing.T) {
	pick := PickHotCold([]Segment{
		{ID: 1, Size: 100, LiveBytes: 20, ReadCount: 1000},
		{ID: 2, Size: 100, LiveBytes: 20, ReadCount: 1},
	}, 0.5, 10)
	if len(pick.Segments) != 1 || pick.Segments[0].ID != 2 {
		t.Fatalf("got %#v want cold segment 2", pick)
	}
}

func TestPickHotColdSkipsHotSegmentWithSameGarbage(t *testing.T) {
	pick := PickHotCold([]Segment{
		{ID: 1, Size: 100, LiveBytes: 10, ReadCount: 1000},
		{ID: 2, Size: 100, LiveBytes: 40, ReadCount: 1},
	}, 0.5, 10)
	if len(pick.Segments) != 1 || pick.Segments[0].ID != 2 {
		t.Fatalf("got %#v want colder segment 2", pick)
	}
}
