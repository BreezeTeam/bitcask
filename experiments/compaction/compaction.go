package compaction

type Segment struct {
	ID        int
	Size      int64
	LiveBytes int64
	ReadCount int64
}

type Pick struct {
	Segments []Segment
	Score    float64
}

func PickByGarbageRatio(segments []Segment, minGarbageRatio float64) Pick {
	best := Pick{}
	for _, segment := range segments {
		garbageRatio := GarbageRatio(segment)
		if garbageRatio < minGarbageRatio || garbageRatio <= best.Score {
			continue
		}
		best = Pick{Segments: []Segment{segment}, Score: garbageRatio}
	}
	return best
}

func PickHotCold(segments []Segment, minGarbageRatio float64, coldReadThreshold int64) Pick {
	best := Pick{}
	for _, segment := range segments {
		garbageRatio := GarbageRatio(segment)
		if garbageRatio < minGarbageRatio {
			continue
		}
		coldnessWeight := 1.0
		if segment.ReadCount > coldReadThreshold {
			coldnessWeight = 0.5
		}
		score := garbageRatio * coldnessWeight
		if score <= best.Score {
			continue
		}
		best = Pick{Segments: []Segment{segment}, Score: score}
	}
	return best
}

func GarbageRatio(segment Segment) float64 {
	if segment.Size <= 0 {
		return 0
	}
	return float64(segment.Size-segment.LiveBytes) / float64(segment.Size)
}
