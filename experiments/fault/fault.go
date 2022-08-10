package fault

type Point int

const (
	PointWrite Point = iota
	PointSync
	PointRead
)

type Schedule struct {
	WriteFailAfter   int64
	SyncFailAfter    int64
	ReadCorruptAfter int64
}

type Counters struct {
	Writes int64
	Syncs  int64
	Reads  int64
}

type Event struct {
	Point Point
	Fault bool
}

func (s Schedule) Observe(c Counters, point Point) Event {
	switch point {
	case PointWrite:
		return Event{Point: point, Fault: shouldFault(s.WriteFailAfter, c.Writes+1)}
	case PointSync:
		return Event{Point: point, Fault: shouldFault(s.SyncFailAfter, c.Syncs+1)}
	case PointRead:
		return Event{Point: point, Fault: shouldFault(s.ReadCorruptAfter, c.Reads+1)}
	default:
		return Event{Point: point}
	}
}

func Replay(s Schedule, points []Point) []Event {
	counters := Counters{}
	events := make([]Event, 0, len(points))
	for _, point := range points {
		event := s.Observe(counters, point)
		events = append(events, event)
		switch point {
		case PointWrite:
			counters.Writes++
		case PointSync:
			counters.Syncs++
		case PointRead:
			counters.Reads++
		}
	}
	return events
}

func shouldFault(limit, count int64) bool {
	return limit >= 0 && count > limit
}
