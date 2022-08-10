package bitcask

import "sort"

type mergeSegment struct {
	id        int
	size      int64
	liveBytes int64
	readCount int64
}

func (db *DB) pickMergeFileIDs(fileIDs []int) []int {
	if len(fileIDs) == 0 {
		return fileIDs
	}
	sort.Ints(fileIDs)
	mode := db.opt.Compaction.Mode
	if mode == CompactionByFileID {
		return fileIDs
	}

	segments := db.mergeSegments(fileIDs)
	var best mergeSegment
	var bestScore float64
	for _, segment := range segments {
		garbageRatio := segment.garbageRatio()
		if garbageRatio < db.opt.Compaction.MinGarbageRatio {
			continue
		}
		score := garbageRatio
		if mode == CompactionHotCold && segment.readCount > int64(db.opt.Compaction.HotKeySampleWindow) {
			score *= 0.5
		}
		if score <= bestScore {
			continue
		}
		best = segment
		bestScore = score
	}
	if bestScore == 0 {
		return fileIDs
	}
	return []int{best.id}
}

func (db *DB) mergeSegments(fileIDs []int) []mergeSegment {
	segments := make([]mergeSegment, 0, len(fileIDs))
	for _, id := range fileIDs {
		segments = append(segments, mergeSegment{id: id, size: db.opt.SegmentSize})
	}
	return segments
}

func (s mergeSegment) garbageRatio() float64 {
	if s.size <= 0 {
		return 0
	}
	return float64(s.size-s.liveBytes) / float64(s.size)
}
