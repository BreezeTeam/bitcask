package bitcask

import (
	"io"
	"sort"
)

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

	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		return fileIDs
	}
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

func (db *DB) mergeSegments(fileIDs []int) ([]mergeSegment, error) {
	segments := make([]mergeSegment, len(fileIDs))
	byID := make(map[int64]*mergeSegment, len(fileIDs))
	for i, id := range fileIDs {
		segments[i] = mergeSegment{id: id}
		byID[int64(id)] = &segments[i]
	}

	for i := range segments {
		segments[i].readCount = db.segmentReadCount(int64(segments[i].id))
		segment := segments[i]
		dataFile, err := newDataFileWithOptions(db.opt.Dir, int64(segment.id), db.opt.SegmentSize, db.opt.RWMode, db.opt)
		if err != nil {
			return nil, err
		}
		var off int64
		for {
			entry, readErr := dataFile.ReadEntryAt(int(off))
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				dataFile.Close()
				return nil, readErr
			}
			if entry == nil {
				break
			}
			off += entry.Size()
			if off >= db.opt.SegmentSize {
				break
			}
		}
		if err := dataFile.Close(); err != nil {
			return nil, err
		}
		byID[int64(segment.id)].size = off
	}

	for _, tree := range db.BPTreeIdx {
		records, err := tree.All()
		if err != nil {
			continue
		}
		for _, record := range records {
			segment := byID[record.H.FileID]
			if segment == nil || record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() {
				continue
			}
			segment.liveBytes += int64(uint32(DataEntryHeaderSize) + record.H.Meta.KeySize + record.H.Meta.ValueSize + record.H.Meta.BucketSize)
		}
	}
	return segments, nil
}

func budgetMergeFileIDs(segments []mergeSegment, maxBytes int64) []int {
	if maxBytes <= 0 {
		return nil
	}
	selected := make([]int, 0, len(segments))
	var used int64
	for _, segment := range segments {
		if segment.size <= 0 || used+segment.size > maxBytes {
			continue
		}
		selected = append(selected, segment.id)
		used += segment.size
	}
	return selected
}

func (s mergeSegment) garbageRatio() float64 {
	if s.size <= 0 {
		return 0
	}
	return float64(s.size-s.liveBytes) / float64(s.size)
}
