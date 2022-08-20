package bitcask

import "sort"

type ValueLogSegmentStats struct {
	FileID     uint64
	TotalBytes int64
	LiveBytes  int64
	StaleBytes int64
	Active     bool
}

func (s ValueLogSegmentStats) StaleRatio() float64 {
	if s.TotalBytes <= 0 {
		return 0
	}
	return float64(s.StaleBytes) / float64(s.TotalBytes)
}

func (db *DB) ValueLogStats() []ValueLogSegmentStats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.valueLogStatsLocked()
}

func (db *DB) valueLogStatsLocked() []ValueLogSegmentStats {
	if db.valueLog == nil {
		return nil
	}

	byID := make(map[uint64]*ValueLogSegmentStats, len(db.valueLog.sizes))
	ids := make([]uint64, 0, len(db.valueLog.sizes))
	for id, size := range db.valueLog.sizes {
		stats := &ValueLogSegmentStats{FileID: id, TotalBytes: size, Active: id == db.valueLog.activeID}
		byID[id] = stats
		ids = append(ids, id)
	}
	for _, tree := range db.BPTreeIdx {
		records, err := tree.All()
		if err != nil {
			continue
		}
		for _, record := range records {
			if record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() || record.H.Meta.Ds != DataStructureValuePointer {
				continue
			}
			var pointerBytes []byte
			if record.E != nil {
				pointerBytes = record.E.Value
			} else {
				dataFile, err := newDataFileWithOptions(db.opt.Dir, record.H.FileID, db.opt.SegmentSize, db.opt.RWMode, db.opt)
				if err != nil {
					continue
				}
				entry, err := dataFile.ReadEntryAt(int(record.H.DataPos))
				dataFile.Close()
				if err != nil || entry == nil {
					continue
				}
				pointerBytes = entry.Value
			}
			ptr, err := decodeValuePointer(pointerBytes)
			if err != nil {
				continue
			}
			if stats := byID[ptr.FileID]; stats != nil {
				stats.LiveBytes += int64(ptr.Size)
			}
		}
	}

	result := make([]ValueLogSegmentStats, 0, len(ids))
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		stats := *byID[id]
		stats.StaleBytes = stats.TotalBytes - stats.LiveBytes
		if stats.StaleBytes < 0 {
			stats.StaleBytes = 0
		}
		result = append(result, stats)
	}
	return result
}

func (db *DB) PickValueLogGCCandidate(minStaleRatio float64) (ValueLogSegmentStats, bool) {
	stats := db.ValueLogStats()
	var best ValueLogSegmentStats
	found := false
	for _, segment := range stats {
		if segment.Active || segment.StaleRatio() < minStaleRatio {
			continue
		}
		if !found || segment.StaleRatio() > best.StaleRatio() || segment.StaleRatio() == best.StaleRatio() && segment.FileID < best.FileID {
			best = segment
			found = true
		}
	}
	return best, found
}
