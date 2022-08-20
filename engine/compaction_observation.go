package bitcask

import (
	"sync/atomic"
)

type CompactionObservation struct {
	LogicalBytes       int64
	LiveBytes          int64
	ObsoleteBytes      int64
	PhysicalBytes      int64
	MergeBytesWritten  uint64
	WriteAmplification float64
	SpaceAmplification float64
	SegmentReads       map[int64]uint64
}

func (db *DB) recordSegmentRead(fileID int64) {
	counter, _ := db.segmentReads.LoadOrStore(fileID, new(uint64))
	atomic.AddUint64(counter.(*uint64), 1)
}

func (db *DB) segmentReadCount(fileID int64) int64 {
	counter, ok := db.segmentReads.Load(fileID)
	if !ok {
		return 0
	}
	return int64(atomic.LoadUint64(counter.(*uint64)))
}

func (db *DB) CompactionObservation() CompactionObservation {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, fileIDs := db.getMaxFileIDAndFileIDs()
	segments, err := db.mergeSegments(fileIDs)
	if err != nil {
		return CompactionObservation{}
	}
	observation := CompactionObservation{SegmentReads: make(map[int64]uint64, len(segments))}
	for _, segment := range segments {
		observation.LogicalBytes += segment.size
		observation.LiveBytes += segment.liveBytes
		observation.SegmentReads[int64(segment.id)] = uint64(segment.readCount)
	}
	observation.ObsoleteBytes = observation.LogicalBytes - observation.LiveBytes
	observation.PhysicalBytes = int64(len(segments)) * db.opt.SegmentSize
	observation.MergeBytesWritten = atomic.LoadUint64(&db.metrics.mergeBytesWritten)
	if observation.LiveBytes > 0 {
		observation.SpaceAmplification = float64(observation.PhysicalBytes) / float64(observation.LiveBytes)
	}
	bytesWritten := atomic.LoadUint64(&db.metrics.bytesWritten)
	if bytesWritten > 0 {
		observation.WriteAmplification = float64(bytesWritten+observation.MergeBytesWritten) / float64(bytesWritten)
	}
	return observation
}
