package bitcask

import (
	"errors"
	"os"
)

var ErrValueLogGCNoCandidate = errors.New("no value-log GC candidate")

type ValueLogGCResult struct {
	SourceFileID   uint64
	SourceBytes    int64
	LiveBytes      int64
	StaleBytes     int64
	ValuesCopied   int
	BytesReclaimed int64
}

type liveValuePointer struct {
	bucket   string
	key      []byte
	pointer  valuePointer
	metadata *MetaData
}

func (db *DB) ValueLogGC(minStaleRatio float64) (ValueLogGCResult, error) {
	candidate, ok := db.PickValueLogGCCandidate(minStaleRatio)
	if !ok {
		return ValueLogGCResult{}, ErrValueLogGCNoCandidate
	}

	tx, err := db.Begin(true)
	if err != nil {
		return ValueLogGCResult{}, err
	}
	pointers, err := db.liveValuePointersLocked(candidate.FileID)
	if err != nil {
		tx.Rollback()
		return ValueLogGCResult{}, err
	}
	manifest := valueLogGCManifest{
		Phase:                  valueLogGCPrepared,
		SourceFileID:           candidate.FileID,
		FirstReplacementFileID: db.valueLog.activeID,
		LastReplacementFileID:  db.valueLog.activeID,
	}
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		tx.Rollback()
		return ValueLogGCResult{}, err
	}
	if err := db.injectSemanticFault(FaultPointGCPrepared); err != nil {
		tx.Rollback()
		return ValueLogGCResult{}, err
	}
	result := ValueLogGCResult{
		SourceFileID: candidate.FileID,
		SourceBytes:  candidate.TotalBytes,
		LiveBytes:    candidate.LiveBytes,
		StaleBytes:   candidate.StaleBytes,
	}
	for _, live := range pointers {
		value, err := db.valueLog.Read(live.pointer)
		if err != nil {
			tx.Rollback()
			return ValueLogGCResult{}, err
		}
		replacement, err := db.valueLog.Append(value)
		if err != nil {
			tx.Rollback()
			return ValueLogGCResult{}, err
		}
		if err := tx.put(live.bucket, live.key, encodeValuePointer(replacement), live.metadata.TTL, live.metadata.Flag, live.metadata.Timestamp, DataStructureValuePointer); err != nil {
			tx.Rollback()
			return ValueLogGCResult{}, err
		}
		result.ValuesCopied++
	}
	manifest.LastReplacementFileID = db.valueLog.activeID
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		tx.Rollback()
		return ValueLogGCResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ValueLogGCResult{}, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.syncDurabilityResources(); err != nil {
		return ValueLogGCResult{}, err
	}
	manifest.Phase = valueLogGCPointersInstalled
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		return ValueLogGCResult{}, err
	}
	if err := db.injectSemanticFault(FaultPointGCPointersInstalled); err != nil {
		return ValueLogGCResult{}, err
	}
	remaining, err := db.liveValuePointersLocked(candidate.FileID)
	if err != nil {
		return ValueLogGCResult{}, err
	}
	if len(remaining) != 0 {
		return ValueLogGCResult{}, errors.New("value-log GC source still has live pointers")
	}
	if err := db.injectSemanticFault(FaultPointGCBeforeSourceRemove); err != nil {
		return ValueLogGCResult{}, err
	}
	if db.valueLog.files[candidate.FileID] == nil {
		return ValueLogGCResult{}, ErrValuePointer
	}
	if err := db.removeValueLogGCSource(candidate.FileID); err != nil {
		return ValueLogGCResult{}, err
	}
	manifest.Phase = valueLogGCSourceRemoved
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		return ValueLogGCResult{}, err
	}
	if err := db.injectSemanticFault(FaultPointGCSourceRemoved); err != nil {
		return ValueLogGCResult{}, err
	}
	manifest.Phase = valueLogGCFinalized
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		return ValueLogGCResult{}, err
	}
	if err := db.injectSemanticFault(FaultPointGCFinalized); err != nil {
		return ValueLogGCResult{}, err
	}
	if err := db.clearValueLogGCManifest(); err != nil {
		return ValueLogGCResult{}, err
	}
	result.BytesReclaimed = result.SourceBytes
	return result, nil
}

func (db *DB) liveValuePointersLocked(fileID uint64) ([]liveValuePointer, error) {
	var result []liveValuePointer
	for bucket, tree := range db.BPTreeIdx {
		records, err := tree.All()
		if err != nil {
			continue
		}
		for _, record := range records {
			if record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() || record.H.Meta.Ds != DataStructureValuePointer {
				continue
			}
			entry := record.E
			if entry == nil {
				dataFile, err := newDataFileWithOptions(db.opt.Dir, record.H.FileID, db.opt.SegmentSize, db.opt.RWMode, db.opt)
				if err != nil {
					return nil, err
				}
				entry, err = dataFile.ReadEntryAt(int(record.H.DataPos))
				closeErr := dataFile.Close()
				if err != nil {
					return nil, err
				}
				if closeErr != nil {
					return nil, closeErr
				}
			}
			ptr, err := decodeValuePointer(entry.Value)
			if err != nil {
				return nil, err
			}
			if ptr.FileID != fileID {
				continue
			}
			metadata := *record.H.Meta
			result = append(result, liveValuePointer{
				bucket:   bucket,
				key:      append([]byte(nil), record.H.Key...),
				pointer:  ptr,
				metadata: &metadata,
			})
		}
	}
	return result, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
