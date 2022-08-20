package bitcask

import (
	"github.com/BreezeTeam/bitcask/internal/helper"
	"github.com/BreezeTeam/bitcask/internal/id"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

var (
	// ErrKeyAndValSize is returned when given key and value size is too big.
	ErrKeyAndValSize = errors.New("key and value size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNotWritable is returned when performing a write operation on
	// a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if an empty key is passed on an update function.
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrBucketEmpty is returned if bucket is empty.
	ErrBucketEmpty = errors.New("bucket is empty")

	// ErrRangeScan is returned when range scanning not found the result
	ErrRangeScan = errors.New("range scans not found")

	// ErrPrefixScan is returned when prefix scanning not found the result
	ErrPrefixScan = errors.New("prefix scans not found")

	// ErrPrefixSearchScan is returned when prefix and search scanning not found the result
	ErrPrefixSearchScan = errors.New("prefix and search scans not found")

	// ErrNotFoundKey is returned when key not found int the bucket on an view function.
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

// Tx represents a transaction.
type Tx struct {
	id                     uint64
	db                     *DB
	writable               bool
	pendingWrites          []*Entry
	ReservedStoreTxIDIdxes map[int64]*BPTree
}

type KV struct {
	Key   []byte
	Value []byte
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (tx *Tx, err error) {
	tx, err = newTx(db, writable)
	if err != nil {
		return nil, err
	}

	tx.lock()

	if db.closed {
		tx.unlock()
		return nil, ErrDBClosed
	}

	return
}

// newTx returns a newly initialized Tx object at given writable.
func newTx(db *DB, writable bool) (tx *Tx, err error) {
	tx = &Tx{
		db:                     db,
		writable:               writable,
		pendingWrites:          []*Entry{},
		ReservedStoreTxIDIdxes: make(map[int64]*BPTree),
	}

	tx.id = tx.getTxID()

	return
}

// getTxID returns the tx id.
func (tx *Tx) getTxID() uint64 {
	return id.SnowflakeID()
}

// Commit commits the transaction, following these steps:
//
// 1. check the length of pendingWrites.If there are no writes, return immediately.
//
// 2. check if the ActiveFile has not enough space to store entry. if not, call rotateActiveFile function.
//
// 3. write pendingWrites to disk, if a non-nil error,return the error.
//
// 4. build Hint index.
//
// 5. Unlock the database and clear the db field.
func (tx *Tx) Commit() (err error) {
	if tx.db == nil {
		return ErrTxClosed
	}
	started := time.Now()
	db := tx.db
	defer func() {
		if err == nil {
			db.commitLatency.observe(time.Since(started))
		}
	}()

	writesLen := len(tx.pendingWrites)
	if writesLen == 0 {
		tx.close()
		return nil
	}

	countFlag := CountFlagEnabled
	if db.isMerging {
		countFlag = CountFlagDisabled
	}

	bytesWritten, err := tx.commitEntries(countFlag)
	if err != nil {
		tx.close()
		return err
	}
	if db.effectiveSyncPolicy() == SyncPolicyGroupCommit {
		db.dirtyBytes += bytesWritten
		db.dirtyCommits++
		request, err := db.groupCommit.enqueue()
		tx.close()
		if err != nil {
			return err
		}
		if err := db.groupCommit.wait(request); err != nil {
			return err
		}
	} else {
		if err := tx.syncAfterTransaction(bytesWritten); err != nil {
			tx.close()
			return err
		}
		tx.close()
	}
	atomic.AddUint64(&db.metrics.commits, 1)
	atomic.AddUint64(&db.metrics.entriesWritten, uint64(writesLen))
	atomic.AddUint64(&db.metrics.bytesWritten, uint64(bytesWritten))

	return nil
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	tx.unlock()
	tx.db = nil
	tx.pendingWrites = nil
	tx.ReservedStoreTxIDIdxes = nil
}

func (tx *Tx) commitEntries(countFlag bool) (int64, error) {
	var (
		bucketMetaTemp BucketMeta
		encodeBuf      []byte
		bytesWritten   int64
	)
	lastIndex := len(tx.pendingWrites) - 1

	for i, entry := range tx.pendingWrites {
		bucket := string(entry.Meta.Bucket)
		if err := tx.db.injectSemanticFault(FaultPointBeforeEntry); err != nil {
			return 0, err
		}
		if i == lastIndex {
			if err := tx.db.injectSemanticFault(FaultPointBeforeCommitMarker); err != nil {
				return 0, err
			}
			entry.Meta.Status = Committed
		}

		off, err := tx.appendEntry(entry, &encodeBuf)
		if err != nil {
			return 0, err
		}
		bytesWritten += entry.Size()

		if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
			bucketMetaTemp = tx.buildTempBucketMetaIdx(bucket, entry.Key, bucketMetaTemp)
		}

		if err := tx.updateIndexesForEntry(entry, bucket, off, i == lastIndex, bucketMetaTemp, countFlag); err != nil {
			return 0, err
		}
	}
	return bytesWritten, nil
}

func (tx *Tx) appendEntry(entry *Entry, encodeBuf *[]byte) (int64, error) {
	if err := tx.maybeSeparateValue(entry); err != nil {
		return 0, err
	}
	entrySize := entry.Size()
	if entrySize > tx.db.opt.SegmentSize {
		return 0, ErrKeyAndValSize
	}
	if tx.db.ActiveFile.ActualSize+entrySize > tx.db.opt.SegmentSize {
		if err := tx.rotateActiveFile(); err != nil {
			return 0, err
		}
	}
	if entry.Meta.Ds == DataStructureBPTree || entry.Meta.Ds == DataStructureValuePointer {
		tx.db.BPTreeKeyEntryPosMap[entryIndexKey(entry.Meta.Bucket, entry.Key)] = tx.db.ActiveFile.writeOff
	}

	off := tx.db.ActiveFile.writeOff
	*encodeBuf = entry.EncodeTo(*encodeBuf)
	if _, err := tx.db.ActiveFile.WriteAt(*encodeBuf, tx.db.ActiveFile.writeOff); err != nil {
		return 0, err
	}
	tx.db.ActiveFile.ActualSize += entrySize
	tx.db.ActiveFile.writeOff += entrySize
	return off, nil
}

func (tx *Tx) maybeSeparateValue(entry *Entry) error {
	opt := tx.db.opt.KVSeparation
	if !opt.Enable || tx.db.valueLog == nil || entry.Meta.Flag != DataSetFlag || entry.Meta.Ds != DataStructureBPTree {
		return nil
	}
	separate := len(entry.Value) >= opt.Threshold
	if opt.LifecycleEnable {
		separate = tx.db.shouldSeparateLifecycle(string(entry.Meta.Bucket), entry.Key, entry.Value)
	}
	if !separate {
		return nil
	}
	if err := tx.db.injectSemanticFault(FaultPointBeforeValueAppend); err != nil {
		return err
	}
	ptr, err := tx.db.valueLog.Append(entry.Value)
	if err != nil {
		return err
	}
	entry.Value = encodeValuePointer(ptr)
	entry.Meta.ValueSize = uint32(len(entry.Value))
	entry.Meta.Ds = DataStructureValuePointer
	return nil
}

func (tx *Tx) updateIndexesForEntry(entry *Entry, bucket string, off int64, isLast bool, bucketMetaTemp BucketMeta, countFlag bool) error {
	if isLast {
		txID := entry.Meta.TxID
		if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
			if err := tx.buildTxIDRootIdx(txID, countFlag); err != nil {
				return err
			}
			if err := tx.buildBucketMetaIdx(bucket, entry.Key, bucketMetaTemp); err != nil {
				return err
			}
		} else {
			tx.db.committedTxIds[txID] = struct{}{}
		}
	}

	var e *Entry
	if tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		e = entry
	}
	if isBPTreeIndexedDataStructure(entry.Meta.Ds) {
		tx.buildBPTreeIdx(bucket, entry, e, off, countFlag)
	}
	if entry.Meta.Ds == DataStructureNone && entry.Meta.Flag == DataBPTreeBucketDeleteFlag {
		tx.db.deleteBucket(DataStructureBPTree, bucket)
	}
	return nil
}

func (tx *Tx) buildTempBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) BucketMeta {
	keySize := uint32(len(key))
	if bucketMetaTemp.start == nil {
		bucketMetaTemp = BucketMeta{start: key, end: key, startSize: keySize, endSize: keySize}
	} else {
		if compare(bucketMetaTemp.start, key) > 0 {
			bucketMetaTemp.start = key
			bucketMetaTemp.startSize = keySize
		}

		if compare(bucketMetaTemp.end, key) < 0 {
			bucketMetaTemp.end = key
			bucketMetaTemp.endSize = keySize
		}
	}

	return bucketMetaTemp
}

func (tx *Tx) buildBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) error {
	bucketMeta, ok := tx.db.bucketMetas[bucket]

	start := bucketMetaTemp.start
	startSize := uint32(len(start))
	end := bucketMetaTemp.end
	endSize := uint32(len(end))
	var updateFlag bool

	if !ok {
		bucketMeta = &BucketMeta{start: start, end: end, startSize: startSize, endSize: endSize}
		updateFlag = true
	} else {
		if compare(bucketMeta.start, bucketMetaTemp.start) > 0 {
			bucketMeta.start = start
			bucketMeta.startSize = startSize
			updateFlag = true
		}

		if compare(bucketMeta.end, bucketMetaTemp.end) < 0 {
			bucketMeta.end = end
			bucketMeta.endSize = endSize
			updateFlag = true
		}
	}

	if updateFlag {
		fd, err := os.OpenFile(tx.db.getBucketMetaFilePath(bucket), os.O_CREATE|os.O_RDWR, 0644)
		defer fd.Close()
		if err != nil {
			return err
		}

		if _, err = fd.WriteAt(bucketMeta.Encode(), 0); err != nil {
			return err
		}

		if tx.db.opt.SyncEnable {
			if err = fd.Sync(); err != nil {
				return err
			}
		}
		tx.db.bucketMetas[bucket] = bucketMeta
	}

	return nil
}

func (tx *Tx) buildTxIDRootIdx(txID uint64, countFlag bool) error {
	txIDStr := helper.IntToStr(int(txID))

	err := tx.db.ActiveCommittedTxIdsIdx.Insert([]byte(txIDStr), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
	if err != nil {
		return err
	}
	if len(tx.ReservedStoreTxIDIdxes) > 0 {
		for fID, txIDIdx := range tx.ReservedStoreTxIDIdxes {
			filePath := tx.db.getBPTTxIDPath(fID)

			txIDIdx.Insert([]byte(txIDStr), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
			txIDIdx.Filepath = filePath

			err := txIDIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}

			filePath = tx.db.getBPTRootTxIDPath(fID)
			txIDRootIdx := NewTree()
			rootAddress := helper.Int64ToStr(txIDIdx.root.Address)

			txIDRootIdx.Insert([]byte(rootAddress), nil, &Hint{Meta: &MetaData{Flag: DataSetFlag}}, countFlag)
			txIDRootIdx.Filepath = filePath

			err = txIDRootIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *Tx) buildBPTreeIdx(bucket string, entry, e *Entry, off int64, countFlag bool) {
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		newKey := []byte(bucket)
		newKey = append(newKey, entry.Key...)
		tx.db.ActiveBPTreeIdx.Insert(newKey, e, &Hint{
			FileID:  tx.db.ActiveFile.fileID,
			Key:     newKey,
			Meta:    entry.Meta,
			DataPos: uint64(off),
		}, countFlag)
	} else {
		if _, ok := tx.db.BPTreeIdx[bucket]; !ok {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}

		if tx.db.BPTreeIdx[bucket] == nil {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}
		_ = tx.db.BPTreeIdx[bucket].Insert(entry.Key, e, &Hint{
			FileID:  tx.db.ActiveFile.fileID,
			Key:     entry.Key,
			Meta:    entry.Meta,
			DataPos: uint64(off),
		}, countFlag)
	}
}

// rotateActiveFile rotates log file when active file is not enough space to store the entry.
func (tx *Tx) rotateActiveFile() error {
	atomic.AddUint64(&tx.db.metrics.rotations, 1)
	var err error
	fID := tx.db.MaxFileID
	tx.db.MaxFileID++

	oldActiveFile := tx.db.ActiveFile
	if tx.db.effectiveSyncPolicy() == SyncPolicyNone {
		if err := oldActiveFile.rwManager.Close(); err != nil {
			return err
		}
	} else {
		tx.db.pendingDurabilityFiles = append(tx.db.pendingDurabilityFiles, oldActiveFile)
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		tx.db.ActiveBPTreeIdx.Filepath = tx.db.getBPTPath(fID)
		tx.db.ActiveBPTreeIdx.enabledKeyPosMap = true
		tx.db.ActiveBPTreeIdx.SetKeyPosMap(tx.db.BPTreeKeyEntryPosMap)

		err = tx.db.ActiveBPTreeIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 1)
		if err != nil {
			return err
		}

		BPTreeRootIdx := &BPTreeRootIdx{
			rootOff:   uint64(tx.db.ActiveBPTreeIdx.root.Address),
			fID:       uint64(fID),
			startSize: uint32(len(tx.db.ActiveBPTreeIdx.FirstKey)),
			endSize:   uint32(len(tx.db.ActiveBPTreeIdx.LastKey)),
			start:     tx.db.ActiveBPTreeIdx.FirstKey,
			end:       tx.db.ActiveBPTreeIdx.LastKey,
		}

		_, err := BPTreeRootIdx.Persistence(tx.db.getBPTRootPath(fID),
			0, tx.db.opt.SyncEnable)
		if err != nil {
			return err
		}

		tx.db.BPTreeRootIdxes = append(tx.db.BPTreeRootIdxes, BPTreeRootIdx)

		// clear and reset BPTreeKeyEntryPosMap
		tx.db.BPTreeKeyEntryPosMap = nil
		tx.db.BPTreeKeyEntryPosMap = make(map[string]int64)

		// clear and reset ActiveBPTreeIdx
		tx.db.ActiveBPTreeIdx = nil
		tx.db.ActiveBPTreeIdx = NewTree()

		tx.ReservedStoreTxIDIdxes[fID] = tx.db.ActiveCommittedTxIdsIdx

		// clear and reset ActiveCommittedTxIdsIdx
		tx.db.ActiveCommittedTxIdsIdx = nil
		tx.db.ActiveCommittedTxIdsIdx = NewTree()
	}

	// reset ActiveFile
	tx.db.ActiveFile, err = newDataFileWithOptions(tx.db.opt.Dir, tx.db.MaxFileID, tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
	if err != nil {
		return err
	}

	tx.db.ActiveFile.fileID = tx.db.MaxFileID
	return nil
}

// Rollback closes the transaction.
func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return ErrTxClosed
	}

	tx.close()
	return nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureBPTree)
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

func (tx *Tx) PutBatch(bucket string, items []KV, ttl uint32) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}
	if bucket == "" {
		return ErrBucketEmpty
	}
	for _, item := range items {
		if len(item.Key) == 0 {
			return ErrKeyEmpty
		}
	}
	if len(items) == 0 {
		return nil
	}
	pending := make([]*Entry, 0, len(tx.pendingWrites)+len(items))
	pending = append(pending, tx.pendingWrites...)
	timestamp := uint64(time.Now().Unix())
	bucketBytes := []byte(bucket)
	entryBytesSize := 0
	for _, item := range items {
		entryBytesSize += len(item.Key) + len(item.Value)
	}
	entryBytes := make([]byte, entryBytesSize)
	entryBytesOff := 0
	for _, item := range items {
		tx.db.recordAutonomousWrite(bucket, item.Key, item.Value)
		key := entryBytes[entryBytesOff : entryBytesOff+len(item.Key)]
		copy(key, item.Key)
		entryBytesOff += len(item.Key)
		value := entryBytes[entryBytesOff : entryBytesOff+len(item.Value)]
		copy(value, item.Value)
		entryBytesOff += len(item.Value)
		pending = append(pending, newPendingEntryWithOwnedBytes(bucketBytes, key, value, ttl, DataSetFlag, timestamp, DataStructureBPTree, tx.id))
	}
	tx.pendingWrites = pending
	return nil
}

func (tx *Tx) checkTxIsClosed() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	return nil
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	tx.db.lifecycleRecordRead(bucket, key)
	tx.db.recordAutonomousRead()
	idxMode := tx.db.opt.EntryIdxMode

	if idxMode == HintBPTSparseIdxMode {
		return tx.getByHintBPTSparseIdx(bucket, key)
	}

	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {
		if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
			r, err := idx.Find(key)
			if err != nil {
				return nil, err
			}

			if _, ok := tx.db.committedTxIds[r.H.Meta.TxID]; !ok {
				return nil, ErrNotFoundKey
			}

			if r.H.Meta.Flag == DataDeleteFlag || r.IsExpired() {
				return nil, ErrNotFoundKey
			}

			tx.db.recordSegmentRead(r.H.FileID)
			if idxMode == HintKeyValAndRAMIdxMode {
				return tx.resolveValuePointer(r.E)
			}

			if idxMode == HintKeyAndRAMIdxMode {
				df, err := newDataFileWithOptions(tx.db.opt.Dir, r.H.FileID, tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
				defer df.rwManager.Close()

				if err != nil {
					return nil, err
				}

				item, err := df.ReadEntryAt(int(r.H.DataPos))
				if err != nil {
					return nil, fmt.Errorf("read err. pos %d, key %s, err %s", r.H.DataPos, string(key), err)
				}

				return tx.resolveValuePointer(item)
			}
		}
	}

	return nil, ErrBucketAndKey(bucket, key)
}

func (tx *Tx) Range(bucket string, start, end []byte) ([]KV, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return nil, ErrNotSupportHintBPTSparseIdxMode
	}
	idx, ok := tx.db.BPTreeIdx[bucket]
	if !ok {
		return nil, ErrBucketAndKey(bucket, start)
	}
	records, err := idx.Range(start, end)
	if err != nil {
		return nil, err
	}
	return tx.visibleKVs(records)
}

func (tx *Tx) Prefix(bucket string, prefix []byte, offsetNum int, limitNum int) ([]KV, int, error) {
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, 0, err
	}
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return nil, 0, ErrNotSupportHintBPTSparseIdxMode
	}
	idx, ok := tx.db.BPTreeIdx[bucket]
	if !ok {
		return nil, 0, ErrBucketAndKey(bucket, prefix)
	}
	records, off, err := idx.PrefixScan(prefix, offsetNum, limitNum)
	if err != nil {
		return nil, off, err
	}
	kvs, err := tx.visibleKVs(records)
	return kvs, off, err
}

func (tx *Tx) visibleKVs(records Records) ([]KV, error) {
	kvs := make([]KV, 0, len(records))
	for _, record := range records {
		if _, ok := tx.db.committedTxIds[record.H.Meta.TxID]; !ok {
			continue
		}
		if record.H.Meta.Flag == DataDeleteFlag || record.IsExpired() {
			continue
		}

		tx.db.recordSegmentRead(record.H.FileID)
		entry := record.E
		if tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
			df, err := newDataFileWithOptions(tx.db.opt.Dir, record.H.FileID, tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
			if err != nil {
				return nil, err
			}
			entry, err = df.ReadEntryAt(int(record.H.DataPos))
			closeErr := df.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
		}
		if entry == nil {
			continue
		}
		entry, err := tx.resolveValuePointer(entry)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, KV{
			Key:   append([]byte(nil), record.H.Key...),
			Value: append([]byte(nil), entry.Value...),
		})
	}
	return kvs, nil
}

// ErrBucketAndKey returns when bucket or key not found.
func ErrBucketAndKey(bucket string, key []byte) error {
	return fmt.Errorf("%w:bucket:%s,key:%s", ErrBucketNotFound, bucket, key)
}

func (tx *Tx) resolveValuePointer(entry *Entry) (*Entry, error) {
	if entry == nil || entry.Meta.Ds != DataStructureValuePointer {
		return entry, nil
	}
	if tx.db.valueLog == nil {
		return nil, ErrValuePointer
	}
	ptr, err := decodeValuePointer(entry.Value)
	if err != nil {
		return nil, err
	}
	value, err := tx.db.valueLog.Read(ptr)
	if err != nil {
		return nil, err
	}
	resolved := *entry
	resolved.Value = value
	meta := *entry.Meta
	meta.ValueSize = uint32(len(value))
	meta.Ds = DataStructureBPTree
	resolved.Meta = &meta
	return &resolved, nil
}

// put sets the value for a key in the bucket.
// Returns an error if tx is closed, if performing a write operation on a read-only transaction, if the key is empty.
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	if len(key) == 0 {
		return ErrKeyEmpty
	}

	tx.db.recordAutonomousWrite(bucket, key, value)
	tx.pendingWrites = append(tx.pendingWrites, newPendingEntry(bucket, key, value, ttl, flag, timestamp, ds, tx.id))

	return nil
}

func newPendingEntry(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16, txID uint64) *Entry {
	return newPendingEntryWithBucket([]byte(bucket), key, value, ttl, flag, timestamp, ds, txID)
}

func newPendingEntryWithBucket(bucket, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16, txID uint64) *Entry {
	return newPendingEntryWithOwnedBytes(bucket, append([]byte(nil), key...), append([]byte(nil), value...), ttl, flag, timestamp, ds, txID)
}

func newPendingEntryWithOwnedBytes(bucket, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16, txID uint64) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(value)),
			Timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			Bucket:     bucket,
			BucketSize: uint32(len(bucket)),
			Status:     UnCommitted,
			Ds:         ds,
			TxID:       txID,
		},
	}
}

func entryIndexKey(bucket, key []byte) string {
	buf := make([]byte, 4+len(bucket)+len(key))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(bucket)))
	copy(buf[4:], bucket)
	copy(buf[4+len(bucket):], key)
	return string(buf)
}

func isBPTreeIndexedDataStructure(ds uint16) bool {
	return ds == DataStructureBPTree || ds == DataStructureValuePointer
}

func (tx *Tx) getByHintBPTSparseIdx(bucket string, key []byte) (e *Entry, err error) {
	newKey := getNewKey(bucket, key)

	entry, err := tx.getByHintBPTSparseIdxInMem(newKey)
	if entry != nil && err == nil {
		if entry.Meta.Flag == DataDeleteFlag || IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
			return nil, ErrNotFoundKey
		}
		return entry, err
	}

	entry, err = tx.getByHintBPTSparseIdxOnDisk(bucket, key)
	if entry != nil && err == nil {
		return entry, err
	}

	return nil, ErrNotFoundKey
}

func (tx *Tx) getByHintBPTSparseIdxInMem(key []byte) (e *Entry, err error) {
	// Read in memory.
	r, err := tx.db.ActiveBPTreeIdx.Find(key)
	if err == nil && r != nil {
		if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(helper.Int64ToStr(int64(r.H.Meta.TxID)))); err == nil {
			df, err := newDataFileWithOptions(tx.db.opt.Dir, r.H.FileID, tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
			defer df.rwManager.Close()
			if err != nil {
				return nil, err
			}

			return df.ReadEntryAt(int(r.H.DataPos))
		}

		return nil, ErrNotFoundKey
	}

	return nil, nil
}

// FindLeafOnDisk returns binary leaf node on disk at given fId, rootOff and key.
func (tx *Tx) FindLeafOnDisk(fID int64, rootOff int64, key, newKey []byte) (bn *BinaryNode, err error) {
	var i uint16
	var curr *BinaryNode

	filepath := tx.db.getBPTPath(fID)
	curr, err = ReadNode(filepath, rootOff)
	if err != nil {
		return nil, err
	}

	for curr.IsLeaf != 1 {
		i = 0
		for i < curr.KeysNum {
			df, err := newDataFileWithOptions(tx.db.opt.Dir, fID, tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
			if err != nil {
				return nil, err
			}

			item, err := df.ReadEntryAt(int(curr.Keys[i]))
			df.rwManager.Close()

			if err != nil {
				return nil, err
			}

			newKeyTemp := getNewKey(string(item.Meta.Bucket), item.Key)
			if compare(newKey, newKeyTemp) >= 0 {
				i++
			} else {
				break
			}
		}
		address := curr.Pointers[i]

		curr, err = ReadNode(filepath, address)
	}

	return curr, nil
}

// FindOnDisk returns entry on disk at given fID, rootOff and key.
func (tx *Tx) FindOnDisk(fID uint64, rootOff uint64, key, newKey []byte) (entry *Entry, err error) {
	var (
		bnLeaf *BinaryNode
		i      uint16
		df     *DataFile
	)

	bnLeaf, err = tx.FindLeafOnDisk(int64(fID), int64(rootOff), key, newKey)

	if bnLeaf == nil {
		return nil, ErrKeyNotFound
	}

	for i = 0; i < bnLeaf.KeysNum; i++ {
		df, err = newDataFileWithOptions(tx.db.opt.Dir, int64(fID), tx.db.opt.SegmentSize, tx.db.opt.RWMode, tx.db.opt)
		if err != nil {
			return nil, err
		}

		entry, err = df.ReadEntryAt(int(bnLeaf.Keys[i]))
		df.rwManager.Close()

		if err != nil {
			return nil, err
		}

		newKeyTemp := getNewKey(string(entry.Meta.Bucket), entry.Key)
		if entry != nil && compare(newKey, newKeyTemp) == 0 {
			return entry, nil
		}
	}

	if i == bnLeaf.KeysNum {
		return nil, ErrKeyNotFound
	}

	return
}

func (tx *Tx) getByHintBPTSparseIdxOnDisk(bucket string, key []byte) (e *Entry, err error) {
	// Read on disk.
	var bptSparseIdxGroup []*BPTreeRootIdx
	for _, bptRootIdxPointer := range tx.db.BPTreeRootIdxes {
		bptSparseIdxGroup = append(bptSparseIdxGroup, &BPTreeRootIdx{
			fID:     bptRootIdxPointer.fID,
			rootOff: bptRootIdxPointer.rootOff,
			start:   bptRootIdxPointer.start,
			end:     bptRootIdxPointer.end,
		})
	}

	// Sort the fid from largest to smallest, to ensure that the latest data is first compared.
	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newKey := getNewKey(bucket, key)
	for _, bptSparse := range bptSparseIdxGroup {
		if compare(newKey, bptSparse.start) >= 0 && compare(newKey, bptSparse.end) <= 0 {
			fID := bptSparse.fID
			rootOff := bptSparse.rootOff

			e, err = tx.FindOnDisk(fID, rootOff, key, newKey)
			if err == nil && e != nil {
				if e.Meta.Flag == DataDeleteFlag || IsExpired(e.Meta.TTL, e.Meta.Timestamp) {
					return nil, ErrNotFoundKey
				}

				txIDStr := helper.Int64ToStr(int64(e.Meta.TxID))
				if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(txIDStr)); err == nil {
					return e, err
				}
				if ok, _ := tx.FindTxIDOnDisk(fID, e.Meta.TxID); !ok {
					return nil, ErrNotFoundKey
				}

				return e, err
			}
		}
		continue
	}

	return nil, nil
}

// FindTxIDOnDisk returns if txId on disk at given fid and txID.
func (tx *Tx) FindTxIDOnDisk(fID, txID uint64) (ok bool, err error) {
	var i uint16

	filepath := tx.db.getBPTRootTxIDPath(int64(fID))
	node, err := ReadNode(filepath, 0)

	if err != nil {
		return false, err
	}

	filepath = tx.db.getBPTTxIDPath(int64(fID))
	rootAddress := node.Keys[0]
	curr, err := ReadNode(filepath, rootAddress)

	if err != nil {
		return false, err
	}

	txIDStr := helper.IntToStr(int(txID))

	for curr.IsLeaf != 1 {
		i = 0
		for i < curr.KeysNum {
			if compare([]byte(txIDStr), []byte(helper.Int64ToStr(curr.Keys[i]))) >= 0 {
				i++
			} else {
				break
			}
		}

		address := curr.Pointers[i]
		curr, err = ReadNode(filepath, int64(address))
	}

	if curr == nil {
		return false, ErrKeyNotFound
	}

	for i = 0; i < curr.KeysNum; i++ {
		if compare([]byte(txIDStr), []byte(helper.Int64ToStr(curr.Keys[i]))) == 0 {
			break
		}
	}

	if i == curr.KeysNum {
		return false, ErrKeyNotFound
	}

	return true, nil
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

type sortBy func(p, q *BPTreeRootIdx) bool

// BPTreeRootIdxWrapper records BSGroup and by, in order to sort.
type BPTreeRootIdxWrapper struct {
	BSGroup []*BPTreeRootIdx
	by      func(p, q *BPTreeRootIdx) bool
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (bsw BPTreeRootIdxWrapper) Less(i, j int) bool {
	return bsw.by(bsw.BSGroup[i], bsw.BSGroup[j])
}

// Swap swaps the elements with indexes i and j.
func (bsw BPTreeRootIdxWrapper) Swap(i, j int) {
	bsw.BSGroup[i], bsw.BSGroup[j] = bsw.BSGroup[j], bsw.BSGroup[i]
}

// Len is the number of elements in the collection bsw.BSGroup.
func (bsw BPTreeRootIdxWrapper) Len() int {
	return len(bsw.BSGroup)
}

// SortFID sorts BPTreeRootIdx data.
func SortFID(BPTreeRootIdxGroup []*BPTreeRootIdx, by sortBy) {
	sort.Sort(BPTreeRootIdxWrapper{BSGroup: BPTreeRootIdxGroup, by: by})
}

// IsExpired checks the ttl if expired or not.
func IsExpired(ttl uint32, timestamp uint64) bool {
	now := time.Now().Unix()
	if ttl > 0 && uint64(ttl)+timestamp > uint64(now) || ttl == Persistent {
		return false
	}

	return true
}

func getNewKey(bucket string, key []byte) []byte {
	newKey := []byte(bucket)
	newKey = append(newKey, key...)
	return newKey
}
