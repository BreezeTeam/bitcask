package bitcask

import (
	"bitcask/helper"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

// 这里就是 value 其数据结构 类型的 flag
const (
	// DataStructureBPTree represents the data structure b+ tree flag
	// 表示数据结构b+树标志
	DataStructureBPTree uint16 = iota

	// DataStructureNone represents not the data structure
	// 不表示数据结构
	DataStructureNone
)

var (
	// ErrDBClosed is returned when db is closed.
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	ErrBucket = errors.New("err bucket")

	// ErrEntryIdxModeOpt is returned when set db EntryIdxMode option is wrong.
	ErrEntryIdxModeOpt = errors.New("err EntryIdxMode option set")

	// ErrFn is returned when fn is nil.
	ErrFn = errors.New("err fn")

	// ErrBucketNotFound is returned when looking for bucket that does not exist
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrNotSupportHintBPTSparseIdxMode is returned not support mode `HintBPTSparseIdxMode`
	ErrNotSupportHintBPTSparseIdxMode = errors.New("not support mode `HintBPTSparseIdxMode`")
)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag uint16 = iota

	// DataSetFlag represents the data set flag
	DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag

	// DataLSetFlag represents the data LSet flag
	DataLSetFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag

	// DataSetBucketDeleteFlag represents the delete Set bucket flag
	DataSetBucketDeleteFlag

	// DataSortedSetBucketDeleteFlag represents the delete Sorted Set bucket flag
	DataSortedSetBucketDeleteFlag

	// DataBPTreeBucketDeleteFlag represents the delete BPTree bucket flag
	DataBPTreeBucketDeleteFlag

	// DataListBucketDeleteFlag represents the delete List bucket flag
	DataListBucketDeleteFlag
)

//DB
type (
	DB struct {
		opt                     Options   // the database options
		BPTreeIdx               BPTreeIdx // Hint Index
		BPTreeRootIdxes         []*BPTreeRootIdx
		ActiveBPTreeIdx         *BPTree
		ActiveCommittedTxIdsIdx *BPTree
		BPTreeKeyEntryPosMap    map[string]int64 // key = bucket+key  val = EntryPos
		committedTxIds          map[uint64]struct{}
		MaxFileID               int64     //活动文件id
		ActiveFile              *DataFile //活动文件对象
		mu                      sync.RWMutex
		KeyCount                int // total key number ,include expired, deleted, repeated.
		closed                  bool
		isMerging               bool
		bucketMetas             BucketMetasIdx // BucketMeta 索引
	}

	// BPTreeIdx B+ tree 索引
	BPTreeIdx map[string]*BPTree

	// BucketMetasIdx 存储桶元信息的索引
	BucketMetasIdx map[string]*BucketMeta
)

func (db *DB) getBPTTxIDPath(fID int64) string {
	return db.getBPTDir() + "/txid/" + helper.Int64ToStr(fID) + BPTTxIDIndexSuffix
}

func (db *DB) getBPTRootTxIDPath(fID int64) string {
	return db.getBPTDir() + "/txid/" + helper.Int64ToStr(fID) + BPTRootTxIDIndexSuffix
}
func (db *DB) getBPTPath(fID int64) string {
	return db.getBPTDir() + "/" + helper.Int64ToStr(fID) + BPTIndexSuffix
}

// Open 根据  option.Options 开启一个 DB
func Open(opt Options) (*DB, error) {
	db := &DB{
		opt:                     opt,
		BPTreeKeyEntryPosMap:    make(map[string]int64),
		committedTxIds:          make(map[uint64]struct{}),
		MaxFileID:               0,
		KeyCount:                0,
		closed:                  false,
		isMerging:               false,
		bucketMetas:             make(map[string]*BucketMeta),
		ActiveCommittedTxIdsIdx: NewTree(),
		ActiveBPTreeIdx:         NewTree(),
		BPTreeIdx:               make(BPTreeIdx),
	}
	//判断文件夹在不在,不在就创建
	if ok := helper.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//创建需要的文件夹
	if opt.EntryIdxMode == HintBPTSparseIdxMode {
		bptRootIdxDir := db.opt.Dir + "/" + bptDir + "/root"
		if ok := helper.PathIsExist(bptRootIdxDir); !ok {
			if err := os.MkdirAll(bptRootIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}

		bptTxIDIdxDir := db.opt.Dir + "/" + bptDir + "/txid"
		if ok := helper.PathIsExist(bptTxIDIdxDir); !ok {
			if err := os.MkdirAll(bptTxIDIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}
		bucketMetaDir := db.opt.Dir + "/meta/bucket"
		if ok := helper.PathIsExist(bucketMetaDir); !ok {
			if err := os.MkdirAll(bucketMetaDir, os.ModePerm); err != nil {
				return nil, err
			}
		}
	}
	// 构建索引
	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	return db, nil
}

// buildIndexes 初始化db 根据 minidb 每种索引都应该是包含了 两步骤: 1.加载数据文件,2.加载索引
func (db *DB) buildIndexes() (err error) {
	//从文件中获取活动文件id,以及获取数据文件列表
	maxFileID, dataFileIds := db.getMaxFileIDAndFileIDs()
	//设置最大的文件id,这个就是ActiveFile,活动文件
	db.MaxFileID = maxFileID

	//初始化并设置活动文件
	//根据当前最大的文件Id 获取文件名,并创建数据文件 对象
	if db.ActiveFile, err = NewDataFile(db.opt.Dir, db.MaxFileID, db.opt.SegmentSize, db.opt.RWMode); err != nil {
		return
	}
	// 如果没有数据文件，那么就直接退出
	if dataFileIds == nil && maxFileID == 0 {
		return
	}
	//获取活动文件写偏移量,并且设置文件的物理大小
	if err = db.ActiveFile.setActiveFileWriteOff(); err != nil {
		return
	}
	//如果开启了 b+树稀疏索引模式  则 构建b+ 树稀疏索引
	if err = db.buildBucketMetaIdx(); err != nil {
		return
	}

	// 根据所有获取到的数据文件列表 构建 hint 索引
	return db.buildHintIdx(dataFileIds)
}

// getMaxFileIDAndFileIDs 查找文件夹下面最大的文件id和文件id列表
func (db *DB) getMaxFileIDAndFileIDs() (int64, []int) {
	var (
		dataFileIds []int
		maxFileID   int
	)
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}
	maxFileID = 0
	//循环每天一个
	for _, f := range files {
		name := f.Name()
		// 判断文件名扩展名是不是数据文件,获取,路径的最后一个元素,然后分割.
		ext := path.Ext(path.Base(name))
		if ext != DataSuffix {
			continue
		}
		//如果扩展就是数据,那么文件名中包含id
		id := strings.TrimSuffix(name, DataSuffix)
		idVal, _ := helper.StrToInt(id) //将id转为int
		if maxFileID <= idVal {
			maxFileID = idVal
		}
		dataFileIds = append(dataFileIds, idVal)
	}
	if len(dataFileIds) == 0 {
		return 0, nil
	}
	return int64(maxFileID), dataFileIds
}

// getMaxFileIDAndFileIDs2 查找文件夹下面最大的文件id和文件id列表，第二种实现方法
func (db *DB) getMaxFileIDAndFileIDs2() (int64, []int) {
	var (
		dataFileIds []int
		maxFileID   int64
	)
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}
	maxFileID = 0
	//循环每天一个
	for _, f := range files {
		name := f.Name()
		// 判断文件名扩展名是不是数据文件,获取,路径的最后一个元素,然后分割.
		ext := path.Ext(path.Base(name))
		if ext != DataSuffix {
			continue
		}
		//如果扩展就是数据,那么文件名中包含id
		id := strings.TrimSuffix(name, DataSuffix)
		idVal, _ := helper.StrToInt(id) //将id转为int
		dataFileIds = append(dataFileIds, idVal)
	}
	if len(dataFileIds) == 0 {
		return 0, nil
	}
	sort.Ints(dataFileIds)
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])

	return maxFileID, dataFileIds
}

// 创建 B+ 树稀疏索引模式
func (db *DB) buildBucketMetaIdx() error {
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		//获取bucketMeta files
		files, err := ioutil.ReadDir(db.getBucketMetaPath())
		if err != nil {
			return err
		}

		if len(files) != 0 {
			for _, f := range files {
				name := f.Name()
				fileSuffix := path.Ext(path.Base(name))
				if fileSuffix != BucketMetaSuffix {
					continue
				}

				name = strings.TrimSuffix(name, BucketMetaSuffix)

				bucketMeta, err := ReadBucketMetaFromPath(db.getBucketMetaFilePath(name))
				if err != nil {
					return err
				}

				db.bucketMetas[name] = bucketMeta
			}
		}
	}

	return nil
}

// bucketMetas 存储路径
func (db *DB) getBucketMetaPath() string {
	return db.getMetaPath() + "/bucket"
}

// 源数据存储路径
func (db *DB) getMetaPath() string {
	return db.opt.Dir + "/meta"
}

// 通过 Bucket 拼接 BucketMetaFilePath
func (db *DB) getBucketMetaFilePath(name string) string {
	return db.getBucketMetaPath() + "/" + name + BucketMetaSuffix
}

// 构建 hint 索引
func (db *DB) buildHintIdx(dataFileIds []int) (err error) {
	var (
		unconfirmedRecords []*Record
	)
	// 根据数据文件 构建  db.BPTreeKeyEntryPosMap,这将能够建立一个map,value 是我们的文件中 entry的 pos
	// 如果解析失败或者是收集的记录数目为0，那么就返回
	if unconfirmedRecords, db.committedTxIds, err = db.parseDataFiles(dataFileIds); err != nil || len(unconfirmedRecords) == 0 {
		return err
	}
	//循环每一个未确认的记录
	for _, record := range unconfirmedRecords {
		//如果记录的 entry 是提交了的 (entry.Meta.Status == Committed)
		//或者 其 hint 源数据中的事务id(record.H.Meta.TxID) == entry.Meta.TxID

		if _, ok := db.committedTxIds[record.H.Meta.TxID]; ok {
			//获取 bucket
			bucket := string(record.H.Meta.Bucket)

			// 如果 是 BPT数据结构
			if record.H.Meta.Ds == DataStructureBPTree {
				record.H.Meta.Status = Committed
				// TODO: 当数据为 BPTree 且 EntryIdxMode 为 HintBPTSparseIdxMode 时，需要使用BPTree 进行构建
				// 如果 是稀疏索引模式，那么 就构建
				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					if err = db.buildActiveBPTreeIdx(record); err != nil {
						return err
					}
				} else {
					if err = db.buildBPTreeIdx(bucket, record); err != nil {
						return err
					}
				}
			}
			// 如果 Ds 标识此处没有数据，那么就从bucket 中删除记录
			if record.H.Meta.Ds == DataStructureNone {
				db.buildNotDSIdxes(bucket, record)
			}
			db.KeyCount++
		}
	}
	if HintBPTSparseIdxMode == db.opt.EntryIdxMode {
		if err = db.buildBPTreeRootIdxes(dataFileIds); err != nil {
			return err
		}
	}

	return nil
}

const bptDir = "bpt"

func (db *DB) getBPTDir() string {
	return db.opt.Dir + "/" + bptDir
}
func (db *DB) getBPTRootPath(fID int64) string {
	return db.getBPTDir() + "/root/" + helper.Int64ToStr(fID) + BPTRootIndexSuffix
}
func (db *DB) buildBPTreeRootIdxes(dataFileIds []int) error {
	var off int64

	dataFileIdsSize := len(dataFileIds)

	if dataFileIdsSize == 1 {
		return nil
	}

	for i := 0; i < len(dataFileIds[0:dataFileIdsSize-1]); i++ {
		off = 0
		path := db.getBPTRootPath(int64(dataFileIds[i]))
		fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		for {
			bs, err := ReadBPTreeRootIdxAt(fd, off)
			if err == io.EOF || err == nil && bs == nil {
				break
			}
			if err != nil {
				return err
			}

			if err == nil && bs != nil {
				db.BPTreeRootIdxes = append(db.BPTreeRootIdxes, bs)
				off += bs.Size()
			}

		}

		fd.Close()
	}

	db.committedTxIds = nil

	return nil
}

// parseDataFiles 读取解析数据文件
func (db *DB) parseDataFiles(dataFileIds []int) (unconfirmedRecords []*Record, committedTxIds map[uint64]struct{}, err error) {
	var (
		dataUnconfirmedRecords []*Record
	)
	committedTxIds = make(map[uint64]struct{})
	// 如果是稀疏索引模式,则 只用解析最后一个数据文件,即 fileId 最大的那个文件
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		sort.Ints(dataFileIds)
		dataFileIds = dataFileIds[len(dataFileIds)-1:]
	}
	//循环每一个数据文件,然后解析通过数据文件解析数据，构建索引，收集记录的数据
	for _, dataID := range dataFileIds {
		// 获取DataFile 结构体
		// 设置Truncate 数据文件大小 为 SegmentSize
		// 设置 数据文件的 读写模型为 StartFileLoadingMode
		f, err := NewDataFile(db.opt.Dir, int64(dataID), db.opt.SegmentSize, db.opt.StartFileLoadingMode)
		if err != nil {
			return nil, nil, err
		}
		// 解析对应dataID的数据
		// EntryIdxMode entry 的索引模式设置
		// BPTreeKeyEntryPosMap bpt 偏移mapping
		// committedTxIds 事务提交mapping
		dataUnconfirmedRecords, err = f.ParseData(int64(dataID), 0, db.opt.EntryIdxMode, db.BPTreeKeyEntryPosMap, db.ActiveCommittedTxIdsIdx, committedTxIds, db.opt.SegmentSize)
		if err != nil {
			return nil, nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
		}
		unconfirmedRecords = append(unconfirmedRecords, dataUnconfirmedRecords...)
	}
	return
}

// 根据 Record信息，将该数据 构建到 db.ActiveBPTreeIdx 中
func (db *DB) buildActiveBPTreeIdx(r *Record) error {
	Key := r.H.Meta.Bucket
	Key = append(Key, r.H.Key...)
	if err := db.ActiveBPTreeIdx.Insert(Key, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

func (db *DB) buildBPTreeIdx(bucket string, r *Record) error {
	if _, ok := db.BPTreeIdx[bucket]; !ok {
		db.BPTreeIdx[bucket] = NewTree()
	}
	if err := db.BPTreeIdx[bucket].Insert(r.H.Key, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

// buildNotDSIdxes 当标识此处没有数据时
//会根据 Record.H.Meta.Flag 来对对应的数据索引进行相关删除
func (db *DB) buildNotDSIdxes(bucket string, r *Record) {
	if r.H.Meta.Flag == DataBPTreeBucketDeleteFlag {
		db.deleteBucket(DataStructureBPTree, bucket)
	}
	return
}

// deleteBucket 根据ds从对应的索引中删除bucket
func (db *DB) deleteBucket(ds uint16, bucket string) {
	if ds == DataStructureBPTree {
		delete(db.BPTreeIdx, bucket)
	}
	return
}

// managed calls a block of code that is fully contained in a transaction.
func (db *DB) managed(writable bool, fn func(tx *Tx) error) error {
	var tx *Tx

	tx, err := db.Begin(writable)
	if err != nil {
		return err
	}

	if err = fn(tx); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			return errRollback
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			return errRollback
		}
		return err
	}

	return nil
}

// Update executes a function within a managed read/write transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(false, fn)
}
