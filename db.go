package bitcask

import (
	"bitcask/helper"
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
	// DataStructureSet represents the data structure set flag
	DataStructureSet uint16 = iota

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet

	// DataStructureBPTree represents the data structure b+ tree flag
	DataStructureBPTree

	// DataStructureList represents the data structure list flag
	DataStructureList

	// DataStructureNone represents not the data structure
	DataStructureNone
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
		opt                  Options          // the database options
		BPTreeIdx            BPTreeIdx        // Hint Index
		BPTreeKeyEntryPosMap map[string]int64 // key = bucket+key  val = EntryPos
		committedTxIds       map[uint64]struct{}
		MaxFileID            int64     //活动文件id
		ActiveFile           *DataFile //活动文件对象
		mu                   sync.RWMutex
		KeyCount             int // total key number ,include expired, deleted, repeated.
		closed               bool
		isMerging            bool
		bucketMetas          BucketMetasIdx // BucketMeta 索引
	}

	// BPTreeIdx B+ tree 索引
	BPTreeIdx map[string]*BPTree

	// BucketMetasIdx 存储桶元信息的索引
	BucketMetasIdx map[string]*BucketMeta
)

func Open(opt Options) (*DB, error) {
	db := &DB{
		opt:                  opt,
		BPTreeKeyEntryPosMap: make(map[string]int64),
		committedTxIds:       make(map[uint64]struct{}),
		MaxFileID:            0,
		KeyCount:             0,
		closed:               false,
		isMerging:            false,
	}
	//判断文件夹在不在,不在就创建
	if ok := helper.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	return db, nil
}

// 初始化db 根据 minidb 每种索引都应该是包含了 两步骤: 1.加载数据文件,2.加载索引
func (db *DB) buildIndexes() (err error) {
	//从文件中获取活动文件id,以及获取数据文件列表
	maxFileID, dataFileIds := db.getMaxFileIDAndFileIDs()
	db.MaxFileID = maxFileID //设置最大的文件id,这个应该就是ActiveFile
	//初始化并设置活动文件
	if err = db.setActiveFile(); err != nil {
		return
	}

	//获取活动文件写偏移量,并且设置文件的物理大小
	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}
	//如果开启了 b+树稀疏索引模式  则 构建b+ 树稀疏索引
	if err = db.buildBucketMetaIdx(); err != nil {
		return
	}

	// 根据所有获取到的数据文件列表 构建 hint 索引
	return db.buildHintIdx(dataFileIds)
}

// 查找文件夹下面最大的文件id和文件id列表
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

// 设置活动文件(数据文件对象)
func (db *DB) setActiveFile() (err error) {
	//获取文件名
	filepath := db.getDataPath(db.MaxFileID)
	//创建活动文件
	db.ActiveFile, err = NewDataFile(filepath, db.opt.SegmentSize, db.opt.RWMode)
	if err != nil {
		return
	}
	db.ActiveFile.fileID = db.MaxFileID

	return nil

}

// 根据文件id 拼接得到数据文件路径
func (db *DB) getDataPath(fID int64) string {
	return db.opt.Dir + "/" + helper.Int64ToStr(fID) + DataSuffix
}

// 对应 minidb.db.loadIndexesFromFile 1.从文件中加载索引 2.获取 活动文件的实际文件大小和写入偏移量,
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off = 0
	for {
		if entry, err := db.ActiveFile.ReadEntryAt(int(off)); err == nil {
			if entry == nil {
				break
			}

			off += entry.Size()
			//todo: minidb 在这里建立了索引, nustdbDB 需要遍历每一个datafile ,然后读取解析entry,设置到BPTreeKeyEntryPosMap中

			//set ActiveFileActualSize
			db.ActiveFile.ActualSize = off

		} else {
			if err == io.EOF {
				break
			}

			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
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
	unconfirmedRecords, db.committedTxIds, err = db.parseDataFiles(dataFileIds)
	if err != nil {
		return err
	}

	if len(unconfirmedRecords) == 0 {
		return nil
	}

	//循环每一个未确认的记录
	for _, record := range unconfirmedRecords {
		//如果记录的 entry 是提交了的 (entry.Meta.Status == Committed) 或者 其 hint 源数据中的事务id(record.H.Meta.TxID) == entry.Meta.TxID
		if _, ok := db.committedTxIds[record.H.Meta.TxID]; ok {
			//获取 bucket
			bucket := string(record.H.Meta.Bucket)

			//会将一部分记录中没有可能没有提交的数据进行修正
			if record.H.Meta.Ds == DataStructureBPTree {
				record.H.Meta.Status = Committed

				// TODO: 当数据为 BPTree 且 EntryIdxMode 为 HintBPTSparseIdxMode 时，需要使用BPTree 进行构建
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
			// TODO: 构建 Set，SortedSet,List
			if err = db.buildOtherIdxes(bucket, record); err != nil {
				return err
			}
			// TODO：根据bucket 从对应的数据结构索引中删除数据
			if record.H.Meta.Ds == DataStructureNone {
				db.buildNotDSIdxes(bucket, record)
			}

			db.KeyCount++
		}
	}
	//TODO： buildBPTreeRootIdxes
	//if HintBPTSparseIdxMode == db.opt.EntryIdxMode {
	//	if err = db.buildBPTreeRootIdxes(dataFileIds); err != nil {
	//		return err
	//	}
	//}

	return nil
}

func (db *DB) parseDataFiles(dataFileIds []int) (unconfirmedRecords []*Record, committedTxIds map[uint64]struct{}, err error) {
	var (
		fUnconfirmedRecords []*Record
	)
	committedTxIds = make(map[uint64]struct{})

	// 如果是稀疏索引模式,则 只用解析最后一个数据文件
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		sort.Ints(dataFileIds)
		dataFileIds = dataFileIds[len(dataFileIds)-1:]
	}
	//循环每一个数据文件,
	for _, dataID := range dataFileIds {
		fID := int64(dataID)
		f, err := NewDataFile(db.getDataPath(fID), db.opt.SegmentSize, db.opt.StartFileLoadingMode)
		if err != nil {
			return nil, nil, err
		}
		fUnconfirmedRecords, err = f.ParseData(fID, 0, db.opt.EntryIdxMode, db.BPTreeKeyEntryPosMap, committedTxIds)
		if err != nil {
			return nil, nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
		}
		unconfirmedRecords = append(unconfirmedRecords, fUnconfirmedRecords...)
	}
	return
}

// 根据 Record信息，将该数据 构建到 db.ActiveBPTreeIdx 中
func (db *DB) buildActiveBPTreeIdx(r *Record) error {
	Key := r.H.Meta.Bucket
	Key = append(Key, r.H.Key...)
	// BPTree Insert TODO
	//if err := db.ActiveBPTreeIdx.Insert(Key, r.E, r.H, CountFlagEnabled); err != nil {
	//	return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	//}

	return nil
}

func (db *DB) buildBPTreeIdx(bucket string, r *Record) error {
	if _, ok := db.BPTreeIdx[bucket]; !ok {
		db.BPTreeIdx[bucket] = NewTree()
	}

	// BPTree Insert TODO
	//if err := db.BPTreeIdx[bucket].Insert(r.H.Key, r.E, r.H, CountFlagEnabled); err != nil {
	//	return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	//}

	return nil
}

// 这里会构建 Set，SortedSet,List
func (db *DB) buildOtherIdxes(bucket string, r *Record) error {
	//TODO：  Set，SortedSet,List
	//if r.H.Meta.Ds == DataStructureSet {
	//	if err := db.buildSetIdx(bucket, r); err != nil {
	//		return err
	//	}
	//}
	//if r.H.Meta.Ds == DataStructureSortedSet {
	//	if err := db.buildSortedSetIdx(bucket, r); err != nil {
	//		return err
	//	}
	//}
	//if r.H.Meta.Ds == DataStructureList {
	//	if err := db.buildListIdx(bucket, r); err != nil {
	//		return err
	//	}
	//}

	return nil
}

// 当标识此处没有数据时，会根据 Record.H.Meta.Flag 来对对应的数据进行删除
func (db *DB) buildNotDSIdxes(bucket string, r *Record) {
	if r.H.Meta.Flag == DataSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSet, bucket)
	}
	if r.H.Meta.Flag == DataSortedSetBucketDeleteFlag {
		db.deleteBucket(DataStructureSortedSet, bucket)
	}
	if r.H.Meta.Flag == DataBPTreeBucketDeleteFlag {
		db.deleteBucket(DataStructureBPTree, bucket)
	}
	if r.H.Meta.Flag == DataListBucketDeleteFlag {
		db.deleteBucket(DataStructureList, bucket)
	}
	return
}

// 根据
func (db *DB) deleteBucket(ds uint16, bucket string) {
	//if ds == DataStructureSet {
	//	delete(db.SetIdx, bucket)
	//}
	//if ds == DataStructureSortedSet {
	//	delete(db.SortedSetIdx, bucket)
	//}
	//if ds == DataStructureBPTree {
	//	delete(db.BPTreeIdx, bucket)
	//}
	//if ds == DataStructureList {
	//	delete(db.ListIdx, bucket)
	//}
	return
}
