package bitcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New(" crc error")

	// 文件申请空间,空间大小错误
	ErrCapacity = errors.New("capacity error")

	// rwmode error
	ErrRWMode = errors.New("RWMode error")
)

const (
	// DataSuffix 数据文件扩展名
	DataSuffix = ".data"
)

// DataFile 数据文件对象
type DataFile struct {
	path       string    // 是文件地址
	fileID     int64     // 文件id,可以获得文件的名字
	writeOff   int64     // 写偏移
	ActualSize int64     // 未知
	rwManager  RWManager // 读写模式
}

// NewDataFile 创建一个数据文件对象
func NewDataFile(path string, capacity int64, rwMode RWMode) (df *DataFile, err error) {
	var rwManager RWManager

	if capacity <= 0 {
		return nil, ErrCapacity
	}

	if rwMode == FileIO {
		rwManager, err = NewFileIORWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrRWMode
	}

	return &DataFile{
		path:       path,
		writeOff:   0,
		ActualSize: 0,
		rwManager:  rwManager,
	}, nil
}

// ReadEntryAt 返回给offset处的数据,此处对应 minidb.db_dile.Read,读取数据项并构建索引
func (df *DataFile) ReadEntryAt(off int) (e *Entry, err error) {
	//读取数据 MetaData
	metaDataBuf := make([]byte, DataEntryHeaderSize)
	if _, err := df.rwManager.ReadAt(metaDataBuf, int64(off)); err != nil {
		return nil, err
	}
	// 通过header 解码为 metadata
	meta := MetaDataDecode(metaDataBuf)
	//根据metadata和crc 构建entry
	e = &Entry{
		crc:  binary.LittleEndian.Uint32(metaDataBuf[0:4]),
		Meta: meta,
	}
	// zero 校验
	if e.IsZero() {
		return nil, nil

	}
	//crc32 校验
	crc := e.GetCrc(metaDataBuf)
	if crc != e.crc {
		return nil, ErrCrc
	}

	//读取变长部分
	//1.读取 Bucket
	off += DataEntryHeaderSize //跳过已经读取的 metadata
	bucketBuf := make([]byte, meta.BucketSize)
	_, err = df.rwManager.ReadAt(bucketBuf, int64(off))
	if err != nil {
		return nil, err
	}
	e.Meta.Bucket = bucketBuf

	// 2.读取key
	off += int(meta.BucketSize) //跳过已经读取的 Bucket
	keyBuf := make([]byte, meta.KeySize)

	_, err = df.rwManager.ReadAt(keyBuf, int64(off))
	if err != nil {
		return nil, err
	}
	e.Key = keyBuf

	// 2.读取 value
	off += int(meta.KeySize) //跳过已经读取的 key
	valBuf := make([]byte, meta.ValueSize)
	_, err = df.rwManager.ReadAt(valBuf, int64(off))
	if err != nil {
		return nil, err
	}
	e.Value = valBuf

	return
}

// ParseData 解析数据 unconfirmedRecords 未经过证实的数据,committedTxIds 已经提交的数据
func (df *DataFile) ParseData(fID int64, off int64, recordEntryIdxMode EntryIdxMode, KeyEntryPosMap map[string]int64, committedTxIds map[uint64]struct{}) (unconfirmedRecords []*Record, err error) {
	var (
		recordEntry *Entry
	)
	defer df.rwManager.Close()
	for {
		if entry, err := df.ReadEntryAt(int(off)); err == nil {
			// 读取结束,结束该文件解析
			if entry == nil {
				break
			}

			// 根据 recordEntryIdxMode 设置Record,并且添加到未确认数据列表中
			recordEntry = nil
			if recordEntryIdxMode == HintKeyValAndRAMIdxMode {
				recordEntry = &Entry{
					Key:   entry.Key,
					Value: entry.Value,
					Meta:  entry.Meta,
				}
				KeyEntryPosMap[string(entry.Meta.Bucket)+string(entry.Key)] = off
			}
			unconfirmedRecords = append(unconfirmedRecords, &Record{
				H: &Hint{
					Key:     entry.Key,
					FileID:  fID,
					Meta:    entry.Meta,
					DataPos: uint64(off),
				},
				E: recordEntry,
			})

			// 关于提交状态的代码 TODO
			if entry.Meta.Status == Committed {
				committedTxIds[entry.Meta.TxID] = struct{}{}
				// todo: BPTree Insert
				//db.ActiveCommittedTxIdsIdx.Insert([]byte(strconv2.Int64ToStr(int64(entry.Meta.TxID))), nil,
				//	&Hint{Meta: &MetaData{Flag: DataSetFlag}}, CountFlagEnabled)
			}
			off += entry.Size()
		} else {
			//读取出错,若文件读完了,则结束
			if err == io.EOF {
				break
			}
			// 理论上这个判断不会生效才对吧
			//if off >= db.opt.SegmentSize {
			//	break
			//}
			return nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
		}
	}
	return
}
