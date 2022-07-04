package bitcask

import (
	"bitcask/helper"
	"bitcask/rwmanager"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New("crc error")

	//ErrCapacity 文件申请空间,空间大小错误
	ErrCapacity = errors.New("capacity error")

	//ErrRWMode RWMode error，读写模式错误
	ErrRWMode = errors.New("RWMode error")
)

const (
	// DataSuffix 数据文件扩展名
	DataSuffix = ".data"
)

// DataFile 数据文件对象
type DataFile struct {
	path       string              // 是文件地址
	fileID     int64               // 文件id,可以获得文件的名字
	writeOff   int64               // 写偏移
	ActualSize int64               // 文件实际大小
	rwManager  rwmanager.RWManager // 读写模式
}

// NewDataFile 创建一个数据文件对象
// path 数据文件路径
// capacity 数据文件 容量大小，一般是段大小
// rwMode io 模型，读取和写入可以设置不同的索引模型
func NewDataFile(dir string, fileID int64, capacity int64, rwMode rwmanager.RWMode) (df *DataFile, err error) {
	var rwManager rwmanager.RWManager
	// 获取数据文件路径
	path := getDataFilePath(dir, fileID)
	// 如果 容量小于等于0，那么就抛出异常
	if capacity <= 0 {
		return nil, ErrCapacity
	}

	// 如果 读写模式是文件io，那么 设置rwManager 为
	if rwMode == rwmanager.FileIO {
		rwManager, err = rwmanager.NewFileIORWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrRWMode
	}

	return &DataFile{
		path:       path,
		fileID:     fileID,
		writeOff:   0,
		ActualSize: 0,
		rwManager:  rwManager,
	}, nil
}

// getDataFilePath 根据文件id 拼接得到数据文件路径
func getDataFilePath(dir string, fileID int64) string {
	return dir + "/" + helper.Int64ToStr(fileID) + DataSuffix
}

// ReadEntryAt 返回给offset处的数据
// 读取数据项并构建 Entry
func (df *DataFile) ReadEntryAt(off int) (e *Entry, err error) {
	//读取数据 MetaData 的header
	//metaDataBuf 的前四个字节为crc
	//剩余的部分是 MetaData
	metaDataBuf := make([]byte, DataEntryHeaderSize)
	if _, err := df.rwManager.ReadAt(metaDataBuf, int64(off)); err != nil {
		return nil, err
	}
	// 通过header 解码为 metadata,获取定长部分数据结构了
	meta := MetaDataDecode(metaDataBuf)
	//根据metadata和crc 构建entry
	e = &Entry{
		crc:  binary.LittleEndian.Uint32(metaDataBuf[0:4]),
		Meta: meta,
	}
	// entry的 zero 校验
	if e.IsZero() {
		return nil, nil

	}
	//如果 entry不是0，那么对 header 进行 crc32 校验
	crc := e.GetCrc(metaDataBuf)
	if crc != e.crc {
		return nil, ErrCrc
	}
	// 如果校验通过，那么 继续读取解析变长部分

	// 继续读取 bucket
	// 1. 跳过已经读取的 metadata
	off += DataEntryHeaderSize
	// 2. 通过 定长部分存储的 BucketSize 读取 Bucket
	bucketBuf := make([]byte, meta.BucketSize)
	if _, err = df.rwManager.ReadAt(bucketBuf, int64(off)); err != nil {
		return nil, err
	}
	e.Meta.Bucket = bucketBuf

	// 继续读取 key
	// 1. 跳过已经读取的 BucketSize
	off += int(meta.BucketSize)
	// 2. 通过 定长部分存储的 KeySize 读取 key
	keyBuf := make([]byte, meta.KeySize)
	if _, err = df.rwManager.ReadAt(keyBuf, int64(off)); err != nil {
		return nil, err
	}
	e.Key = keyBuf

	// 继续读取 val
	// 1. 跳过已经读取的 KeySize
	off += int(meta.KeySize)
	// 2. 通过 定长部分存储的 ValueSize 读取 val
	valBuf := make([]byte, meta.ValueSize)
	if _, err = df.rwManager.ReadAt(valBuf, int64(off)); err != nil {
		return nil, err
	}
	e.Value = valBuf

	return
}

// ParseData 解析数据
// fID 数据文件id
// off 数据文件偏移量
// unconfirmedRecords 未经过证实的数据,
// committedTxIds 已经提交的数据
func (df *DataFile) ParseData(fID int64, off int64,
	entryIdxMode EntryIdxMode,
	entryKeyPosMap map[string]int64,
	committedTxIds map[uint64]struct{}, segmentSize int64) (unconfirmedRecords []*Record, err error) {
	var (
		recordEntry *Entry
	)
	// 读取结束自动退出
	defer func(rwManager rwmanager.RWManager) {
		err = rwManager.Close()
	}(df.rwManager)

	for {
		if entry, err := df.ReadEntryAt(int(off)); err == nil {
			// 读取结束,如果entry 为nil，那么就是读取entry 失败，那么就退出读取
			if entry == nil {
				break
			}
			recordEntry = nil

			// 根据 recordEntryIdxMode 设置Record，如果是 KeyVal 模型
			// 那么 recordEntry 就拷贝 entry 到 recordEntry
			if entryIdxMode == HintKeyValAndRAMIdxMode {
				recordEntry = &Entry{
					Key:   entry.Key,
					Value: entry.Value,
					Meta:  entry.Meta,
				}
			}
			// 如果 recordEntryIdxMode 为 HintBPTSparseIdxMode
			// 那么就保存 ${Bucket}${Key}:off 到 keyEntryPosMap
			if entryIdxMode == HintBPTSparseIdxMode {
				entryKeyPosMap[string(entry.Meta.Bucket)+string(entry.Key)] = off
			}
			// 如果该entry的状态是已提交，那么将该提交的事务id保存
			if entry.Meta.Status == Committed {
				committedTxIds[entry.Meta.TxID] = struct{}{}
				// todo: BPTree Insert
				//db.ActiveCommittedTxIdsIdx.Insert([]byte(strconv2.Int64ToStr(int64(entry.Meta.TxID))), nil,
				//	&Hint{Meta: &MetaData{Flag: DataSetFlag}}, CountFlagEnabled)
			}
			// 添加到未经证实的记录列表中
			// 如果 不是KeyVal 模式，那么recordEntry 将为nil
			unconfirmedRecords = append(unconfirmedRecords,
				&Record{
					H: &Hint{
						Key:     entry.Key,
						FileID:  fID,
						Meta:    entry.Meta,
						DataPos: uint64(off),
					},
					E: recordEntry,
				})

			off += entry.Size()
		} else {
			//读取出错,若文件读完了,则结束
			if err == io.EOF {
				break
			}
			//如果偏移量大于段大小，那么就不读取了
			if off >= segmentSize {
				break
			}
			//如果是别的错误，那么关系比文件读写
			return nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
		}
	}
	return
}

// getActiveFileWriteOff
//对应 minidb.db.loadIndexesFromFile
//1.从文件中加载索引
//2.获取 活动文件的实际文件大小和写入偏移量,
//minidb 在这里建立了索引(因为 minidb 是单文件简化 bitcask模型)
//nustdb 只是读取了所有的entry，并更新了ActualSize  活动文件的实际大小
func (df *DataFile) setActiveFileWriteOff() (err error) {
	off := int64(0)
	for {
		if entry, err := df.ReadEntryAt(int(off)); err == nil {
			if entry == nil {
				break
			}
			off += entry.Size()
			//更新 活动文件的实际大小
			df.ActualSize = off
		} else {
			// 如果err 为EOF，那么顺利读取完毕
			if err == io.EOF {
				break
			}
			df.writeOff = -1
			return fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}
	df.writeOff = off
	return
}
