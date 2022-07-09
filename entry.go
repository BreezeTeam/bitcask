package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1

	// Persistent represents the data persistent flag
	Persistent uint32 = 0

	// ScanNoLimit represents the data scan no limit flag
	ScanNoLimit int = -1
)

var (
	// DataEntryHeaderSize returns the entry header size
	// 数据头的定长部分
	DataEntryHeaderSize uint32 = 42

	// BucketMetaHeaderSize BucketMeta header size
	BucketMetaHeaderSize uint32 = 12

	// BucketMetaSuffix B +树索引后缀。
	BucketMetaSuffix = ".meta"
)

type (
	// Entry 数据item
	Entry struct {
		Key      []byte
		Value    []byte
		Meta     *MetaData
		crc      uint32
		position uint64
	}

	// Hint represents the index of the key
	Hint struct {
		Key     []byte
		FileID  int64
		Meta    *MetaData
		DataPos uint64
	}

	// MetaData represents the meta information of the data item.
	//表示数据项的元信息。
	MetaData struct {
		KeySize    uint32
		ValueSize  uint32
		Timestamp  uint64
		TTL        uint32
		Flag       uint16 // delete / set
		Bucket     []byte
		BucketSize uint32
		TxID       uint64 //committedTxId
		Status     uint16 // committed / uncommitted
		Ds         uint16 // data structure
	}

	// Record 记录 hint 和 entry
	Record struct {
		H *Hint
		E *Entry
	}
	// BucketMeta represents the bucket's meta-information.
	BucketMeta struct {
		startSize uint32
		endSize   uint32
		start     []byte
		end       []byte
		crc       uint32
	}
)

// Encode returns the slice after the BucketMeta be encoded.
func (bm *BucketMeta) Encode() []byte {
	buf := make([]byte, bm.Size())

	binary.LittleEndian.PutUint32(buf[4:8], bm.startSize)
	binary.LittleEndian.PutUint32(buf[8:12], bm.endSize)

	startBuf := buf[BucketMetaHeaderSize:(BucketMetaHeaderSize + bm.startSize)]
	copy(startBuf, bm.start)
	endBuf := buf[BucketMetaHeaderSize+bm.startSize : (BucketMetaHeaderSize + bm.startSize + bm.endSize)]
	copy(endBuf, bm.end)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// GetCrc returns the crc at given buf slice.
func (bm *BucketMeta) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, bm.start)
	crc = crc32.Update(crc, crc32.IEEETable, bm.end)

	return crc
}

// Size returns the size of the BucketMeta.
func (bm *BucketMeta) Size() int64 {
	return int64(BucketMetaHeaderSize + bm.startSize + bm.endSize)
}

/*Entry*/

//IsZero 检查 entry 是否为0
//这里是否能有一个字段判断即可
func (e *Entry) IsZero() bool {
	if e.crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
		return true
	}
	return false
}

//GetCrc returns the crc at given buf slice.
//返回给定buf片的CRC。
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Meta.Bucket)
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)

	return crc
}

//Size entry 的数据大小
func (e *Entry) Size() int64 {
	return int64(uint32(DataEntryHeaderSize) + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.BucketSize)
}

// Encode returns the slice after the entry be encoded.
//
//  the entry stored format:
//  |----------------------------------------------------------------------------------------------------------------|
//  |  crc  | timestamp | ksz | valueSize | flag  | TTL  |bucketSize| status | ds   | txId |  bucket |  key  | value |
//  |----------------------------------------------------------------------------------------------------------------|
//  | uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint32 | uint16 | uint16 |uint64 |[]byte|[]byte | []byte |
//  |----------------------------------------------------------------------------------------------------------------|
//
func (e *Entry) Encode() []byte {
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize
	bucketSize := e.Meta.BucketSize

	//set DataItemHeader buf
	buf := make([]byte, e.Size())
	buf = e.setEntryHeaderBuf(buf)
	//set bucket\key\value
	copy(buf[DataEntryHeaderSize:(bucketSize+DataEntryHeaderSize)], e.Meta.Bucket)
	copy(buf[(DataEntryHeaderSize+bucketSize):(DataEntryHeaderSize+bucketSize+keySize)], e.Key)
	copy(buf[(DataEntryHeaderSize+bucketSize+keySize):(DataEntryHeaderSize+bucketSize+keySize+valueSize)], e.Value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
func (e *Entry) setEntryHeaderBuf(buf []byte) []byte {
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.Timestamp)
	binary.LittleEndian.PutUint32(buf[12:16], e.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Meta.ValueSize)
	binary.LittleEndian.PutUint16(buf[20:22], e.Meta.Flag)
	binary.LittleEndian.PutUint32(buf[22:26], e.Meta.TTL)
	binary.LittleEndian.PutUint32(buf[26:30], e.Meta.BucketSize)
	binary.LittleEndian.PutUint16(buf[30:32], e.Meta.Status)
	binary.LittleEndian.PutUint16(buf[32:34], e.Meta.Ds)
	binary.LittleEndian.PutUint64(buf[34:42], e.Meta.TxID)

	return buf
}

// MetaDataDecode 根据buffer 构建 metadata 元数据结构
// 定长部分会进行crc校验
// crc, timestamp,key_size,value_size,flag,ttl,bucket_size,status,data_structure,tx_id
func MetaDataDecode(buf []byte) *MetaData {
	return &MetaData{
		Timestamp:  binary.LittleEndian.Uint64(buf[4:12]),
		KeySize:    binary.LittleEndian.Uint32(buf[12:16]),
		ValueSize:  binary.LittleEndian.Uint32(buf[16:20]),
		Flag:       binary.LittleEndian.Uint16(buf[20:22]),
		TTL:        binary.LittleEndian.Uint32(buf[22:26]),
		BucketSize: binary.LittleEndian.Uint32(buf[26:30]),
		Status:     binary.LittleEndian.Uint16(buf[30:32]),
		Ds:         binary.LittleEndian.Uint16(buf[32:34]),
		TxID:       binary.LittleEndian.Uint64(buf[34:42]),
	}
}

/*BucketMeta*/

// ReadBucketMetaFromPath bucketMeta 解码
func ReadBucketMetaFromPath(path string) (bucketMeta *BucketMeta, err error) {

	//open File
	fd, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer fd.Close()

	var off int64
	//读取数据 BucketMeta
	headerBuff := make([]byte, BucketMetaHeaderSize)
	_, err = fd.ReadAt(headerBuff, off)
	if err != nil {
		return
	}
	// 将header 解码为 crc ,startSize,endSize
	startSize := binary.LittleEndian.Uint32(headerBuff[4:8])
	endSize := binary.LittleEndian.Uint32(headerBuff[8:12])
	bucketMeta = &BucketMeta{
		startSize: startSize,
		endSize:   endSize,
		crc:       binary.LittleEndian.Uint32(headerBuff[0:4]),
	}

	//读取变长部分
	//1.读取 start
	off += int64(BucketMetaHeaderSize) // 跳过已经读取的 header 部分
	startBuf := make([]byte, startSize)
	if _, err = fd.ReadAt(startBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.start = startBuf

	// 2.读取 end
	off += int64(startSize) // 跳过已经读取的 start 部分
	endBuf := make([]byte, endSize)
	if _, err = fd.ReadAt(endBuf, off); err != nil {
		return nil, err
	}
	bucketMeta.end = endBuf
	//crc32 校验
	if bucketMeta.GetCrc(headerBuff) != bucketMeta.crc {
		return nil, ErrCrc
	}
	return
}

// UpdateRecord updates the record.
func (r *Record) UpdateRecord(h *Hint, e *Entry) error {
	r.E = e
	r.H = h

	return nil
}

// IsExpired returns the record if expired or not.
func (r *Record) IsExpired() bool {
	return IsExpired(r.H.Meta.TTL, r.H.Meta.Timestamp)
}
