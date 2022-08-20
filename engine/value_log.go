package bitcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	valueLogFileName      = "values.vlog"
	valueLogSegmentPrefix = "values-"
	valueLogSegmentSuffix = ".vlog"
	valuePointerMagic     = uint32(0x564c4f47)
	valuePointerVersion   = uint16(1)
	valuePointerSizeBytes = 30
)

var ErrValuePointer = errors.New("invalid value pointer")

type valuePointer struct {
	FileID uint64
	Offset int64
	Size   uint32
	CRC    uint32
}

type valueLog struct {
	dir         string
	segmentSize int64
	activeID    uint64
	files       map[uint64]*os.File
	sizes       map[uint64]int64
	dirty       map[uint64]struct{}
}

func openValueLog(dir string, segmentSize ...int64) (*valueLog, error) {
	var limit int64
	if len(segmentSize) > 0 {
		limit = segmentSize[0]
	}
	vl := &valueLog{
		dir:         dir,
		segmentSize: limit,
		files:       make(map[uint64]*os.File),
		sizes:       make(map[uint64]int64),
		dirty:       make(map[uint64]struct{}),
	}
	ids, err := discoverValueLogSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		ids = []uint64{0}
	}
	for _, id := range ids {
		file, err := os.OpenFile(vl.path(id), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			vl.Close()
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			vl.Close()
			return nil, err
		}
		vl.files[id] = file
		vl.sizes[id] = stat.Size()
		if id > vl.activeID {
			vl.activeID = id
		}
	}
	return vl, nil
}

func discoverValueLogSegments(dir string) ([]uint64, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []uint64
	for _, file := range files {
		name := file.Name()
		if name == valueLogFileName {
			ids = append(ids, 0)
			continue
		}
		if !strings.HasPrefix(name, valueLogSegmentPrefix) || !strings.HasSuffix(name, valueLogSegmentSuffix) {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(name, valueLogSegmentPrefix), valueLogSegmentSuffix)
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || id == 0 {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (vl *valueLog) path(id uint64) string {
	if id == 0 {
		return filepath.Join(vl.dir, valueLogFileName)
	}
	return filepath.Join(vl.dir, fmt.Sprintf("%s%020d%s", valueLogSegmentPrefix, id, valueLogSegmentSuffix))
}

func (vl *valueLog) Append(value []byte) (valuePointer, error) {
	if vl.segmentSize > 0 && vl.sizes[vl.activeID] > 0 && vl.sizes[vl.activeID]+int64(len(value)) > vl.segmentSize {
		if err := vl.rotate(); err != nil {
			return valuePointer{}, err
		}
	}
	file := vl.files[vl.activeID]
	off := vl.sizes[vl.activeID]
	n, err := file.WriteAt(value, off)
	if err != nil {
		return valuePointer{}, err
	}
	if n != len(value) {
		return valuePointer{}, errors.New("short value-log write")
	}
	vl.sizes[vl.activeID] += int64(n)
	vl.dirty[vl.activeID] = struct{}{}
	return valuePointer{FileID: vl.activeID, Offset: off, Size: uint32(len(value)), CRC: crc32.ChecksumIEEE(value)}, nil
}

func (vl *valueLog) rotate() error {
	vl.activeID++
	file, err := os.OpenFile(vl.path(vl.activeID), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	vl.files[vl.activeID] = file
	vl.sizes[vl.activeID] = 0
	return nil
}

func (vl *valueLog) Read(ptr valuePointer) ([]byte, error) {
	file := vl.files[ptr.FileID]
	if file == nil || ptr.Offset < 0 || ptr.Size > uint32(^uint32(0)>>1) {
		return nil, ErrValuePointer
	}
	end := ptr.Offset + int64(ptr.Size)
	if end < ptr.Offset || end > vl.sizes[ptr.FileID] {
		return nil, ErrValuePointer
	}
	value := make([]byte, ptr.Size)
	if _, err := file.ReadAt(value, ptr.Offset); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(value) != ptr.CRC {
		return nil, ErrCrc
	}
	return value, nil
}

func (vl *valueLog) Sync() error {
	for id := uint64(0); id <= vl.activeID; id++ {
		if _, ok := vl.dirty[id]; !ok {
			continue
		}
		if file := vl.files[id]; file != nil {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	vl.dirty = make(map[uint64]struct{})
	return nil
}

func (vl *valueLog) Close() error {
	var firstErr error
	for id, file := range vl.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(vl.files, id)
	}
	return firstErr
}

func encodeValuePointer(ptr valuePointer) []byte {
	buf := make([]byte, valuePointerSizeBytes)
	binary.LittleEndian.PutUint32(buf[0:4], valuePointerMagic)
	binary.LittleEndian.PutUint16(buf[4:6], valuePointerVersion)
	binary.LittleEndian.PutUint64(buf[6:14], ptr.FileID)
	binary.LittleEndian.PutUint64(buf[14:22], uint64(ptr.Offset))
	binary.LittleEndian.PutUint32(buf[22:26], ptr.Size)
	binary.LittleEndian.PutUint32(buf[26:30], ptr.CRC)
	return buf
}

func decodeValuePointer(buf []byte) (valuePointer, error) {
	if len(buf) != valuePointerSizeBytes {
		return valuePointer{}, ErrValuePointer
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != valuePointerMagic || binary.LittleEndian.Uint16(buf[4:6]) != valuePointerVersion {
		return valuePointer{}, ErrValuePointer
	}
	return valuePointer{
		FileID: binary.LittleEndian.Uint64(buf[6:14]),
		Offset: int64(binary.LittleEndian.Uint64(buf[14:22])),
		Size:   binary.LittleEndian.Uint32(buf[22:26]),
		CRC:    binary.LittleEndian.Uint32(buf[26:30]),
	}, nil
}
