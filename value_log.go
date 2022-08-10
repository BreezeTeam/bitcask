package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
)

const (
	valueLogFileName      = "values.vlog"
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
	file *os.File
}

func openValueLog(dir string) (*valueLog, error) {
	file, err := os.OpenFile(dir+"/"+valueLogFileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &valueLog{file: file}, nil
}

func (vl *valueLog) Append(value []byte) (valuePointer, error) {
	off, err := vl.file.Seek(0, os.SEEK_END)
	if err != nil {
		return valuePointer{}, err
	}
	if _, err := vl.file.Write(value); err != nil {
		return valuePointer{}, err
	}
	return valuePointer{Offset: off, Size: uint32(len(value)), CRC: crc32.ChecksumIEEE(value)}, nil
}

func (vl *valueLog) Read(ptr valuePointer) ([]byte, error) {
	value := make([]byte, ptr.Size)
	if _, err := vl.file.ReadAt(value, ptr.Offset); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(value) != ptr.CRC {
		return nil, ErrCrc
	}
	return value, nil
}

func (vl *valueLog) Sync() error {
	return vl.file.Sync()
}

func (vl *valueLog) Close() error {
	return vl.file.Close()
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
