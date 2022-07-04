package rwmanager

import (
	"os"
)

type RWMode int

const (
	// FileIO RWMode 标准io
	FileIO RWMode = iota

	// MMap todo: implement
	MMap
)

// RWManager 定义RWManager接口
type RWManager interface {
	WriteAt(b []byte, off int64) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Sync() (err error)
	Close() (err error)
}

// Truncate Linux创造固定的文件大小-为文件预分配磁盘空间
func Truncate(path string, capacity int64, f *os.File) error {
	fileInfo, _ := os.Stat(path)
	if fileInfo.Size() < capacity {
		if err := f.Truncate(capacity); err != nil {
			return err
		}
	}
	return nil
}
