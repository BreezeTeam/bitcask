package bitcask

import (
	"os"
	"path/filepath"
)

type RWMode int

const (
	// 标准io
	FileIO RWMode = iota

	// mmap todo: implement
	MMap
)

// 定义RWManager接口
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

// FileIORWManager

// 标准io RWManager
type FileIORWManager struct {
	fd *os.File //FileIORWManager.fb 对应 minidb 的 DBFile.File
}

//返回一个新初始化的FileIORWManager。 此处对应 minidb 的 db_file.newInternal
func NewFileIORWManager(path string, capacity int64) (*FileIORWManager, error) {
	fd, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// [Seek vs Truncate to prealocate file size](https://stackoverflow.com/questions/66611770/seek-vs-truncate-to-grow-prealocate-file-size)
	// 应用于持续写场景,目标是为了减小磁盘碎片化,提高写卡速度.这个也是 顺序写的 一个思想体现
	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}
	return &FileIORWManager{fd: fd}, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// `WriteAt` is a wrapper of the *File.WriteAt.
func (fm *FileIORWManager) WriteAt(b []byte, off int64) (n int, err error) {
	return fm.fd.WriteAt(b, off)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
// `ReadAt` is a wrapper of the *File.ReadAt.
func (fm *FileIORWManager) ReadAt(b []byte, off int64) (n int, err error) {
	return fm.fd.ReadAt(b, off)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
// `Sync` is a wrapper of the *File.Sync.
func (fm *FileIORWManager) Sync() (err error) {
	return fm.fd.Sync()
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// `Close` is a wrapper of the *File.Close.
func (fm *FileIORWManager) Close() (err error) {
	return fm.fd.Close()
}
