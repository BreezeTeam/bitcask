package rwmanager

import (
	"os"
	"path/filepath"
)

// FileIORWManager 标准io RWManager
type FileIORWManager struct {
	fd *os.File //FileIORWManager.fb 对应 minidb 的 DBFile.File
}

// NewFileIORWManager 返回一个新初始化的FileIORWManager。 此处对应 minidb 的 db_file.newInternal
// [Seek vs Truncate to prealocate file size](https://stackoverflow.com/questions/66611770/seek-vs-truncate-to-grow-prealocate-file-size)
// 应用于持续写场景,目标是为了减小磁盘碎片化,提高写卡速度.这个也是 顺序写的 一个思想体现
func NewFileIORWManager(path string, capacity int64) (*FileIORWManager, error) {
	fd, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	err = Truncate(path, capacity, fd)
	if err != nil {
		return nil, err
	}
	return &FileIORWManager{fd: fd}, nil
}

// WriteAt 从字节偏移量off开始写入len(b)字节到文件。 *File.WriteAt
func (fm *FileIORWManager) WriteAt(b []byte, off int64) (n int, err error) {
	return fm.fd.WriteAt(b, off)
}

// ReadAt 从字节偏移量off开始从文件中读取len(b)个字节。 *File.ReadAt
func (fm *FileIORWManager) ReadAt(b []byte, off int64) (n int, err error) {
	return fm.fd.ReadAt(b, off)
}

// Sync 将文件的当前内容提交到稳定存储。*File.Sync
// 通常，这意味着要写入内存副本数据到文件系统的磁盘。
func (fm *FileIORWManager) Sync() (err error) {
	return fm.fd.Sync()
}

// Close 关闭文件，使其无法使用I/O。 *File.Close.
func (fm *FileIORWManager) Close() (err error) {
	return fm.fd.Close()
}
