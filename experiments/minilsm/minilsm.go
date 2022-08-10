package minilsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var ErrNotFound = errors.New("key not found")

type DB struct {
	dir      string
	walPath  string
	sstPath  string
	mu       sync.RWMutex
	wal      *os.File
	memtable map[string][]byte
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	db := &DB{
		dir:      dir,
		walPath:  filepath.Join(dir, "wal.log"),
		sstPath:  filepath.Join(dir, "sst.data"),
		memtable: make(map[string][]byte),
	}
	if err := db.loadSSTable(); err != nil {
		return nil, err
	}
	if err := db.replayWAL(); err != nil {
		return nil, err
	}
	wal, err := os.OpenFile(db.walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	db.wal = wal
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := writeRecord(db.wal, key, value); err != nil {
		return err
	}
	db.memtable[string(key)] = append([]byte(nil), value...)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	value, ok := db.memtable[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tmpPath := db.sstPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(db.memtable))
	for key := range db.memtable {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writeRecord(file, []byte(key), db.memtable[key]); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, db.sstPath); err != nil {
		return err
	}
	if err := db.wal.Truncate(0); err != nil {
		return err
	}
	if _, err := db.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return db.wal.Sync()
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.wal == nil {
		return nil
	}
	err := db.wal.Close()
	db.wal = nil
	return err
}

func (db *DB) loadSSTable() error {
	file, err := os.Open(db.sstPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	return readRecords(file, db.memtable)
}

func (db *DB) replayWAL() error {
	file, err := os.Open(db.walPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	return readRecords(file, db.memtable)
}

func writeRecord(w io.Writer, key, value []byte) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	if len(key) > int(^uint32(0)) || len(value) > int(^uint32(0)) {
		return errors.New("record too large")
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func readRecords(r io.Reader, dst map[string][]byte) error {
	reader := bufio.NewReader(r)
	for {
		var header [8]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		keySize := binary.LittleEndian.Uint32(header[0:4])
		valueSize := binary.LittleEndian.Uint32(header[4:8])
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("read key: %w", err)
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return fmt.Errorf("read value: %w", err)
		}
		dst[string(key)] = value
	}
}
