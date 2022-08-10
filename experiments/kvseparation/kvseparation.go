package kvseparation

import "errors"

var ErrNotFound = errors.New("key not found")

type Pointer struct {
	Offset int64
	Size   int
}

type ValueLog struct {
	values []byte
}

type Store struct {
	index map[string]Pointer
	vlog  ValueLog
}

func NewStore() *Store {
	return &Store{index: make(map[string]Pointer)}
}

func (s *Store) Put(key, value []byte) Pointer {
	ptr := s.vlog.Append(value)
	s.index[string(key)] = ptr
	return ptr
}

func (s *Store) Get(key []byte) ([]byte, error) {
	ptr, ok := s.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return s.vlog.Read(ptr)
}

func (s *Store) LivePointers() map[string]Pointer {
	pointers := make(map[string]Pointer, len(s.index))
	for key, ptr := range s.index {
		pointers[key] = ptr
	}
	return pointers
}

func (v *ValueLog) Append(value []byte) Pointer {
	ptr := Pointer{Offset: int64(len(v.values)), Size: len(value)}
	v.values = append(v.values, value...)
	return ptr
}

func (v *ValueLog) Read(ptr Pointer) ([]byte, error) {
	end := ptr.Offset + int64(ptr.Size)
	if ptr.Offset < 0 || end > int64(len(v.values)) {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v.values[ptr.Offset:end]...), nil
}
