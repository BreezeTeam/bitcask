package bitcask

import (
	"testing"
)

/////////////////
// getMaxFileIDAndFileIDs getMaxFileIDAndFileIDs2 基准测试
/////////////////

func BenchmarkDB_getMaxFileIDAndFileIDs(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = "./data"
	db, _ := Open(opt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getMaxFileIDAndFileIDs()
	}
}

func BenchmarkDB_getMaxFileIDAndFileIDs2(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = "./data"
	db, _ := Open(opt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getMaxFileIDAndFileIDs2()
	}
}
