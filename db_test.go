package bitcask

import (
	"testing"
)

func TestDB_Basic(t *testing.T) {
	opt := DefaultOptions
	opt.Dir = "./data"
	db, _ := Open(opt)

	bucket := "bucket1"
	key := []byte("key1")
	val := []byte("val1")

	//put
	if err := db.Update(
		func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		}); err != nil {
		t.Fatal(err)
	}

	//get
	//if err := db.View(
	//	func(tx *Tx) error {
	//		e, err := tx.Get(bucket, key)
	//		if err == nil {
	//			if string(e.Value) != string(val) {
	//				t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
	//			}
	//		}
	//		return nil
	//	}); err != nil {
	//	t.Fatal(err)
	//}

	// delete
	//if err := db.Update(
	//	func(tx *Tx) error {
	//		err := tx.Delete(bucket, key)
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//		return nil
	//	}); err != nil {
	//	t.Fatal(err)
	//}

	//if err := db.View(
	//	func(tx *Tx) error {
	//		_, err := tx.Get(bucket, key)
	//		if err == nil {
	//			t.Errorf("err Tx Get.")
	//		}
	//		return nil
	//	}); err != nil {
	//	t.Fatal(err)
	//}

	////update
	//val = []byte("val001")
	//if err := db.Update(
	//	func(tx *Tx) error {
	//		return tx.Put(bucket, key, val, Persistent)
	//	}); err != nil {
	//	t.Fatal(err)
	//}

	//if err := db.View(
	//	func(tx *Tx) error {
	//		e, err := tx.Get(bucket, key)
	//		if err == nil {
	//			if string(e.Value) != string(val) {
	//				t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
	//			}
	//		}
	//		return nil
	//	}); err != nil {
	//	t.Fatal(err)
	//}
}

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
