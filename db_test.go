package bitcask

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
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
			return tx.Put(bucket, key, val, 1)
		}); err != nil {
		t.Fatal(err)
	}
	//get
	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				if string(e.Value) != string(val) {
					t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
				} else {
					t.Logf("Tx Get. got %s want %s", string(e.Value), string(val))
				}
			} else {
				t.Errorf("error: %s", err.Error())
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	//delete
	if err := db.Update(
		func(tx *Tx) error {
			err := tx.Delete(bucket, key)
			if err != nil {
				t.Fatal(err)
			} else {
				t.Logf("delete success %s %s", bucket, key)
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				if string(e.Value) != string(val) {
					t.Logf("err Tx Get. got %s want %s", string(e.Value), string(val))
				} else {
					t.Logf("Tx Get. got %s want %s", string(e.Value), string(val))
				}
			} else {
				t.Logf("error: %s", err.Error())
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	//update
	val = []byte("val001")
	if err := db.Update(
		func(tx *Tx) error {
			return tx.Put(bucket, key, val, Persistent)
		}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(
		func(tx *Tx) error {
			e, err := tx.Get(bucket, key)
			if err == nil {
				if string(e.Value) != string(val) {
					t.Errorf("err Tx Get. got %s want %s", string(e.Value), string(val))
				} else {
					t.Logf("Tx Get. got %s want %s", string(e.Value), string(val))
				}
			} else {
				t.Errorf("error: %s", err.Error())
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Merge_For_string(t *testing.T) {
	fileDir := "./data"

	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}

	opt := DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 1 * 100

	db2, err := Open(opt)

	if err != nil {
		t.Fatal(err)
	}

	bucketForString := "test_merge"

	key1 := []byte("key_" + fmt.Sprintf("%07d", 1))
	value1 := []byte("value1value1value1value1value1")
	if err := db2.Update(
		func(tx *Tx) error {
			return tx.Put(bucketForString, key1, value1, Persistent)
		}); err != nil {
		t.Error("initStringDataAndDel,err batch put", err)
	}

	key2 := []byte("key_" + fmt.Sprintf("%07d", 2))
	value2 := []byte("value2value2value2value2value2")
	if err := db2.Update(
		func(tx *Tx) error {
			return tx.Put(bucketForString, key2, value2, Persistent)
		}); err != nil {
		t.Error("initStringDataAndDel,err batch put", err)
	}

	if err := db2.Update(
		func(tx *Tx) error {
			return tx.Delete(bucketForString, key2)
		}); err != nil {
		t.Error(err)
	}

	if err := db2.View(
		func(tx *Tx) error {
			if _, err := tx.Get(bucketForString, key2); err == nil {
				t.Error("err read data ", err)
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	//GetValidKeyCount
	//validKeyNum := db2.BPTreeIdx[bucketForString].ValidKeyCount
	//if validKeyNum != 1 {
	//	t.Errorf("err GetValidKeyCount. got %d want %d", validKeyNum, 1)
	//}
	time.Sleep(1)
	if err = db2.Merge(); err != nil {
		t.Error("err merge", err)
	}
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
