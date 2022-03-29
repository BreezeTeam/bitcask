package bitcask

import (
	"bitcask/helper"
	"io/ioutil"
	"path"
	"sort"
	"strings"
	"testing"
)

func getMaxFileIDAndFileIDs(t *testing.T) {

	tests := []struct {
		name string
		dir  string
	}{
		{
			name: "1",
			dir:  "./data",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			opt := DefaultOptions
			opt.Dir = tt.dir
			db, _ := Open(opt)

			got, got1 := db.getMaxFileIDAndFileIDs()
			t.Logf("gotgetMaxFileIDAndFileIDs() got = %v", got)
			t.Logf("getMaxFileIDAndFileIDs() got1 = %v", got1)
		})
	}
}

func BenchmarkDB_getMaxFileIDAndFileIDs(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = "./data"
	db, _ := Open(opt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.getMaxFileIDAndFileIDs()
	}

}

//查找文件夹下面最大的文件id和文件id列表
func (db *DB) getMaxFileIDAndFileIDs2() (int64, []int) {
	var (
		dataFileIds []int
		maxFileID   int64
	)
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}
	maxFileID = 0
	//循环每天一个
	for _, f := range files {
		name := f.Name()
		// 判断文件名扩展名是不是数据文件,获取,路径的最后一个元素,然后分割.
		ext := path.Ext(path.Base(name))
		if ext != DataSuffix {
			continue
		}
		//如果扩展就是数据,那么文件名中包含id
		id := strings.TrimSuffix(name, DataSuffix)
		idVal, _ := helper.StrToInt(id) //将id转为int
		dataFileIds = append(dataFileIds, idVal)
	}
	if len(dataFileIds) == 0 {
		return 0, nil
	}
	sort.Ints(dataFileIds)
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])

	return maxFileID, dataFileIds
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
