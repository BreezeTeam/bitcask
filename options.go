package bitcask

type EntryIdxMode int // 条目索引模式

const (
	// HintKeyValAndRAMIdxMode ram key value
	HintKeyValAndRAMIdxMode EntryIdxMode = iota // todo:

	// HintKeyAndRAMIdxMode ram key
	HintKeyAndRAMIdxMode

	// HintBPTSparseIdxMode  b+ 树稀疏索引模式
	HintBPTSparseIdxMode
)

type Options struct {
	Dir                  string
	SegmentSize          int64        // wiki:最大分段大小是传输控制协议的一个参数，以字节数定义一个计算机或通信设备所能接受的分段的最大数据量。
	RWMode               RWMode       // RWManager 接口实现方式选择 有 标准io和 mmap 两种
	EntryIdxMode         EntryIdxMode // 条目索引模式。
	StartFileLoadingMode RWMode       // 打开一个数据库，加载文件 时的 RWMode
}

//8 mb 的默认写入大小
var defaultSegmentSize int64 = 8 * 1024 * 1024

// DefaultOptions represents the default options.
var DefaultOptions = Options{
	SegmentSize:  defaultSegmentSize,
	RWMode:       FileIO,               //默认为标准文件io
	EntryIdxMode: HintBPTSparseIdxMode, // B+ 树稀疏索引
}
