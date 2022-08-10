package bitcask

import (
	"bitcask/rwmanager"
	"time"
)

type EntryIdxMode int // 条目索引模式

type SyncPolicyMode int

type CompactionPolicyMode int

const (
	// HintKeyValAndRAMIdxMode ram key value
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode ram key
	HintKeyAndRAMIdxMode

	// HintBPTSparseIdxMode  b+ 树稀疏索引模式
	HintBPTSparseIdxMode
)

const (
	SyncPolicyDefault SyncPolicyMode = iota
	SyncPolicyNone
	SyncPolicyEveryCommit
	SyncPolicyGroupCommit
	SyncPolicyAdaptive
)

const (
	CompactionByFileID CompactionPolicyMode = iota
	CompactionByGarbageRatio
	CompactionHotCold
)

type SyncPolicyOptions struct {
	Mode              SyncPolicyMode
	GroupMaxDelay     time.Duration
	GroupMaxWrites    int
	DirtyBytesLimit   int64
	AdaptiveMinDelay  time.Duration
	AdaptiveMaxDelay  time.Duration
	TargetSyncLatency time.Duration
}

type CompactionOptions struct {
	Mode               CompactionPolicyMode
	MinGarbageRatio    float64
	HotKeySampleWindow int
	ColdAgeThreshold   time.Duration
}

type KVSeparationOptions struct {
	Enable              bool
	Threshold           int
	ValueLogSegmentSize int64
}

type FaultInjectionOptions struct {
	Enable            bool
	WriteFailAfter    int64
	SyncFailAfter     int64
	ShortWriteAfter   int64
	CorruptAfterWrite bool
	ReadCorruptAfter  int64
}

// Options 配置 结构体
type Options struct {
	Dir                  string
	SegmentSize          int64            // wiki:最大分段大小是传输控制协议的一个参数，以字节数定义一个计算机或通信设备所能接受的分段的最大数据量。
	RWMode               rwmanager.RWMode // RWManager 接口实现方式选择 有 标准io和 mmap 两种
	EntryIdxMode         EntryIdxMode     // 条目索引模式。
	StartFileLoadingMode rwmanager.RWMode // 打开一个数据库，加载文件 时的 RWMode
	// SyncEnable represents if call Sync() function.
	// if SyncEnable is false, high write performance but potential data loss likely.
	// if SyncEnable is true, slower but persistent.
	SyncEnable     bool
	SyncPolicy     SyncPolicyOptions
	Compaction     CompactionOptions
	KVSeparation   KVSeparationOptions
	FaultInjection FaultInjectionOptions
	NodeNum        int64

	faultState *faultInjectionState
}

// defaultSegmentSize 8 mb 的默认写入大小
// var defaultSegmentSize int64 = 0.5 * 1024 * 1024
var defaultSegmentSize int64 = 100

// DefaultOptions represents the default options.
var DefaultOptions = Options{
	SegmentSize:  defaultSegmentSize,
	RWMode:       rwmanager.FileIO,        //默认为标准文件io
	EntryIdxMode: HintKeyValAndRAMIdxMode, // B+ 树稀疏索引
	FaultInjection: FaultInjectionOptions{
		WriteFailAfter:   -1,
		SyncFailAfter:    -1,
		ShortWriteAfter:  -1,
		ReadCorruptAfter: -1,
	},
}
