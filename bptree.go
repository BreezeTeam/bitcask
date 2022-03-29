package bitcask

const (
	// The maximum number of keys in a record
	// Default number of b+ tree orders.
	order = 8

	// DefaultInvalidAddress returns default invalid node address.
	DefaultInvalidAddress = -1

	// RangeScan returns range scanMode flag.
	RangeScan = "RangeScan"

	// PrefixScan returns prefix scanMode flag.
	PrefixScan = "PrefixScan"

	// PrefixSearchScan returns prefix and search scanMode flag.
	PrefixSearchScan = "PrefixSearchScan"

	// CountFlagEnabled returns enabled CountFlag.
	CountFlagEnabled = true

	// CountFlagDisabled returns disabled CountFlag.
	CountFlagDisabled = false

	// BPTIndexSuffix returns b+ tree index suffix.
	BPTIndexSuffix = ".bptidx"

	// BPTRootIndexSuffix returns b+ tree root index suffix.
	BPTRootIndexSuffix = ".bptridx"

	// BPTTxIDIndexSuffix returns b+ tree tx ID index suffix.
	BPTTxIDIndexSuffix = ".bpttxid"

	// BPTRootTxIDIndexSuffix returns b+ tree root tx ID index suffix.
	BPTRootTxIDIndexSuffix = ".bptrtxid"
)

type (
	// BPTree records root node and valid key number.
	BPTree struct {
		root             *Node
		ValidKeyCount    int // the number of the key that not expired or deleted
		FirstKey         []byte
		LastKey          []byte
		LastAddress      int64
		Filepath         string
		bucketSize       uint32
		keyPosMap        map[string]int64
		enabledKeyPosMap bool
	}

	// Records records multi-records as result when is called Range or PrefixScan.
	Records []*Record

	// Node records keys and pointers and parent node.
	Node struct {
		Keys     [][]byte
		pointers []interface{}
		parent   *Node
		isLeaf   bool
		KeysNum  int
		Next     *Node
		Address  int64
	}

	// BinaryNode represents binary node.
	BinaryNode struct {
		// hint offset
		Keys [order - 1]int64
		// 1. not leaf node represents node address
		// 2. leaf node represents data address
		Pointers    [order]int64
		IsLeaf      uint16
		KeysNum     uint16
		Address     int64
		NextAddress int64
	}
)

// NewTree 初始化 BPTree
func NewTree() *BPTree {
	return &BPTree{LastAddress: 0, keyPosMap: make(map[string]int64), enabledKeyPosMap: false}
}
