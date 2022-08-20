package bitcask

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkTxCommitLifecycle(b *testing.B) {
	for _, writes := range []int{0, 1} {
		writes := writes
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 64 * 1024 * 1024
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			key := []byte("key")
			value := []byte("value")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin(true)
				if err != nil {
					b.Fatal(err)
				}
				if writes == 1 {
					if err := tx.Put("bench", key, value, Persistent); err != nil {
						b.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTxCommitSingleEntryBaseline(b *testing.B) {
	for _, valueSize := range []int{0, 8, 16, 32, 1024, 16 * 1024} {
		valueSize := valueSize
		for _, syncEnable := range []bool{false, true} {
			syncEnable := syncEnable
			b.Run(fmt.Sprintf("value=%dB/sync=%t", valueSize, syncEnable), func(b *testing.B) {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 64 * 1024 * 1024
				opt.SyncEnable = syncEnable
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				payload := make([]byte, valueSize)
				b.SetBytes(int64(valueSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := db.Update(func(tx *Tx) error {
						return tx.Put("bench", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTxCommitBatchBaseline(b *testing.B) {
	for _, batchSize := range []int{1, 8, 64} {
		batchSize := batchSize
		for _, valueSize := range []int{32, 1024} {
			valueSize := valueSize
			for _, syncEnable := range []bool{false, true} {
				syncEnable := syncEnable
				b.Run(fmt.Sprintf("batch=%d/value=%dB/sync=%t", batchSize, valueSize, syncEnable), func(b *testing.B) {
					opt := DefaultOptions
					opt.Dir = b.TempDir()
					opt.SegmentSize = 64 * 1024 * 1024
					opt.SyncEnable = syncEnable
					db, err := Open(opt)
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()

					payload := make([]byte, valueSize)
					b.SetBytes(int64(valueSize * batchSize))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := db.Update(func(tx *Tx) error {
							for j := 0; j < batchSize; j++ {
								if err := tx.Put("bench", []byte(fmt.Sprintf("key-%09d-%03d", i, j)), payload, Persistent); err != nil {
									return err
								}
							}
							return nil
						}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkSmallValueFixedCostBaseline(b *testing.B) {
	for _, valueSize := range []int{0, 8, 16, 32, 64} {
		valueSize := valueSize
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 64 * 1024 * 1024
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			payload := make([]byte, valueSize)
			b.SetBytes(int64(valueSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error {
					return tx.Put("bench", []byte(fmt.Sprintf("small-%09d", i)), payload, Persistent)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTxPendingEntryConstruction(b *testing.B) {
	key := []byte("key")
	value := make([]byte, 32)
	bucket := "bench"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newPendingEntry(bucket, key, value, Persistent, DataSetFlag, uint64(time.Now().Unix()), DataStructureBPTree, uint64(i))
	}
}

func BenchmarkEntryIndexKey(b *testing.B) {
	bucket := []byte("bench")
	key := []byte("key-000000001")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = entryIndexKey(bucket, key)
	}
}

func BenchmarkWriteAllocationMatrix(b *testing.B) {
	for _, valueSize := range []int{0, 8, 32, 1024, 16 * 1024, 64 * 1024} {
		valueSize := valueSize
		for _, batchSize := range []int{1, 8, 64, 256} {
			batchSize := batchSize
			b.Run(fmt.Sprintf("value=%dB/batch=%d", valueSize, batchSize), func(b *testing.B) {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 256 * 1024 * 1024
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				value := make([]byte, valueSize)
				items := make([]KV, batchSize)
				for i := range items {
					items[i] = KV{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: value}
				}
				b.SetBytes(int64(valueSize * batchSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := db.PutBatch("bench", items, Persistent); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkSmallValueFixedCostOptimized(b *testing.B) {
	for _, valueSize := range []int{0, 8, 16, 32, 64} {
		valueSize := valueSize
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 64 * 1024 * 1024
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			payload := make([]byte, valueSize)
			b.SetBytes(int64(valueSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error {
					return tx.Put("bench", []byte(fmt.Sprintf("small-%09d", i)), payload, Persistent)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTxPutBatchStaging(b *testing.B) {
	for _, batchSize := range []int{1, 8, 64, 256} {
		batchSize := batchSize
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			db := &DB{}
			items := make([]KV, batchSize)
			for i := range items {
				items[i] = KV{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: make([]byte, 32)}
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tx := &Tx{db: db, writable: true, pendingWrites: nil}
				if err := tx.PutBatch("bench", items, Persistent); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTxPutBatchArenaStaging(b *testing.B) {
	for _, batchSize := range []int{1, 8, 64, 256} {
		batchSize := batchSize
		for _, valueSize := range []int{0, 32, 1024} {
			valueSize := valueSize
			b.Run(fmt.Sprintf("batch=%d/value=%dB", batchSize, valueSize), func(b *testing.B) {
				db := &DB{}
				items := make([]KV, batchSize)
				for i := range items {
					items[i] = KV{Key: []byte(fmt.Sprintf("key-%03d", i)), Value: make([]byte, valueSize)}
				}
				b.ReportAllocs()
				b.SetBytes(int64(valueSize * batchSize))
				for i := 0; i < b.N; i++ {
					tx := &Tx{db: db, writable: true, pendingWrites: nil}
					if err := tx.PutBatch("bench", items, Persistent); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTxPutBatch(b *testing.B) {
	for _, batchSize := range []int{1, 8, 64, 256} {
		batchSize := batchSize
		for _, valueSize := range []int{32, 1024, 16 * 1024} {
			valueSize := valueSize
			b.Run(fmt.Sprintf("batch=%d/value=%dB", batchSize, valueSize), func(b *testing.B) {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 128 * 1024 * 1024
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				payload := make([]byte, valueSize)
				items := make([]KV, batchSize)
				for i := range items {
					items[i].Value = payload
				}
				b.SetBytes(int64(valueSize * batchSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := range items {
						items[j].Key = []byte(fmt.Sprintf("key-%09d-%03d", i, j))
					}
					if err := db.PutBatch("bench", items, Persistent); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkTxCommitSyncPolicy(b *testing.B) {
	policies := []struct {
		name       string
		syncEnable bool
		mode       SyncPolicyMode
	}{
		{name: "none", mode: SyncPolicyNone},
		{name: "every-commit", mode: SyncPolicyEveryCommit},
		{name: "group", mode: SyncPolicyGroupCommit},
	}
	for _, policy := range policies {
		policy := policy
		for _, batchSize := range []int{1, 8, 64} {
			batchSize := batchSize
			for _, valueSize := range []int{32, 1024} {
				valueSize := valueSize
				b.Run(fmt.Sprintf("policy=%s/batch=%d/value=%dB", policy.name, batchSize, valueSize), func(b *testing.B) {
					opt := DefaultOptions
					opt.Dir = b.TempDir()
					opt.SegmentSize = 128 * 1024 * 1024
					opt.SyncEnable = policy.syncEnable
					opt.SyncPolicy.Mode = policy.mode
					db, err := Open(opt)
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()

					payload := make([]byte, valueSize)
					items := make([]KV, batchSize)
					for i := range items {
						items[i].Value = payload
					}
					b.SetBytes(int64(valueSize * batchSize))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for j := range items {
							items[j].Key = []byte(fmt.Sprintf("sync-%09d-%03d", i, j))
						}
						if err := db.PutBatch("sync", items, Persistent); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func BenchmarkTxCommitAdaptiveSync(b *testing.B) {
	policies := []struct {
		name string
		mode SyncPolicyMode
	}{
		{name: "every-commit", mode: SyncPolicyEveryCommit},
		{name: "group", mode: SyncPolicyGroupCommit},
		{name: "adaptive", mode: SyncPolicyAdaptive},
	}
	for _, policy := range policies {
		policy := policy
		b.Run(policy.name, func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 128 * 1024 * 1024
			opt.SyncPolicy.Mode = policy.mode
			opt.SyncPolicy.DirtyBytesLimit = 64 * 1024
			opt.SyncPolicy.AdaptiveMinDelay = 100 * time.Microsecond
			opt.SyncPolicy.AdaptiveMaxDelay = 5 * time.Millisecond
			opt.SyncPolicy.TargetSyncLatency = time.Millisecond
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			payload := make([]byte, 1024)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error {
					return tx.Put("adaptive", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMetricsSnapshot(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		return tx.Put("metrics", []byte("k"), []byte("v"), Persistent)
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Metrics()
	}
}

func BenchmarkTxCommitWithMetrics(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 64 * 1024 * 1024
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("metrics", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBMergePolicy(b *testing.B) {
	policies := []struct {
		name string
		mode CompactionPolicyMode
	}{
		{name: "file-id", mode: CompactionByFileID},
		{name: "garbage-ratio", mode: CompactionByGarbageRatio},
		{name: "hot-cold", mode: CompactionHotCold},
	}
	for _, policy := range policies {
		policy := policy
		b.Run(policy.name, func(b *testing.B) {
			payload := []byte("01234567890123456789012345678901")
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 512
				opt.Compaction.Mode = policy.mode
				opt.Compaction.MinGarbageRatio = 0.1
				opt.Compaction.HotKeySampleWindow = 10
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				// Fixed-size overwrite workload so each timed op has bounded merge work.
				for j := 0; j < 64; j++ {
					key := []byte(fmt.Sprintf("k-%03d", j%16))
					if err := db.PutBatch("merge", []KV{{Key: key, Value: payload}}, Persistent); err != nil {
						db.Close()
						b.Fatal(err)
					}
				}
				if err := db.Merge(); err != nil {
					db.Close()
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkKVSeparationPut(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		enabled := enabled
		for _, valueSize := range []int{512, 4096, 65536} {
			valueSize := valueSize
			b.Run(fmt.Sprintf("kvsep=%t/value=%dB", enabled, valueSize), func(b *testing.B) {
				opt := DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 128 * 1024 * 1024
				opt.KVSeparation.Enable = enabled
				opt.KVSeparation.Threshold = 1024
				db, err := Open(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				payload := make([]byte, valueSize)
				b.SetBytes(int64(valueSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := db.Update(func(tx *Tx) error {
						return tx.Put("kvsep", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkKVSeparationGet(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		enabled := enabled
		b.Run(fmt.Sprintf("kvsep=%t", enabled), func(b *testing.B) {
			opt := DefaultOptions
			opt.Dir = b.TempDir()
			opt.SegmentSize = 128 * 1024 * 1024
			opt.KVSeparation.Enable = enabled
			opt.KVSeparation.Threshold = 1024
			db, err := Open(opt)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			payload := make([]byte, 4096)
			if err := db.Update(func(tx *Tx) error { return tx.Put("kvsep", []byte("key"), payload, Persistent) }); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.View(func(tx *Tx) error {
					_, err := tx.Get("kvsep", []byte("key"))
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFaultInjectionDisabledWritePath(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 64 * 1024 * 1024
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("fault", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFaultInjectionEnabledNoFault(b *testing.B) {
	opt := DefaultOptions
	opt.Dir = b.TempDir()
	opt.SegmentSize = 64 * 1024 * 1024
	opt.FaultInjection.Enable = true
	opt.FaultInjection.WriteFailAfter = int64(b.N + 1)
	opt.FaultInjection.SyncFailAfter = int64(b.N + 1)
	opt.FaultInjection.ShortWriteAfter = int64(b.N + 1)
	opt.FaultInjection.ReadCorruptAfter = int64(b.N + 1)
	db, err := Open(opt)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put("fault", []byte(fmt.Sprintf("key-%09d", i)), payload, Persistent)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWritePathEncode(b *testing.B) {
	for _, valueSize := range []int{32, 1024, 16 * 1024} {
		valueSize := valueSize
		entry := benchEntry("bench", []byte("key"), make([]byte, valueSize), 1)
		b.Run(fmt.Sprintf("value=%dB/alloc", valueSize), func(b *testing.B) {
			b.SetBytes(entry.Size())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = entry.Encode()
			}
		})
		b.Run(fmt.Sprintf("value=%dB/reuse", valueSize), func(b *testing.B) {
			buf := make([]byte, 0, entry.Size())
			b.SetBytes(entry.Size())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf = entry.EncodeTo(buf)
			}
		})
	}
}

func BenchmarkWritePathDataFileAppend(b *testing.B) {
	for _, valueSize := range []int{32, 1024, 16 * 1024} {
		valueSize := valueSize
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			df, err := NewDataFile(b.TempDir(), 0, int64(b.N*(valueSize+1024)+1024*1024), DefaultOptions.RWMode)
			if err != nil {
				b.Fatal(err)
			}
			defer df.Close()

			encoded := benchEntry("bench", []byte("key"), make([]byte, valueSize), 1).Encode()
			b.SetBytes(int64(len(encoded)))
			b.ReportAllocs()
			b.ResetTimer()
			var off int64
			for i := 0; i < b.N; i++ {
				if _, err := df.WriteAt(encoded, off); err != nil {
					b.Fatal(err)
				}
				off += int64(len(encoded))
			}
		})
	}
}

func BenchmarkWritePathDataFileSync(b *testing.B) {
	df, err := NewDataFile(b.TempDir(), 0, int64(b.N*4096+1024*1024), DefaultOptions.RWMode)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Close()

	encoded := benchEntry("bench", []byte("key"), make([]byte, 1024), 1).Encode()
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	var off int64
	for i := 0; i < b.N; i++ {
		if _, err := df.WriteAt(encoded, off); err != nil {
			b.Fatal(err)
		}
		if err := df.Sync(); err != nil {
			b.Fatal(err)
		}
		off += int64(len(encoded))
	}
}

func BenchmarkWritePathBPTreeInsert(b *testing.B) {
	for _, countFlag := range []bool{CountFlagEnabled, CountFlagDisabled} {
		countFlag := countFlag
		b.Run(fmt.Sprintf("count=%t", countFlag), func(b *testing.B) {
			tree := NewTree()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%09d", i))
				record := &Record{H: &Hint{Key: key, FileID: 0, Meta: &MetaData{Flag: DataSetFlag, TTL: Persistent, Timestamp: uint64(time.Now().Unix()), TxID: 1}, DataPos: uint64(i)}}
				tree.Insert(key, nil, record.H, countFlag)
			}
		})
	}
}

func benchEntry(bucket string, key, value []byte, txID uint64) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			KeySize:    uint32(len(key)),
			ValueSize:  uint32(len(value)),
			Timestamp:  uint64(time.Now().Unix()),
			Flag:       DataSetFlag,
			TTL:        Persistent,
			Bucket:     []byte(bucket),
			BucketSize: uint32(len(bucket)),
			Status:     Committed,
			Ds:         DataStructureBPTree,
			TxID:       txID,
		},
	}
}
