package example

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitcask"
)

type profilePayload struct {
	UserID string            `json:"user_id"`
	Name   string            `json:"name"`
	Tags   []string          `json:"tags"`
	Attrs  map[string]string `json:"attrs"`
}

type eventPayload struct {
	ID        string
	Kind      string
	CreatedAt int64
	Data      []byte
}

type metricPayload struct {
	Timestamp int64
	CPU       uint64
	Memory    uint64
	Disk      uint64
}

type mixedWriteCase struct {
	name       string
	bucket     string
	value      func(worker, seq int) []byte
	writes     int
	paceEvery  int
	pace       time.Duration
	sampleKeys []string
}

func TestBackendStorageConcurrentMixedContent(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cases := []mixedWriteCase{
		{
			name:      "json-profile",
			bucket:    "backend:json-profile",
			writes:    80,
			paceEvery: 16,
			pace:      200 * time.Microsecond,
			value: func(worker, seq int) []byte {
				payload, err := json.Marshal(profilePayload{
					UserID: fmt.Sprintf("u-%02d-%04d", worker, seq),
					Name:   fmt.Sprintf("user-%02d", worker),
					Tags:   []string{"kv", "json", "profile"},
					Attrs:  map[string]string{"tier": "gold", "region": fmt.Sprintf("r-%d", seq%4)},
				})
				if err != nil {
					panic(err)
				}
				return payload
			},
		},
		{
			name:      "binary-metric",
			bucket:    "backend:binary-metric",
			writes:    160,
			paceEvery: 0,
			value: func(worker, seq int) []byte {
				payload := metricPayload{Timestamp: int64(seq), CPU: uint64(worker*100 + seq), Memory: uint64(seq * 1024), Disk: uint64(seq * 4096)}
				buf := make([]byte, 32)
				binary.LittleEndian.PutUint64(buf[0:8], uint64(payload.Timestamp))
				binary.LittleEndian.PutUint64(buf[8:16], payload.CPU)
				binary.LittleEndian.PutUint64(buf[16:24], payload.Memory)
				binary.LittleEndian.PutUint64(buf[24:32], payload.Disk)
				return buf
			},
		},
		{
			name:      "gob-event",
			bucket:    "backend:gob-event",
			writes:    60,
			paceEvery: 8,
			pace:      time.Millisecond,
			value: func(worker, seq int) []byte {
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(eventPayload{ID: fmt.Sprintf("evt-%02d-%04d", worker, seq), Kind: "checkout", CreatedAt: int64(seq), Data: bytes.Repeat([]byte{byte(seq % 251)}, 512+seq%256)}); err != nil {
					panic(err)
				}
				return buf.Bytes()
			},
		},
		{
			name:      "raw-blob",
			bucket:    "backend:raw-blob",
			writes:    30,
			paceEvery: 5,
			pace:      2 * time.Millisecond,
			value: func(worker, seq int) []byte {
				return bytes.Repeat([]byte{byte(worker + seq)}, 8*1024+seq*17)
			},
		},
	}

	var writes atomic.Int64
	var bytesWritten atomic.Int64
	var wg sync.WaitGroup
	for worker, wc := range cases {
		worker := worker
		wc := wc
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; seq < wc.writes; seq++ {
				key := mixedKey(wc.name, worker, seq)
				value := wc.value(worker, seq)
				if err := store.PutObject(Object{Bucket: wc.bucket, Key: key, Value: value, TTL: bitcask.Persistent}); err != nil {
					t.Errorf("put %s: %v", key, err)
					return
				}
				writes.Add(1)
				bytesWritten.Add(int64(len(value)))
				if wc.paceEvery > 0 && seq%wc.paceEvery == 0 {
					time.Sleep(wc.pace)
				}
			}
		}()
	}
	wg.Wait()

	var expectedWrites int64
	for worker, wc := range cases {
		expectedWrites += int64(wc.writes)
		for _, seq := range []int{0, wc.writes / 2, wc.writes - 1} {
			key := mixedKey(wc.name, worker, seq)
			want := wc.value(worker, seq)
			got, err := store.Object(wc.bucket, key)
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("value mismatch for %s: got %d bytes want %d bytes", key, len(got), len(want))
			}
		}
	}
	if writes.Load() != expectedWrites {
		t.Fatalf("writes got %d want %d", writes.Load(), expectedWrites)
	}
	if store.Stats().ValidKeyCount != int(expectedWrites) {
		t.Fatalf("valid keys got %d want %d", store.Stats().ValidKeyCount, expectedWrites)
	}
	if bytesWritten.Load() < 250*1024 {
		t.Fatalf("mixed workload too small: wrote %d bytes", bytesWritten.Load())
	}
}

func TestStorePutObjectsPersistsAllObjects(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	objects := []Object{
		{Bucket: "objects", Key: "json", Value: mustJSON(profilePayload{UserID: "u1", Name: "n1"}), TTL: bitcask.Persistent},
		{Bucket: "objects", Key: "metric", Value: makeMetricPayload(metricPayload{Timestamp: 1, CPU: 2}), TTL: bitcask.Persistent},
		{Bucket: "objects", Key: "blob", Value: bytes.Repeat([]byte("x"), 4096), TTL: bitcask.Persistent},
	}
	if err := store.PutObjects(objects); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, object := range objects {
		got, err := store.Object(object.Bucket, object.Key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, object.Value) {
			t.Fatalf("%s got %d bytes want %d", object.Key, len(got), len(object.Value))
		}
	}
}

func BenchmarkBackendConcurrentMixedWrites(b *testing.B) {
	store, err := OpenStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	payloads := [][]byte{
		mustJSON(profilePayload{UserID: "bench-user", Name: "benchmark", Tags: []string{"json", "small"}, Attrs: map[string]string{"region": "local"}}),
		makeMetricPayload(metricPayload{Timestamp: 1, CPU: 2, Memory: 3, Disk: 4}),
		mustGob(eventPayload{ID: "bench-event", Kind: "event", CreatedAt: 1, Data: bytes.Repeat([]byte("g"), 1024)}),
		bytes.Repeat([]byte("x"), 16*1024),
	}
	benchmarkConcurrentPayloadWrites(b, store, "backend:bench", payloads)
}

func BenchmarkBackendBatchWrites(b *testing.B) {
	for _, mode := range []string{"individual", "batch"} {
		mode := mode
		for _, batchSize := range []int{1, 8, 64} {
			batchSize := batchSize
			b.Run(fmt.Sprintf("mode=%s/batch=%d", mode, batchSize), func(b *testing.B) {
				opt := bitcask.DefaultOptions
				opt.Dir = b.TempDir()
				opt.SegmentSize = 1024 * 1024
				store, err := OpenStoreWithOptions(opt)
				if err != nil {
					b.Fatal(err)
				}
				defer store.Close()

				payload := bytes.Repeat([]byte("x"), 1024)
				b.SetBytes(int64(len(payload) * batchSize))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					objects := make([]Object, 0, batchSize)
					for j := 0; j < batchSize; j++ {
						objects = append(objects, Object{Bucket: "backend:batch", Key: fmt.Sprintf("batch-%d-%d", i, j), Value: payload, TTL: bitcask.Persistent})
					}
					if mode == "individual" {
						for _, object := range objects {
							if err := store.PutObject(object); err != nil {
								b.Fatal(err)
							}
						}
						continue
					}
					if err := store.PutObjects(objects); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkBackendWriteMatrix(b *testing.B) {
	for _, valueSize := range []int{32, 1024, 16 * 1024} {
		valueSize := valueSize
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			for _, segmentSize := range []int64{64 * 1024, 1024 * 1024} {
				segmentSize := segmentSize
				b.Run(fmt.Sprintf("segment=%dKB", segmentSize/1024), func(b *testing.B) {
					for _, syncEnable := range []bool{false, true} {
						syncEnable := syncEnable
						b.Run(fmt.Sprintf("sync=%t", syncEnable), func(b *testing.B) {
							opt := bitcask.DefaultOptions
							opt.Dir = b.TempDir()
							opt.SegmentSize = segmentSize
							opt.SyncEnable = syncEnable
							store, err := OpenStoreWithOptions(opt)
							if err != nil {
								b.Fatal(err)
							}
							defer store.Close()

							payload := bytes.Repeat([]byte("x"), valueSize)
							benchmarkConcurrentPayloadWrites(b, store, "backend:matrix", [][]byte{payload})
						})
					}
				})
			}
		})
	}
}

func benchmarkConcurrentPayloadWrites(b *testing.B, store *Store, bucket string, payloads [][]byte) {
	var totalPayloadBytes int
	for _, payload := range payloads {
		totalPayloadBytes += len(payload)
	}
	b.SetBytes(int64(totalPayloadBytes / len(payloads)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var seq int
		for pb.Next() {
			payload := payloads[seq%len(payloads)]
			key := fmt.Sprintf("bench-%d-%p", seq, pb)
			if err := store.PutObject(Object{Bucket: bucket, Key: key, Value: payload, TTL: bitcask.Persistent}); err != nil {
				b.Fatal(err)
			}
			seq++
		}
	})
}

func mixedKey(name string, worker, seq int) string {
	return fmt.Sprintf("%s:%02d:%06d", name, worker, seq)
}

func mustJSON(payload profilePayload) []byte {
	value, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return value
}

func mustGob(payload eventPayload) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func makeMetricPayload(payload metricPayload) []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(payload.Timestamp))
	binary.LittleEndian.PutUint64(buf[8:16], payload.CPU)
	binary.LittleEndian.PutUint64(buf[16:24], payload.Memory)
	binary.LittleEndian.PutUint64(buf[24:32], payload.Disk)
	return buf
}
