package workload

import "testing"

func TestSequentialWriteRead(t *testing.T) {
	ops := SequentialWriteRead(2)
	if len(ops) != 4 {
		t.Fatalf("got %d ops want 4", len(ops))
	}
	if ops[0].Kind != Put || string(ops[0].Key) != "key-000000" || string(ops[0].Value) != "value-000000" {
		t.Fatalf("unexpected first op: %#v", ops[0])
	}
	if ops[2].Kind != Get || string(ops[2].Key) != "key-000000" {
		t.Fatalf("unexpected first read op: %#v", ops[2])
	}
}

func TestHotspotRead(t *testing.T) {
	ops := HotspotRead(3, 4, []int{1, 2})
	if len(ops) != 10 {
		t.Fatalf("got %d ops want 10", len(ops))
	}
	want := []string{"key-000001", "key-000002", "key-000001", "key-000002"}
	for i, key := range want {
		got := ops[6+i]
		if got.Kind != Get || string(got.Key) != key {
			t.Fatalf("op %d got %#v want get %s", 6+i, got, key)
		}
	}
}

func TestSmallValueWrite(t *testing.T) {
	ops := SmallValueWrite(3, 8)
	if len(ops) != 3 {
		t.Fatalf("got %d ops want 3", len(ops))
	}
	for i, op := range ops {
		if op.Kind != Put || len(op.Value) != 8 {
			t.Fatalf("op %d got %#v want 8-byte put", i, op)
		}
	}
}

func TestOverwriteHotset(t *testing.T) {
	ops := OverwriteHotset(4, 5, 2, 16)
	if len(ops) != 9 {
		t.Fatalf("got %d ops want 9", len(ops))
	}
	want := []string{"key-000000", "key-000001", "key-000000", "key-000001", "key-000000"}
	for i, key := range want {
		got := ops[4+i]
		if got.Kind != Put || string(got.Key) != key || len(got.Value) != 16 {
			t.Fatalf("op %d got %#v want put %s", 4+i, got, key)
		}
	}
}

func TestMixedReadWrite(t *testing.T) {
	ops := MixedReadWrite(5, 10, 30, 4)
	if len(ops) != 15 {
		t.Fatalf("got %d ops want 15", len(ops))
	}
	var puts, gets int
	for _, op := range ops[5:] {
		if op.Kind == Put {
			puts++
		}
		if op.Kind == Get {
			gets++
		}
	}
	if puts != 3 || gets != 7 {
		t.Fatalf("got %d puts and %d gets, want 3 puts and 7 gets", puts, gets)
	}
}
