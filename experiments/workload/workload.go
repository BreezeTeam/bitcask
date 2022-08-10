package workload

import "fmt"

type OperationKind string

const (
	Put OperationKind = "put"
	Get OperationKind = "get"
)

type Operation struct {
	Kind  OperationKind
	Key   []byte
	Value []byte
}

func SequentialWriteRead(keys int) []Operation {
	ops := make([]Operation, 0, keys*2)
	for i := 0; i < keys; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		ops = append(ops, Operation{Kind: Put, Key: append([]byte(nil), key...), Value: []byte(fmt.Sprintf("value-%06d", i))})
	}
	for i := 0; i < keys; i++ {
		ops = append(ops, Operation{Kind: Get, Key: []byte(fmt.Sprintf("key-%06d", i))})
	}
	return ops
}

func HotspotRead(keys, reads int, hotKeys []int) []Operation {
	ops := SequentialWriteRead(keys)
	for i := 0; i < reads; i++ {
		keyID := hotKeys[i%len(hotKeys)]
		ops = append(ops, Operation{Kind: Get, Key: []byte(fmt.Sprintf("key-%06d", keyID))})
	}
	return ops
}

func SmallValueWrite(keys int, valueSize int) []Operation {
	ops := make([]Operation, 0, keys)
	value := fixedValue(valueSize)
	for i := 0; i < keys; i++ {
		ops = append(ops, Operation{Kind: Put, Key: []byte(fmt.Sprintf("key-%06d", i)), Value: append([]byte(nil), value...)})
	}
	return ops
}

func OverwriteHotset(keys, writes, hotset int, valueSize int) []Operation {
	ops := SmallValueWrite(keys, valueSize)
	value := fixedValue(valueSize)
	for i := 0; i < writes; i++ {
		keyID := i % hotset
		ops = append(ops, Operation{Kind: Put, Key: []byte(fmt.Sprintf("key-%06d", keyID)), Value: append([]byte(nil), value...)})
	}
	return ops
}

func MixedReadWrite(keys, operations int, writeRatio int, valueSize int) []Operation {
	ops := SmallValueWrite(keys, valueSize)
	value := fixedValue(valueSize)
	for i := 0; i < operations; i++ {
		keyID := i % keys
		if ((i + 1) * writeRatio / 100) > (i * writeRatio / 100) {
			ops = append(ops, Operation{Kind: Put, Key: []byte(fmt.Sprintf("key-%06d", keyID)), Value: append([]byte(nil), value...)})
			continue
		}
		ops = append(ops, Operation{Kind: Get, Key: []byte(fmt.Sprintf("key-%06d", keyID))})
	}
	return ops
}

func fixedValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i % 251)
	}
	return value
}
