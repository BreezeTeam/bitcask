package compaction

import "fmt"

type OperationKind string

const (
	OperationPut OperationKind = "put"
	OperationGet OperationKind = "get"
)

type Operation struct {
	Kind  OperationKind
	Key   string
	Bytes int
}

type WorkloadReport struct {
	Operations         int
	Writes             int
	Reads              int
	LogicalWriteBytes  int64
	LiveBytes          int64
	ObsoleteBytes      int64
	RewriteBytes       int64
	WriteAmplification float64
	SpaceAmplification float64
}

func OverwriteHotset(keys, overwrites, valueBytes int) []Operation {
	operations := make([]Operation, 0, keys+overwrites)
	for i := 0; i < keys; i++ {
		operations = append(operations, Operation{Kind: OperationPut, Key: fmt.Sprintf("key-%06d", i), Bytes: valueBytes})
	}
	for i := 0; i < overwrites; i++ {
		operations = append(operations, Operation{Kind: OperationPut, Key: fmt.Sprintf("key-%06d", i%keys), Bytes: valueBytes})
	}
	return operations
}

func ColdGarbage(keys, overwrittenKeys, valueBytes int) []Operation {
	operations := make([]Operation, 0, keys+overwrittenKeys)
	for i := 0; i < keys; i++ {
		operations = append(operations, Operation{Kind: OperationPut, Key: fmt.Sprintf("key-%06d", i), Bytes: valueBytes})
	}
	for i := 0; i < overwrittenKeys; i++ {
		operations = append(operations, Operation{Kind: OperationPut, Key: fmt.Sprintf("key-%06d", i), Bytes: valueBytes})
	}
	return operations
}

func Mixed(keys, operations, writePercent, valueBytes int) []Operation {
	result := make([]Operation, 0, keys+operations)
	for i := 0; i < keys; i++ {
		result = append(result, Operation{Kind: OperationPut, Key: fmt.Sprintf("key-%06d", i), Bytes: valueBytes})
	}
	for i := 0; i < operations; i++ {
		kind := OperationGet
		if ((i + 1) * writePercent / 100) > (i * writePercent / 100) {
			kind = OperationPut
		}
		result = append(result, Operation{Kind: kind, Key: fmt.Sprintf("key-%06d", i%keys), Bytes: valueBytes})
	}
	return result
}

func AnalyzeWorkload(operations []Operation, rewriteBytes, physicalBytes int64) WorkloadReport {
	latest := make(map[string]int)
	report := WorkloadReport{Operations: len(operations), RewriteBytes: rewriteBytes}
	for _, operation := range operations {
		if operation.Kind == OperationGet {
			report.Reads++
			continue
		}
		report.Writes++
		report.LogicalWriteBytes += int64(operation.Bytes)
		latest[operation.Key] = operation.Bytes
	}
	for _, size := range latest {
		report.LiveBytes += int64(size)
	}
	report.ObsoleteBytes = report.LogicalWriteBytes - report.LiveBytes
	if report.LogicalWriteBytes > 0 {
		report.WriteAmplification = float64(report.LogicalWriteBytes+rewriteBytes) / float64(report.LogicalWriteBytes)
	}
	if report.LiveBytes > 0 {
		report.SpaceAmplification = float64(physicalBytes) / float64(report.LiveBytes)
	}
	return report
}
