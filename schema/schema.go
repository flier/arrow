package schema

import (
	"github.com/flier/arrow/memory"
)

type Schema struct{}

type FieldNode struct {
	Length    int
	NullCount int
}

type RecordBatch struct {
	Length  int
	Nodes   []*FieldNode
	Buffers []*memory.Buffer
}
