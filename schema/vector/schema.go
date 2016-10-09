package vector

import (
	"github.com/flier/arrow/memory"
)

type FieldNode struct {
	Length    int
	NullCount int
}

type RecordBatch struct {
	Length  int
	Nodes   []*FieldNode
	Buffers []*memory.Buffer
}
