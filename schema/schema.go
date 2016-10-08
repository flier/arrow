package schema

import (
	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/memory"
)

type Schema struct {
	Fields []*Field
}

func NewSchema(schema *flatbuf.Schema) *Schema {
	return &Schema{}
}

type Field struct {
	Name     string
	Nullable bool
	Children []*Field
}

type FieldNode struct {
	Length    int
	NullCount int
}

type RecordBatch struct {
	Length  int
	Nodes   []*FieldNode
	Buffers []*memory.Buffer
}
