package vector

import (
	"github.com/flier/arrow/flatbuf"
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

func UnmarshalRecordBatch(batch *flatbuf.RecordBatch, body []byte) (*RecordBatch, error) {
	var nodes []*FieldNode
	var node flatbuf.FieldNode

	for i := 0; i < batch.NodesLength(); i++ {
		if batch.Nodes(&node, i) {
			nodes = append(nodes, &FieldNode{
				Length:    int(node.Length()),
				NullCount: int(node.NullCount()),
			})
		}
	}

	var buffers []*memory.Buffer
	var buffer flatbuf.Buffer

	for i := 0; i < batch.BuffersLength(); i++ {
		if batch.Buffers(&buffer, i) {
			buffers = append(buffers, memory.NewBuffer(body[buffer.Offset():buffer.Offset()+buffer.Length()]))
		}
	}

	return &RecordBatch{
		Length:  int(batch.Length()),
		Nodes:   nodes,
		Buffers: buffers,
	}, nil
}
