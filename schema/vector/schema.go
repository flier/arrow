package vector

import (
	"fmt"

	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/memory"
)

type FieldNode struct {
	Length    int
	NullCount int
}

func (n *FieldNode) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	return flatbuf.CreateFieldNode(builder, int32(n.Length), int32(n.NullCount)), nil
}

type Buffer struct {
	Page   int
	Offset int64
	Size   int64
}

func (b *Buffer) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	return flatbuf.CreateBuffer(builder, int32(b.Page), b.Offset, b.Size), nil
}

type RecordBatch struct {
	Length  int
	Nodes   []*FieldNode
	Buffers []*memory.Buffer
	Layouts []*Buffer
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

func (b *RecordBatch) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	nodesOffset, err := b.marshalNodes(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal recordBatch, %s", err)
	}

	buffersOffset, err := b.marshalLayouts(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal recordBatch, %s", err)
	}

	flatbuf.RecordBatchStart(builder)
	flatbuf.RecordBatchAddLength(builder, int32(b.Length))
	flatbuf.RecordBatchAddNodes(builder, nodesOffset)
	flatbuf.RecordBatchAddBuffers(builder, buffersOffset)
	return flatbuf.RecordBatchEnd(builder), nil
}

func (b *RecordBatch) marshalNodes(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.RecordBatchStartNodesVector(builder, len(b.Nodes))

	for _, node := range b.Nodes {
		if _, err := node.Marshal(builder); err != nil {
			return 0, fmt.Errorf("fail to marshal node, %s", err)
		}
	}

	return builder.EndVector(len(b.Nodes)), nil
}

func (b *RecordBatch) marshalLayouts(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.RecordBatchStartBuffersVector(builder, len(b.Layouts))

	for _, layout := range b.Layouts {
		if _, err := layout.Marshal(builder); err != nil {
			return 0, fmt.Errorf("fail to marshal layout, %s", err)
		}
	}

	return builder.EndVector(len(b.Layouts)), nil
}
