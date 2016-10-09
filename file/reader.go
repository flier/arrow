package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/memory"
	"github.com/flier/arrow/schema/vector"
)

const (
	Magic = "ARROW1"
)

var (
	ErrTooSmall      = errors.New("buffer too small")
	ErrBadMagic      = errors.New("missing magic number")
	ErrInvalidFooter = errors.New("invalid footer")
	ErrInvalidBlock  = errors.New("invalid block")
)

type Reader struct {
	in     *io.SectionReader
	footer *Footer
}

func (r *Reader) ReadFooter() (*Footer, error) {
	if r.footer != nil {
		return r.footer, nil
	}

	minSize := int64(len(Magic)*2 + 4)

	if r.in.Size() <= minSize {
		return nil, ErrTooSmall
	}

	buf := make([]byte, 4+len(Magic))
	off := r.in.Size() - int64(len(buf))

	if _, err := r.in.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("fail to read magic, %s", err)
	}

	if string(buf[4:]) != Magic {
		return nil, ErrBadMagic
	}

	footerLength := int64(int32(binary.LittleEndian.Uint32(buf[:4])))

	if footerLength <= 0 || footerLength+minSize > r.in.Size() {
		return nil, ErrInvalidFooter
	}

	off -= footerLength

	buf = make([]byte, footerLength)

	if _, err := r.in.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("fail to read footer, %s", err)
	}

	if footer, err := LoadFooter(flatbuf.GetRootAsFooter(buf, 0)); err != nil {
		return nil, fmt.Errorf("fail to parse footer, %s", err)
	} else {
		r.footer = footer
	}

	return r.footer, nil
}

// TODO: read dictionaries

func (r *Reader) ReadRecordBatch(block *Block) (*vector.RecordBatch, error) {
	bufSize := block.MetadataLen + block.BodyLen

	if bufSize < 0 {
		return nil, ErrInvalidBlock
	}

	buf := make([]byte, bufSize)

	if _, err := r.in.ReadAt(buf, block.Offset); err != nil {
		return nil, fmt.Errorf("fail to read records, %s", err)
	}

	batch := flatbuf.GetRootAsRecordBatch(buf[:block.MetadataLen], 0)
	body := buf[block.MetadataLen:]

	var nodes []*vector.FieldNode
	var node flatbuf.FieldNode

	for i := 0; i < batch.NodesLength(); i++ {
		if batch.Nodes(&node, i) {
			nodes = append(nodes, &vector.FieldNode{
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

	return &vector.RecordBatch{
		Length:  int(batch.Length()),
		Nodes:   nodes,
		Buffers: buffers,
	}, nil
}
