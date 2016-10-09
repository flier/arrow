package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema/vector"
)

var (
	errTooSmall      = errors.New("buffer too small")
	errBadMagic      = errors.New("missing magic number")
	errInvalidFooter = errors.New("invalid footer")
	errInvalidBlock  = errors.New("invalid block")
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
		return nil, errTooSmall
	}

	buf := make([]byte, 4+len(Magic))
	off := r.in.Size() - int64(len(buf))

	if _, err := r.in.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("fail to read magic, %s", err)
	}

	if string(buf[4:]) != Magic {
		return nil, errBadMagic
	}

	footerLength := int64(int32(binary.LittleEndian.Uint32(buf[:4])))

	if footerLength <= 0 || footerLength+minSize > r.in.Size() {
		return nil, errInvalidFooter
	}

	off -= footerLength

	buf = make([]byte, footerLength)

	if _, err := r.in.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("fail to read footer, %s", err)
	}

	if footer, err := UnmarshalFooter(flatbuf.GetRootAsFooter(buf, 0)); err != nil {
		return nil, fmt.Errorf("fail to parse footer, %s", err)
	} else {
		r.footer = footer
	}

	return r.footer, nil
}

// TODO: read dictionaries

func (r *Reader) ReadRecordBatch(block *Block) (*vector.RecordBatch, error) {
	bufSize := int64(block.MetadataLen) + block.BodyLen

	if bufSize < 0 {
		return nil, errInvalidBlock
	}

	buf := make([]byte, bufSize)

	if _, err := r.in.ReadAt(buf, block.Offset); err != nil {
		return nil, fmt.Errorf("fail to read records, %s", err)
	}

	batch := flatbuf.GetRootAsRecordBatch(buf[:block.MetadataLen], 0)
	body := buf[block.MetadataLen:]

	return vector.UnmarshalRecordBatch(batch, body)
}
