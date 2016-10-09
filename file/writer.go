package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/schema"
	"github.com/flier/arrow/schema/vector"
)

var (
	errInvalidRecordBatch = errors.New("invalid recordBatch")
)

type Writer struct {
	out           io.WriteSeeker
	schema        *schema.Schema
	recordBatches []*Block
	pos           int64
}

// align on 8 byte boundaries
func (w *Writer) align() error {
	if w.pos%8 != 0 {
		return w.writeZeros(8 - (w.pos % 8))
	}

	return nil
}

func (w *Writer) WriteRecordBatch(batch *vector.RecordBatch) error {
	if w.pos == 0 {
		if err := w.writeMagic(); err != nil {
			return err
		}
	}

	if err := w.align(); err != nil {
		return err
	}

	// write metadata header

	off := w.pos

	if err := w.Marshal(batch); err != nil {
		return err
	}

	if err := w.align(); err != nil {
		return err
	}

	// write body

	bodyOffset := w.pos

	if len(batch.Buffers) != len(batch.Layouts) {
		return errors.New("the layout does not match buffers")
	}

	for i, buffer := range batch.Buffers {
		layout := batch.Layouts[i]

		startPosition := bodyOffset + layout.Offset

		if err := w.writeZeros(startPosition - w.pos); err != nil {
			return fmt.Errorf("fail to write pad bytes, %s", err)
		}

		if err := w.Write(buffer.Bytes()); err != nil {
			return fmt.Errorf("fail to write buffer, %s", err)
		}

		if w.pos != startPosition+layout.Size {
			return fmt.Errorf("wrong buffer size, %d", layout.Size)
		}
	}

	metadataLength := bodyOffset - off

	if metadataLength <= 0 {
		return errInvalidRecordBatch
	}

	bodyLength := w.pos - bodyOffset

	w.recordBatches = append(w.recordBatches, &Block{off, int(metadataLength), bodyLength})

	return nil
}

func (w *Writer) Marshal(obj schema.Marshaler) error {
	builder := fb.NewBuilder(1024)

	off, err := obj.Marshal(builder)

	if err != nil {
		return err
	}

	builder.Finish(off)

	return w.Write(builder.FinishedBytes())
}

func (w *Writer) Write(buf []byte) error {
	n, err := w.out.Write(buf)

	if err == nil {
		w.pos += int64(n)
	}

	return err
}

func (w *Writer) Flush() error {
	footerStart := w.pos

	if err := w.writeFooter(); err != nil {
		return err
	}

	footerLength := w.pos - footerStart

	if footerLength <= 0 {
		return errInvalidFooter
	}

	buf := make([]byte, 4)

	binary.LittleEndian.PutUint32(buf, uint32(footerLength))

	if err := w.Write(buf); err != nil {
		return fmt.Errorf("fail to write footer, %s", err)
	}

	return w.writeMagic()
}

func (w *Writer) writeMagic() error {
	return w.Write([]byte(Magic))
}

func (w *Writer) writeZeros(n int64) error {
	if n <= 0 {
		return nil
	}

	return w.Write(make([]byte, n))
}

func (w *Writer) writeFooter() error {
	return w.Marshal(&Footer{
		Schema:        w.schema,
		RecordBatches: w.recordBatches,
	})
}
