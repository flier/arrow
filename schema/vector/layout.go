package vector

import (
	"errors"
	"strconv"

	"github.com/flier/arrow/flatbuf"
)

type VectorType int

const (
	Validity VectorType = flatbuf.VectorTypeVALIDITY
	Offset   VectorType = flatbuf.VectorTypeOFFSET
	Type     VectorType = flatbuf.VectorTypeTYPE
	Data     VectorType = flatbuf.VectorTypeDATA
)

func (t VectorType) String() string {
	switch t {
	case Validity:
		return "VALIDITY"
	case Offset:
		return "OFFSET"
	case Type:
		return "TYPE"
	case Data:
		return "DATA"
	default:
		return strconv.FormatInt(int64(t), 10)
	}
}

var (
	ValidityVector = &Layout{Validity, 1}
	OffsetVector   = &Layout{Offset, 32}
	TypeVector     = &Layout{Type, 32}
	BooleanVector  = &Layout{Data, 1}
	Value64Vector  = &Layout{Data, 64}
	Value32Vector  = &Layout{Data, 32}
	Value16Vector  = &Layout{Data, 16}
	Value8Vector   = &Layout{Data, 8}
	ByteVector     = Value8Vector
)

type Layout struct {
	Type     VectorType
	BitWidth int
}

func DataVector(bitWidth int) (*Layout, error) {
	switch bitWidth {
	case 8:
		return Value8Vector, nil
	case 16:
		return Value16Vector, nil
	case 32:
		return Value32Vector, nil
	case 64:
		return Value64Vector, nil
	default:
		return nil, errors.New("only 8, 16, 32, or 64 bits supported")
	}
}
