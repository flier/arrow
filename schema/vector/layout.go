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
	ValidityVector = &VectorLayout{Validity, 1}
	OffsetVector   = &VectorLayout{Offset, 32}
	TypeVector     = &VectorLayout{Type, 32}
	BooleanVector  = &VectorLayout{Data, 1}
	Value64Vector  = &VectorLayout{Data, 64}
	Value32Vector  = &VectorLayout{Data, 32}
	Value16Vector  = &VectorLayout{Data, 16}
	Value8Vector   = &VectorLayout{Data, 8}
	ByteVector     = Value8Vector
)

type VectorLayout struct {
	Type     VectorType
	BitWidth int
}

func UnmarshalVectorLayout(layout *flatbuf.VectorLayout) (*VectorLayout, error) {
	switch layout.Type() {
	case flatbuf.VectorTypeVALIDITY:
		return ValidityVector, nil
	case flatbuf.VectorTypeOFFSET:
		return OffsetVector, nil
	case flatbuf.VectorTypeTYPE:
		return TypeVector, nil
	case flatbuf.VectorTypeDATA:
		return DataVector(int(layout.BitWidth()))
	}

	return &VectorLayout{
		Type:     VectorType(layout.Type()),
		BitWidth: int(layout.BitWidth()),
	}, nil
}

func DataVector(bitWidth int) (*VectorLayout, error) {
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

type TypeLayout struct {
	Vectors []*VectorLayout
}
