package schema

import (
	"github.com/flier/arrow/flatbuf"
)

var (
	Null      = ArrowType(flatbuf.TypeNull)
	Int       = ArrowType(flatbuf.TypeInt)
	Binary    = ArrowType(flatbuf.TypeBinary)
	Utf8      = ArrowType(flatbuf.TypeUtf8)
	Bool      = ArrowType(flatbuf.TypeBool)
	Decimal   = ArrowType(flatbuf.TypeDecimal)
	Date      = ArrowType(flatbuf.TypeDate)
	Time      = ArrowType(flatbuf.TypeTime)
	Timestamp = ArrowType(flatbuf.TypeTimestamp)
	Interval  = ArrowType(flatbuf.TypeInterval)
	List      = ArrowType(flatbuf.TypeList)
	Struct    = ArrowType(flatbuf.TypeStruct_)
	Union     = ArrowType(flatbuf.TypeUnion)
)

type ArrowType int

func (t ArrowType) Type() ArrowType { return t }

func (t ArrowType) Name() string { return flatbuf.EnumNamesType[int(t)] }

type Type interface {
	Type() ArrowType

	Name() string
}

type FloatingPoint struct {
	ArrowType

	Precision int
}

func NewFloatingPoint(precision int) *FloatingPoint {
	return &FloatingPoint{flatbuf.TypeFloatingPoint, precision}
}
