package schema

import (
	"strconv"

	"github.com/flier/arrow/flatbuf"
)

var (
	Null   = arrowType(flatbuf.TypeNull)
	Binary = arrowType(flatbuf.TypeBinary)
	Utf8   = arrowType(flatbuf.TypeUtf8)
	Bool   = arrowType(flatbuf.TypeBool)
	Date   = arrowType(flatbuf.TypeDate)
	Time   = arrowType(flatbuf.TypeTime)
	List   = arrowType(flatbuf.TypeList)
	Struct = arrowType(flatbuf.TypeStruct_)
)

type arrowType int

func (t arrowType) Value() int { return int(t) }

func (t arrowType) String() string {
	v := t.Value()

	if 0 <= v && v < len(flatbuf.EnumNamesType) {
		return flatbuf.EnumNamesType[v]
	}

	return strconv.FormatInt(int64(v), 10)
}

type Type interface {
	Value() int

	String() string
}

type Int struct {
	arrowType

	BitWidth int
	Signed   bool
}

func NewInt(bitWidth int, signed bool) *Int {
	return &Int{flatbuf.TypeInt, bitWidth, signed}
}

type Precision int

const (
	Half   Precision = flatbuf.PrecisionHALF
	Single Precision = flatbuf.PrecisionSINGLE
	Double Precision = flatbuf.PrecisionDOUBLE
)

func (p Precision) String() string {
	switch p {
	case Half:
		return "HALF"
	case Single:
		return "SINGLE"
	case Double:
		return "DOUBLE"
	default:
		return strconv.FormatInt(int64(p), 10)
	}
}

type FloatingPoint struct {
	arrowType

	Precision Precision
}

func NewFloatingPoint(precision Precision) *FloatingPoint {
	return &FloatingPoint{flatbuf.TypeFloatingPoint, precision}
}

type Decimal struct {
	arrowType

	Precision Precision
	Scale     int
}

func NewDecimal(precision Precision, scale int) *Decimal {
	return &Decimal{flatbuf.TypeDecimal, precision, scale}
}

type TimeUnit int

const (
	Nanosecond  TimeUnit = flatbuf.TimeUnitNANOSECOND
	Microsecond TimeUnit = flatbuf.TimeUnitMICROSECOND
	Millisecond TimeUnit = flatbuf.TimeUnitMILLISECOND
	Second      TimeUnit = flatbuf.TimeUnitSECOND
)

func (u TimeUnit) String() string {
	switch u {
	case Nanosecond:
		return "NANOSECOND"
	case Microsecond:
		return "MICROSECOND"
	case Millisecond:
		return "MILLISECOND"
	case Second:
		return "SECOND"
	default:
		return strconv.FormatInt(int64(u), 10)
	}
}

type Timestamp struct {
	arrowType

	Unit TimeUnit
}

func NewTimeStamp(unit TimeUnit) *Timestamp {
	return &Timestamp{flatbuf.TypeTimestamp, unit}
}

type IntervalUnit int

const (
	YearMonth IntervalUnit = flatbuf.IntervalUnitYEAR_MONTH
	DayTime   IntervalUnit = flatbuf.IntervalUnitDAY_TIME
)

func (u IntervalUnit) String() string {
	switch u {
	case YearMonth:
		return "YEAR_MONTH"
	case DayTime:
		return "DAY_TIME"
	default:
		return strconv.FormatInt(int64(u), 10)
	}
}

type Interval struct {
	arrowType

	Unit IntervalUnit
}

func NewInterval(unit IntervalUnit) *Interval {
	return &Interval{flatbuf.TypeInterval, unit}
}

type UnionMode int

const (
	Sparse UnionMode = flatbuf.UnionModeSparse
	Dense  UnionMode = flatbuf.UnionModeDense
)

func (m UnionMode) String() string {
	switch m {
	case Sparse:
		return "Sparse"
	case Dense:
		return "Dense"
	default:
		return "Unknown"
	}
}

type Union struct {
	arrowType

	Mode    UnionMode
	TypeIDs []int
}

func NewUnion(mode UnionMode, typeIDs []int) *Union {
	return &Union{flatbuf.TypeUnion, mode, typeIDs}
}
