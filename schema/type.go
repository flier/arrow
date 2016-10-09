package schema

import (
	"fmt"
	"strconv"

	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
)

type Marshaler interface {
	Marshal(builder *fb.Builder) (fb.UOffsetT, error)
}

type Type interface {
	Marshaler

	Value() int

	String() string
}

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

func (t arrowType) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	switch t.Value() {
	case flatbuf.TypeNull:
		flatbuf.NullStart(builder)
		return flatbuf.NullEnd(builder), nil

	case flatbuf.TypeBinary:
		flatbuf.BinaryStart(builder)
		return flatbuf.BinaryEnd(builder), nil

	case flatbuf.TypeUtf8:
		flatbuf.Utf8Start(builder)
		return flatbuf.Utf8End(builder), nil

	case flatbuf.TypeBool:
		flatbuf.BoolStart(builder)
		return flatbuf.BoolEnd(builder), nil

	case flatbuf.TypeDate:
		flatbuf.DateStart(builder)
		return flatbuf.DateEnd(builder), nil

	case flatbuf.TypeTime:
		flatbuf.TimeStart(builder)
		return flatbuf.TimeEnd(builder), nil

	case flatbuf.TypeList:
		flatbuf.ListStart(builder)
		return flatbuf.ListEnd(builder), nil

	case flatbuf.TypeStruct_:
		flatbuf.Struct_Start(builder)
		return flatbuf.Struct_End(builder), nil
	}

	return 0, fmt.Errorf("unsupport type, %s", t)
}

type Int struct {
	arrowType

	BitWidth int
	Signed   bool
}

func NewInt(bitWidth int, signed bool) *Int {
	return &Int{flatbuf.TypeInt, bitWidth, signed}
}

func (i *Int) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.IntStart(builder)
	flatbuf.IntAddBitWidth(builder, int32(i.BitWidth))

	var signed byte

	if i.Signed {
		signed = 1
	}

	flatbuf.IntAddIsSigned(builder, signed)
	return flatbuf.IntEnd(builder), nil
}

type Precision int16

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

func (f *FloatingPoint) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.FloatingPointStart(builder)
	flatbuf.FloatingPointAddPrecision(builder, int16(f.Precision))
	return flatbuf.FloatingPointEnd(builder), nil
}

type Decimal struct {
	arrowType

	Precision Precision
	Scale     int
}

func NewDecimal(precision Precision, scale int) *Decimal {
	return &Decimal{flatbuf.TypeDecimal, precision, scale}
}

func (d *Decimal) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.DecimalStart(builder)
	flatbuf.DecimalAddPrecision(builder, int32(d.Precision))
	flatbuf.DecimalAddScale(builder, int32(d.Scale))
	return flatbuf.DecimalEnd(builder), nil
}

type TimeUnit int16

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

func (t *Timestamp) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.TimestampStart(builder)
	flatbuf.TimestampAddUnit(builder, int16(t.Unit))
	return flatbuf.TimestampEnd(builder), nil
}

type IntervalUnit int16

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

func (i *Interval) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	flatbuf.IntervalStart(builder)
	flatbuf.IntervalAddUnit(builder, int16(i.Unit))
	return flatbuf.IntervalEnd(builder), nil
}

type UnionMode int16

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

func (u *Union) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	var typeIdOffset fb.UOffsetT

	if len(u.TypeIDs) > 0 {
		flatbuf.UnionStartTypeIdsVector(builder, len(u.TypeIDs))

		for _, typeID := range u.TypeIDs {
			builder.PrependInt32(int32(typeID))
		}

		typeIdOffset = builder.EndVector(len(u.TypeIDs))
	}

	flatbuf.UnionStart(builder)
	flatbuf.UnionAddMode(builder, int16(u.Mode))

	if len(u.TypeIDs) > 0 {
		flatbuf.UnionAddTypeIds(builder, typeIdOffset)
	}

	return flatbuf.UnionEnd(builder), nil
}
