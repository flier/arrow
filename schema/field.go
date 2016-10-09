package schema

import (
	"fmt"
	"unsafe"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema/vector"
)

type Field struct {
	Name     string
	Nullable bool
	Type     Type
	Children []*Field
	Layout   *vector.Layout
}

func NewField(field *flatbuf.Field) (*Field, error) {
	tp, err := getTypeForField(field)

	if err != nil {
		return nil, err
	}

	return &Field{
		Name:     string(field.Name()),
		Nullable: field.Nullable() != 0,
		Type:     tp,
	}, nil
}

func getTypeForField(field *flatbuf.Field) (Type, error) {
	switch field.TypeType() {
	case flatbuf.TypeNull:
		return Null, nil

	case flatbuf.TypeInt:
		var i flatbuf.Int

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&i))) {
			return NewInt(int(i.BitWidth()), i.IsSigned() != 0), nil
		}

	case flatbuf.TypeFloatingPoint:
		var f flatbuf.FloatingPoint

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&f))) {
			return NewFloatingPoint(Precision(f.Precision())), nil
		}

	case flatbuf.TypeDecimal:
		var d flatbuf.Decimal

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&d))) {
			return NewDecimal(Precision(d.Precision()), int(d.Scale())), nil
		}

	case flatbuf.TypeBinary:
		return Binary, nil

	case flatbuf.TypeUtf8:
		return Utf8, nil

	case flatbuf.TypeBool:
		return Bool, nil

	case flatbuf.TypeDate:
		return Date, nil

	case flatbuf.TypeTime:
		return Time, nil

	case flatbuf.TypeTimestamp:
		var ts flatbuf.Timestamp

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&ts))) {
			return NewTimeStamp(TimeUnit(ts.Unit())), nil
		}

	case flatbuf.TypeInterval:
		var i flatbuf.Interval

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&i))) {
			return NewInterval(IntervalUnit(i.Unit())), nil
		}

	case flatbuf.TypeList:
		return List, nil

	case flatbuf.TypeStruct_:
		return Struct, nil

	case flatbuf.TypeUnion:
		var u flatbuf.Union

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&u))) {
			var typeIDs []int

			for i := 0; i < u.TypeIdsLength(); i++ {
				typeIDs = append(typeIDs, int(u.TypeIds(i)))
			}

			return NewUnion(UnionMode(u.Mode()), typeIDs), nil
		}

	default:
		return nil, fmt.Errorf("unsupported type, %s", arrowType(field.TypeType()))
	}

	return nil, fmt.Errorf("fail to parse type, %s", arrowType(field.TypeType()))
}
