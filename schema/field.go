package schema

import (
	"fmt"
	"unsafe"

	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema/vector"
)

type Field struct {
	Name     string
	Nullable bool
	Type     Type
	Children []*Field
	Layout   *vector.TypeLayout
}

func UnmarshalField(field *flatbuf.Field) (*Field, error) {
	tp, err := getTypeForField(field)

	if err != nil {
		return nil, err
	}

	var layouts []*vector.VectorLayout
	var layout flatbuf.VectorLayout

	for i := 0; i < field.LayoutLength(); i++ {
		if field.Layout(&layout, i) {
			l, err := vector.UnmarshalVectorLayout(&layout)

			if err != nil {
				return nil, err
			}

			layouts = append(layouts, l)
		}
	}

	var children []*Field
	var child flatbuf.Field

	for i := 0; i < field.ChildrenLength(); i++ {
		if field.Children(&child, i) {
			f, err := UnmarshalField(&child)

			if err != nil {
				return nil, err
			}

			children = append(children, f)
		}
	}

	return &Field{
		Name:     string(field.Name()),
		Nullable: field.Nullable() != 0,
		Type:     tp,
		Children: children,
		Layout:   &vector.TypeLayout{layouts},
	}, nil
}

func getTypeForField(field *flatbuf.Field) (Type, error) {
	switch field.TypeType() {
	case flatbuf.TypeNull:
		return Null, nil

	case flatbuf.TypeInt:
		var i flatbuf.Int

		if field.Type((*fb.Table)(unsafe.Pointer(&i))) {
			return NewInt(int(i.BitWidth()), i.IsSigned() != 0), nil
		}

	case flatbuf.TypeFloatingPoint:
		var f flatbuf.FloatingPoint

		if field.Type((*fb.Table)(unsafe.Pointer(&f))) {
			return NewFloatingPoint(Precision(f.Precision())), nil
		}

	case flatbuf.TypeDecimal:
		var d flatbuf.Decimal

		if field.Type((*fb.Table)(unsafe.Pointer(&d))) {
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

		if field.Type((*fb.Table)(unsafe.Pointer(&ts))) {
			return NewTimeStamp(TimeUnit(ts.Unit())), nil
		}

	case flatbuf.TypeInterval:
		var i flatbuf.Interval

		if field.Type((*fb.Table)(unsafe.Pointer(&i))) {
			return NewInterval(IntervalUnit(i.Unit())), nil
		}

	case flatbuf.TypeList:
		return List, nil

	case flatbuf.TypeStruct_:
		return Struct, nil

	case flatbuf.TypeUnion:
		var u flatbuf.Union

		if field.Type((*fb.Table)(unsafe.Pointer(&u))) {
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

func (f *Field) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	var nameOffset fb.UOffsetT

	if len(f.Name) > 0 {
		nameOffset = builder.CreateString(f.Name)
	}

	typeOffset, err := f.Type.Marshal(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal type, %s", err)
	}

	childrenOffset, err := f.marshalChildren(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal children, %s", err)
	}

	layoutOffset, err := f.marshalLayout(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal layout, %s", err)
	}

	flatbuf.FieldStart(builder)

	if len(f.Name) > 0 {
		flatbuf.FieldAddName(builder, nameOffset)
	}

	var nullable byte

	if f.Nullable {
		nullable = 1
	}

	flatbuf.FieldAddNullable(builder, nullable)
	flatbuf.FieldAddTypeType(builder, byte(f.Type.Value()))
	flatbuf.FieldAddType(builder, typeOffset)
	flatbuf.FieldAddChildren(builder, childrenOffset)
	flatbuf.FieldAddLayout(builder, layoutOffset)

	return flatbuf.FieldEnd(builder), nil
}

func (f *Field) marshalChildren(builder *fb.Builder) (fb.UOffsetT, error) {
	var childOffsets []fb.UOffsetT

	for _, child := range f.Children {
		off, err := child.Marshal(builder)

		if err != nil {
			return 0, err
		}

		childOffsets = append(childOffsets, off)
	}

	flatbuf.FieldStartChildrenVector(builder, len(childOffsets))

	for _, off := range childOffsets {
		builder.PrependUOffsetT(off)
	}

	return builder.EndVector(len(childOffsets)), nil
}

func (f *Field) marshalLayout(builder *fb.Builder) (fb.UOffsetT, error) {
	var bufferOffsets []fb.UOffsetT

	for _, layout := range f.Layout.Vectors {
		off, err := layout.Marshal(builder)

		if err != nil {
			return 0, err
		}

		bufferOffsets = append(bufferOffsets, off)
	}

	flatbuf.FieldStartLayoutVector(builder, len(bufferOffsets))

	for _, off := range bufferOffsets {
		builder.PrependUOffsetT(off)
	}

	return builder.EndVector(len(bufferOffsets)), nil
}
