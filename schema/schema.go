package schema

import (
	"fmt"
	"unsafe"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/memory"
)

type Schema struct {
	Fields []*Field
}

func NewSchema(schema *flatbuf.Schema) (*Schema, error) {
	var fields []*Field
	var field flatbuf.Field

	for i := 0; i < schema.FieldsLength(); i++ {
		if schema.Fields(&field, i) {
			f, err := NewField(&field)

			if err != nil {
				return nil, err
			}

			fields = append(fields, f)
		}
	}

	return &Schema{
		Fields: fields,
	}, nil
}

type Field struct {
	Name     string
	Nullable bool
	Type     Type
	Children []*Field
	Layout   *VectorLayout
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
		return Struct, nil

	case flatbuf.TypeFloatingPoint:
		var fp flatbuf.FloatingPoint

		if field.Type((*flatbuffers.Table)(unsafe.Pointer(&fp))) {
			return NewFloatingPoint(int(fp.Precision())), nil
		}

	case flatbuf.TypeBinary:
	case flatbuf.TypeUtf8:
		return Utf8, nil

	case flatbuf.TypeBool:
		return Bool, nil

	case flatbuf.TypeDecimal:
	case flatbuf.TypeDate:
		return Date, nil

	case flatbuf.TypeTime:
		return Time, nil

	case flatbuf.TypeTimestamp:
	case flatbuf.TypeInterval:
	case flatbuf.TypeList:
		return List, nil

	case flatbuf.TypeStruct_:
		return Struct, nil

	case flatbuf.TypeUnion:
	default:
		return nil, fmt.Errorf("unsupported type, %d", field.TypeType())
	}

	return nil, fmt.Errorf("fail to parse type, %d", field.TypeType())
}

type FieldNode struct {
	Length    int
	NullCount int
}

type RecordBatch struct {
	Length  int
	Nodes   []*FieldNode
	Buffers []*memory.Buffer
}
