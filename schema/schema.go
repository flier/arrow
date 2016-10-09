package schema

import (
	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
)

type Schema struct {
	Fields []*Field
}

func UnmarshalSchema(schema *flatbuf.Schema) (*Schema, error) {
	var fields []*Field
	var field flatbuf.Field

	for i := 0; i < schema.FieldsLength(); i++ {
		if schema.Fields(&field, i) {
			f, err := UnmarshalField(&field)

			if err != nil {
				return nil, err
			}

			fields = append(fields, f)
		}
	}

	return &Schema{fields}, nil
}

func (s *Schema) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	var offsets []fb.UOffsetT

	for _, field := range s.Fields {
		off, err := field.Marshal(builder)

		if err != nil {
			return off, err
		}

		offsets = append(offsets, off)
	}

	flatbuf.SchemaStartFieldsVector(builder, len(offsets))

	for _, off := range offsets {
		builder.PrependUOffsetT(off)
	}

	fieldsOffset := builder.EndVector(len(offsets))

	flatbuf.SchemaStart(builder)
	flatbuf.SchemaAddFields(builder, fieldsOffset)
	return flatbuf.SchemaEnd(builder), nil
}
