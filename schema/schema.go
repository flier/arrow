package schema

import (
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
