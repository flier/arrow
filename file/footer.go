package file

import (
	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema"
)

type Footer struct {
	Schema        *schema.Schema
	Dictionaries  []*Block
	RecordBatches []*Block
}

func LoadFooter(footer *flatbuf.Footer) (*Footer, error) {
	return &Footer{}, nil
}
