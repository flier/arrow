package file

import (
	"fmt"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema"
)

type Footer struct {
	Schema        *schema.Schema
	Dictionaries  []*Block
	RecordBatches []*Block
}

func LoadFooter(footer *flatbuf.Footer) (*Footer, error) {
	var dictionaries []*Block
	var recordBatches []*Block
	var block flatbuf.Block

	for i := 0; i < footer.DictionariesLength(); i++ {
		if footer.Dictionaries(&block, i) {
			dictionaries = append(dictionaries, NewBlock(&block))
		}
	}

	for i := 0; i < footer.RecordBatchesLength(); i++ {
		if footer.RecordBatches(&block, i) {
			recordBatches = append(recordBatches, NewBlock(&block))
		}
	}

	schema, err := schema.NewSchema(footer.Schema(nil))

	if err != nil {
		return nil, fmt.Errorf("fail to parse schema, %s", err)
	}

	return &Footer{
		Schema:        schema,
		Dictionaries:  dictionaries,
		RecordBatches: recordBatches,
	}, nil
}
