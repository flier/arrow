package file

import (
	"fmt"

	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
	"github.com/flier/arrow/schema"
)

const (
	Magic = "ARROW1"
)

type Footer struct {
	Schema        *schema.Schema
	Dictionaries  []*Block
	RecordBatches []*Block
}

func UnmarshalFooter(footer *flatbuf.Footer) (*Footer, error) {
	var dictionaries []*Block
	var recordBatches []*Block
	var block flatbuf.Block

	for i := 0; i < footer.DictionariesLength(); i++ {
		if footer.Dictionaries(&block, i) {
			dictionaries = append(dictionaries, UnmarshalBlock(&block))
		}
	}

	for i := 0; i < footer.RecordBatchesLength(); i++ {
		if footer.RecordBatches(&block, i) {
			recordBatches = append(recordBatches, UnmarshalBlock(&block))
		}
	}

	s, err := schema.UnmarshalSchema(footer.Schema(nil))

	if err != nil {
		return nil, fmt.Errorf("fail to parse schema, %s", err)
	}

	return &Footer{
		Schema:        s,
		Dictionaries:  dictionaries,
		RecordBatches: recordBatches,
	}, nil
}

func (f *Footer) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	schemaOffset, err := f.Schema.Marshal(builder)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal schema, %s", err)
	}

	flatbuf.FooterStartDictionariesVector(builder, len(f.Dictionaries))
	dicsOffset, err := f.marshalBlocks(builder, f.Dictionaries)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal dictionaries, %s", err)
	}

	flatbuf.FooterStartRecordBatchesVector(builder, len(f.RecordBatches))
	rbsOffset, err := f.marshalBlocks(builder, f.RecordBatches)

	if err != nil {
		return 0, fmt.Errorf("fail to marshal recordBatches, %s", err)
	}

	flatbuf.FooterStart(builder)
	flatbuf.FooterAddSchema(builder, schemaOffset)
	flatbuf.FooterAddDictionaries(builder, dicsOffset)
	flatbuf.FooterAddRecordBatches(builder, rbsOffset)
	return flatbuf.FooterEnd(builder), nil
}

func (f *Footer) marshalBlocks(builder *fb.Builder, blocks []*Block) (fb.UOffsetT, error) {
	for _, block := range blocks {
		if _, err := block.Marshal(builder); err != nil {
			return 0, err
		}
	}

	return builder.EndVector(len(blocks)), nil
}
