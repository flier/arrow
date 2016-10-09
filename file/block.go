package file

import (
	fb "github.com/google/flatbuffers/go"

	"github.com/flier/arrow/flatbuf"
)

type Block struct {
	Offset      int64
	MetadataLen int
	BodyLen     int64
}

func UnmarshalBlock(block *flatbuf.Block) *Block {
	return &Block{
		Offset:      block.Offset(),
		MetadataLen: int(block.MetaDataLength()),
		BodyLen:     block.BodyLength(),
	}
}

func (b *Block) Marshal(builder *fb.Builder) (fb.UOffsetT, error) {
	return flatbuf.CreateBlock(builder, b.Offset, int32(b.MetadataLen), b.BodyLen), nil
}
