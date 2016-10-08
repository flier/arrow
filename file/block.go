package file

import (
	"github.com/flier/arrow/flatbuf"
)

type Block struct {
	Offset      int64
	MetadataLen int
	BodyLen     int
}

func NewBlock(block *flatbuf.Block) *Block {
	return &Block{
		Offset:      block.Offset(),
		MetadataLen: int(block.MetaDataLength()),
		BodyLen:     int(block.BodyLength()),
	}
}
