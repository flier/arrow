// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Binary struct {
	_tab flatbuffers.Table
}

func GetRootAsBinary(buf []byte, offset flatbuffers.UOffsetT) *Binary {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Binary{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Binary) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func BinaryStart(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func BinaryEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
