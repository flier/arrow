// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// A Struct_ in the flatbuffer metadata is the same as an Arrow Struct
/// (according to the physical memory layout). We used Struct_ here as
/// Struct is a reserved word in Flatbuffers
type Struct_ struct {
	_tab flatbuffers.Table
}

func GetRootAsStruct_(buf []byte, offset flatbuffers.UOffsetT) *Struct_ {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Struct_{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Struct_) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func Struct_Start(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func Struct_End(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}