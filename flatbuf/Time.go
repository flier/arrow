// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Time struct {
	_tab flatbuffers.Table
}

func GetRootAsTime(buf []byte, offset flatbuffers.UOffsetT) *Time {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Time{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Time) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func TimeStart(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func TimeEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
