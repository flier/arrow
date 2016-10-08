// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// time from the Unix epoch, 00:00:00.000 on 1 January 1970, UTC.
type Timestamp struct {
	_tab flatbuffers.Table
}

func GetRootAsTimestamp(buf []byte, offset flatbuffers.UOffsetT) *Timestamp {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Timestamp{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Timestamp) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Timestamp) Unit() int16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Timestamp) MutateUnit(n int16) bool {
	return rcv._tab.MutateInt16Slot(4, n)
}

func TimestampStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func TimestampAddUnit(builder *flatbuffers.Builder, unit int16) {
	builder.PrependInt16Slot(0, unit, 0)
}
func TimestampEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}