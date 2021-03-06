// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type FloatingPoint struct {
	_tab flatbuffers.Table
}

func GetRootAsFloatingPoint(buf []byte, offset flatbuffers.UOffsetT) *FloatingPoint {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &FloatingPoint{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *FloatingPoint) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *FloatingPoint) Precision() int16 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt16(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *FloatingPoint) MutatePrecision(n int16) bool {
	return rcv._tab.MutateInt16Slot(4, n)
}

func FloatingPointStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func FloatingPointAddPrecision(builder *flatbuffers.Builder, precision int16) {
	builder.PrependInt16Slot(0, precision, 0)
}
func FloatingPointEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
