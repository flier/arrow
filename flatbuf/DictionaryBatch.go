// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// ----------------------------------------------------------------------
/// For sending dictionary encoding information. Any Field can be
/// dictionary-encoded, but in this case none of its children may be
/// dictionary-encoded.
/// There is one dictionary batch per dictionary
///
type DictionaryBatch struct {
	_tab flatbuffers.Table
}

func GetRootAsDictionaryBatch(buf []byte, offset flatbuffers.UOffsetT) *DictionaryBatch {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &DictionaryBatch{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *DictionaryBatch) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *DictionaryBatch) Id() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DictionaryBatch) MutateId(n int64) bool {
	return rcv._tab.MutateInt64Slot(4, n)
}

func (rcv *DictionaryBatch) Data(obj *RecordBatch) *RecordBatch {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(RecordBatch)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func DictionaryBatchStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func DictionaryBatchAddId(builder *flatbuffers.Builder, id int64) {
	builder.PrependInt64Slot(0, id, 0)
}
func DictionaryBatchAddData(builder *flatbuffers.Builder, data flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(data), 0)
}
func DictionaryBatchEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
