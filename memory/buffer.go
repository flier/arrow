package memory

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

const (
	Day   = 24 * time.Hour
	Month = 30 * Day
)

type Buffer struct {
	*bytes.Buffer

	Order binary.ByteOrder
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{
		Buffer: bytes.NewBuffer(buf),
		Order:  binary.LittleEndian,
	}
}

func (b *Buffer) TinyInt(index int) int8 {
	return int8(b.UInt1(index))
}

func (b *Buffer) SmallInt(index int) int16 {
	return int16(b.UInt2(index))
}

func (b *Buffer) Int(index int) int32 {
	return int32(b.UInt4(index))
}

func (b *Buffer) BigInt(index int) int64 {
	return int64(b.UInt8(index))
}

func (b *Buffer) UInt1(index int) uint8 {
	return b.Bytes()[index]
}

func (b *Buffer) UInt2(index int) uint16 {
	return b.Order.Uint16(b.Bytes()[index*2 : (index+1)*2])
}

func (b *Buffer) UInt4(index int) uint32 {
	return b.Order.Uint32(b.Bytes()[index*4 : (index+1)*4])
}

func (b *Buffer) UInt8(index int) uint64 {
	return b.Order.Uint64(b.Bytes()[index*8 : (index+1)*8])
}

func (b *Buffer) Float4(index int) float32 {
	return math.Float32frombits(b.UInt4(index))
}

func (b *Buffer) Float8(index int) float64 {
	return math.Float64frombits(b.UInt8(index))
}

func (b *Buffer) Date(index int) time.Time {
	ts := b.BigInt(index)

	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
}

func (b *Buffer) Time(index int) time.Time {
	ts := int64(b.Int(index))

	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
}

func (b *Buffer) TimeStamp(index int) time.Time {
	ts := b.BigInt(index)

	return time.Unix(ts/1000, (ts%1000)*int64(time.Millisecond))
}

func (b *Buffer) IntervalDay(index int) time.Duration {
	days := b.Int(index * 2)
	milliseconds := b.Int(index*2 + 1)

	return time.Duration(days)*Day + time.Duration(milliseconds)*time.Millisecond
}

func (b *Buffer) IntervalYear(index int) time.Duration {
	return time.Duration(time.Duration(b.Int(index)) * Month)
}

func (b *Buffer) VarChar(index int) string { return "" }

func (b *Buffer) VarBinary(index int) []byte { return nil }

func (b *Buffer) PutTinyInt(index int, v int8) {
	b.PutUInt1(index, uint8(v))
}

func (b *Buffer) PutSmallInt(index int, v int16) {
	b.PutUInt2(index, uint16(v))
}

func (b *Buffer) PutInt(index int, v int32) {
	b.PutUInt4(index, uint32(v))
}

func (b *Buffer) PutBigInt(index int, v int64) {
	b.PutUInt8(index, uint64(v))
}

func (b *Buffer) PutUInt1(index int, v uint8) {
	b.Bytes()[index] = v
}

func (b *Buffer) PutUInt2(index int, v uint16) {
	b.Order.PutUint16(b.Bytes()[index*2:(index+1)*2], v)
}

func (b *Buffer) PutUInt4(index int, v uint32) {
	b.Order.PutUint32(b.Bytes()[index*4:(index+1)*4], v)
}

func (b *Buffer) PutUInt8(index int, v uint64) {
	b.Order.PutUint64(b.Bytes()[index*8:(index+1)*8], v)
}

func (b *Buffer) PutFloat4(index int, v float32) {
	b.PutUInt4(index, math.Float32bits(v))
}

func (b *Buffer) PutFloat8(index int, v float64) {
	b.PutUInt8(index, math.Float64bits(v))
}

func (b *Buffer) PutDate(index int, v time.Time) {
	b.PutBigInt(index, v.UnixNano()/int64(time.Millisecond))
}

func (b *Buffer) PutTime(index int, v time.Time) {
	b.PutInt(index, int32(v.UnixNano()/int64(time.Millisecond)))
}

func (b *Buffer) PutTimeStamp(index int, v time.Time) {
	b.PutBigInt(index, v.UnixNano()/int64(time.Millisecond))
}

func (b *Buffer) PutIntervalDay(index int, v time.Duration) {
	b.PutInt(index*2, int32(v/Day))
	b.PutInt(index*2+1, int32((v%Day)/time.Millisecond))
}

func (b *Buffer) PutIntervalYear(index int, v time.Duration) {
	b.PutInt(index, int32(v/Month))
}

func (b *Buffer) PutVarChar(index int, v string) {}

func (b *Buffer) PutVarBinary(index int, v []byte) {}
