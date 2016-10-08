package vector

import (
	"math"
)

type BitVector struct {
	*BaseValueVector

	valueCount int
}

func (v *BitVector) sizeFromCount(valueCount int) int {
	return int(math.Ceil(float64(valueCount) / 8.0))
}

func (v *BitVector) ValueCapacity() int {
	return v.data.Cap() * 8
}

func (v *BitVector) Accessor() Accessor { return v }

func (v *BitVector) Mutator() Mutator { return v }

// implement Accessor

func (v *BitVector) GetBit(index int) (Bit, error) {
	byteIndex := index >> 3

	if byteIndex >= v.data.Len() {
		return false, errOutOfRange
	}

	bitIndex := index & 7
	bitMask := byte(1 << uint(bitIndex))
	b := v.data.Bytes()[byteIndex]

	return (b & bitMask) == bitMask, nil
}

func (v *BitVector) Get(index int) (interface{}, error) { return v.GetBit(index) }

func (v *BitVector) ValueCount() int { return v.valueCount }

func (v *BitVector) IsNull(index int) bool { return false }

// implement Mutator

func (v *BitVector) SetBit(index int, value Bit) error {
	byteIndex := index >> 3

	if byteIndex >= v.data.Len() {
		return errOutOfRange
	}

	bitIndex := index & 7
	bitMask := byte(1 << uint(bitIndex))
	b := v.data.Bytes()[byteIndex]

	if value {
		b |= bitMask
	} else {
		b -= (bitMask & b)
	}

	v.data.Bytes()[byteIndex] = b

	return nil
}

func (v *BitVector) SetValueCount(valueCount int) {
	v.valueCount = valueCount

	idx := v.sizeFromCount(valueCount)

	if valueCount > v.ValueCapacity() {
		v.data.Grow(idx)
	}
}

func (v *BitVector) Reset() {

}
