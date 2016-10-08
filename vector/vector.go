package vector

import (
	"errors"

	"github.com/flier/arrow/memory"
)

//go:generate gen

// +gen vector:"Value,Accessor,Mutator"
type TinyInt int8

// +gen vector:"Value,Accessor,Mutator"
type SmallInt int16

// +gen vector:"Value,Accessor,Mutator"
type Int int32

// +gen vector:"Value,Accessor,Mutator"
type BigInt int64

// +gen vector:"Value,Accessor,Mutator"
type UInt1 uint8

// +gen vector:"Value,Accessor,Mutator"
type UInt2 uint16

// +gen vector:"Value,Accessor,Mutator"
type UInt4 uint32

// +gen vector:"Value,Accessor,Mutator"
type UInt8 uint64

// +gen vector:"Value,Accessor,Mutator"
type Float4 float32

// +gen vector:"Value,Accessor,Mutator"
type Float8 float64

// +gen vector:"Time,Accessor,Mutator"
type Date int64

// +gen vector:"Time,Accessor,Mutator"
type Time int32

// +gen vector:"Time,Accessor,Mutator"
type TimeStamp int64

// +gen vector:"Duration,Accessor,Mutator"
type IntervalDay int64

// +gen vector:"Duration,Accessor,Mutator"
type IntervalYear int32

// +gen vector:"Value,Accessor,Mutator"
type VarChar string

// +gen vector:"Value,Accessor,Mutator"
type VarBinary []byte

type Bit bool

var (
	errOutOfRange = errors.New("out of range")
)

type BaseValueVector struct {
	data *memory.Buffer
}

func (v *BaseValueVector) BufferSize() int {
	return v.data.Len()
}

func (v *BaseValueVector) Buffer() *memory.Buffer {
	return v.data
}
