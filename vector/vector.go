package vector

import (
	"time"
)

//go:generate gen

// +gen vector:""
type TinyInt int8

// +gen vector:""
type SmallInt int16

// +gen vector:""
type Int int32

// +gen vector:""
type BigInt int64

// +gen vector:""
type UInt1 uint8

// +gen vector:""
type UInt2 uint16

// +gen vector:""
type UInt4 uint32

// +gen vector:""
type UInt8 uint64

// +gen vector:""
type Float4 float32

// +gen vector:""
type Float8 float64

type Date time.Time
type Time time.Time
type TimeStamp int64
type IntervalDay time.Duration
type IntervalYear time.Duration
type VarChar string
type VarBinary []byte
type Bit bool
