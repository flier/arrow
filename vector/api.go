package vector

// An abstraction that is used to read from this vector instance.
type Accessor interface {
	// Get the representation of the element at the specified position.
	Get(index int) (interface{}, error)

	// Returns the number of values that is stored in this vector.
	ValueCount() int

	// Returns true if the value at the given index is null, false otherwise.
	IsNull(index int) bool
}

// An abstraction that is used to write into this vector instance.
type Mutator interface {
	// Sets the number of values that is stored in this vector to the given value count.
	SetValueCount(valueCount int)
}

// An abstraction that is used to store a sequence of values in an individual column.
type ValueVector interface {
	// Returns the maximum number of values that can be stored in this vector instance.
	ValueCapacity() int

	// Return a VectorAccessor that is used to read from this vector instance
	Accessor() Accessor

	// Return a VectorMutator that is used to write to this vector instance
	Mutator() Mutator

	// Returns the number of bytes that is used by this vector instance.
	BufferSize() int
}
