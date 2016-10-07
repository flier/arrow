package arrow

// An abstraction that is used to read from this vector instance.
type VectorAccessor interface {
	// Get the representation of the element at the specified position.
	Get(index int) interface{}

	// Returns the number of values that is stored in this vector.
	Len() int

	// Returns true if the value at the given index is null, false otherwise.
	IsNull(index int) bool
}

// An abstraction that is used to write into this vector instance.
type VectorMutator interface {
	// Resets the mutator to pristine state.
	Reset()
}

// An abstraction that is used to store a sequence of values in an individual column.
type ValueVector interface {
	// Release the underlying ArrowBuf and reset the ValueVector to empty.
	Clear()

	// Return a VectorAccessor that is used to read from this vector instance
	Accessor() VectorAccessor

	// Return a VectorMutator that is used to write to this vector instance
	Mutator() VectorMutator

	// Returns the number of bytes that is used by this vector instance.
	Size() int
}
