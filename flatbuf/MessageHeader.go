// automatically generated by the FlatBuffers compiler, do not modify

package flatbuf

/// ----------------------------------------------------------------------
/// The root Message type
/// This union enables us to easily send different message types without
/// redundant storage, and in the future we can easily add new message types.
const (
	MessageHeaderNONE = 0
	MessageHeaderSchema = 1
	MessageHeaderDictionaryBatch = 2
	MessageHeaderRecordBatch = 3
)

var EnumNamesMessageHeader = map[int]string{
	MessageHeaderNONE:"NONE",
	MessageHeaderSchema:"Schema",
	MessageHeaderDictionaryBatch:"DictionaryBatch",
	MessageHeaderRecordBatch:"RecordBatch",
}
