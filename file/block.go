package file

type Block struct {
	Offset      int64
	MetadataLen int
	BodyLen     int
}
