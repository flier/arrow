package file

import (
	"io"
)

type Writer struct {
	out io.WriteSeeker
}
