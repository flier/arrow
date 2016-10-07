package vector

import (
	"github.com/clipperhouse/typewriter"
)

type model struct {
	typewriter.TagValue

	Type          typewriter.Type
	TypeParameter typewriter.Type
}

func (m *model) Name() string {
	return m.Type.Name + "Vector"
}

var templates = typewriter.TemplateSlice{
	vector,
}
