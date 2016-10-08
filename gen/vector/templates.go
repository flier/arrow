package vector

import (
	"github.com/clipperhouse/typewriter"
)

type model struct {
	typewriter.TagValue

	Type      typewriter.Type
	TypeParam typewriter.Type
}

func (m *model) Name() string {
	return m.Type.Name + "Vector"
}

var templates = typewriter.TemplateSlice{
	vector,

	value,
	time,
	duration,

	accessor,
	mutator,
}
