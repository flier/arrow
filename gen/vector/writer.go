package vector

import (
	"io"

	"github.com/clipperhouse/typewriter"
)

//go:generate gen add github.com/flier/arrow/gen/vector

type VectorWriter struct{}

func init() {
	if err := typewriter.Register(&VectorWriter{}); err != nil {
		panic(err)
	}
}
func (w *VectorWriter) Name() string {
	return "vector"
}

func (w *VectorWriter) Imports(t typewriter.Type) []typewriter.ImportSpec {
	// typewriter uses golang.org/x/tools/imports, depend on that
	return nil
}

func (w *VectorWriter) Write(out io.Writer, typ typewriter.Type) error {
	tag, found := typ.FindTag(w)

	if !found {
		return nil
	}

	// start with the slice template
	tmpl, err := templates.ByTag(typ, tag)

	if err != nil {
		return err
	}

	m := &model{
		Type: typ,
	}

	if err := tmpl.Execute(out, m); err != nil {
		return err
	}

	for _, value := range tag.Values {
		var tp typewriter.Type

		if len(value.TypeParameters) > 0 {
			tp = value.TypeParameters[0]
		}

		tmpl, err := templates.ByTagValue(typ, value)

		if err != nil {
			return err
		}

		m := &model{
			TagValue:      value,
			Type:          typ,
			TypeParameter: tp,
		}

		if err := tmpl.Execute(out, m); err != nil {
			return err
		}
	}

	return nil
}
