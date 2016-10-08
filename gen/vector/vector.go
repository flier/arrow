package vector

import (
	"github.com/clipperhouse/typewriter"
)

var vector = &typewriter.Template{
	Name: "vector",
	Text: `// {{.Name}} is a vector of type {{.Type}}. Use it where you would use []{{.Type}}.
type {{.Name}} struct {{"{"}}
	*BaseValueVector
{{"}"}}

func (v *{{.Name}}) ValueCapacity() int { return v.data.Cap() / int(unsafe.Sizeof(*(*{{.Type}})(nil))) }

func (v *{{.Name}}) Accessor() Accessor { return v }

func (v *{{.Name}}) Mutator() Mutator { return v }
`,
}

var value = &typewriter.Template{
	Name: "value",
	Text: `
func (v *{{.Name}}) {{.Type}}(index int) (value {{.Type}}, err error) {
	if 0 <= index && index < v.ValueCount() {
		value = {{.Type}}(v.data.{{.Type}}(index))
	} else {
		err = errOutOfRange
	}
	return
}

func (v *{{.Name}}) Put{{.Type}}(index int, value {{.Type}}) error {
	if 0 <= index && index < v.ValueCount() {
		v.data.Put{{.Type}}(index, ({{.Type.Underlying}})(value))

		return nil
	}

	return errOutOfRange
}`,
}

var time = &typewriter.Template{
	Name: "time",
	Text: `
func (v *{{.Name}}) {{.Type}}(index int) (value time.Time, err error) {
	if 0 <= index && index < v.ValueCount() {
		value = v.data.{{.Type}}(index)
	} else {
		err = errOutOfRange
	}
	return
}

func (v *{{.Name}}) Put{{.Type}}(index int, value time.Time) error {
	if 0 <= index && index < v.ValueCount() {
		v.data.Put{{.Type}}(index, value)

		return nil
	}

	return errOutOfRange
}`,
}

var duration = &typewriter.Template{
	Name: "duration",
	Text: `
func (v *{{.Name}}) {{.Type}}(index int) (value time.Duration, err error) {
	if 0 <= index && index < v.ValueCount() {
		value = v.data.{{.Type}}(index)
	} else {
		err = errOutOfRange
	}
	return
}

func (v *{{.Name}}) Put{{.Type}}(index int, value time.Duration) error {
	if 0 <= index && index < v.ValueCount() {
		v.data.Put{{.Type}}(index, value)

		return nil
	}

	return errOutOfRange
}`,
}

var accessor = &typewriter.Template{
	Name: "accessor",
	Text: `
// implement Accessor

func (v *{{.Name}}) Get(index int) (interface{}, error) {
	value, err := v.{{.Type}}(index)

	return value, err
}

func (v *{{.Name}}) ValueCount() int { return v.data.Len() / int(unsafe.Sizeof(*(*{{.Type}})(nil))) }

func (v *{{.Name}}) IsNull(index int) bool { return false }
`,
}

var mutator = &typewriter.Template{
	Name: "mutator",
	Text: `
// implement Mutator

func (v *{{.Name}}) SetValueCount(valueCount int) {
	v.data.Grow(valueCount * int(unsafe.Sizeof(*(*{{.Type}})(nil))))
}
`,
}
