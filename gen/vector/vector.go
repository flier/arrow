package vector

import (
	"github.com/clipperhouse/typewriter"
)

var vector = &typewriter.Template{
	Name: "vector",
	Text: `// {{.Name}} is a vector of type {{.Type}}. Use it where you would use []{{.Type}}.
//type {{.Name}} {{"{}"}}
`,
}
