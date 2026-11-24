package main

import (
	"github.com/monopolly/jayson/tools"
	"strings"
)

// tags
func (a *Field) parseTagsOptions() {
	helps.Golang["#"] = "Add lists for fields. Ex: id int //#readonly #must..."
	for x := range strings.FieldsSeq(a.Comment) {
		tag := tools.Value(x, "#")
		if tag != "" {
			tag = strings.TrimSpace(tag)
			a.Go.Tags[tools.Title(tag)] = true
		}
	}
}
