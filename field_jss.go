package main

import (
	"github.com/monopolly/jayson/tools"
)

// json options: js{name="sid"}
func (a *Field) parseJSOptions(v string) {
	a.Json.Name = tools.Between(v, `name="`, `"`)
	helps.Field[`js{name=""}`] = `Replace json field name. Ex: js{name="sid"}`
}
