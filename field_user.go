package main

import (
	"github.com/monopolly/jayson/tools"
	"strings"
)

func (a *Field) parseUserOptions() {

	for _, list := range tools.Betweens(a.Comment, "list{", "}") {
		words := strings.Split(list, ",")
		var name string
		for _, x := range words {
			x = strings.TrimSpace(x)
			if x == "" {
				continue
			}

			switch name == "" {
			case true:
				name = strings.TrimSpace(tools.Between(x, `title="`, `"`))
			case false:
				a.Go.UserLists[name] = append(a.Go.UserLists[name], x)
			}

		}
	}

	a.Go.Desc = strings.TrimSpace(tools.Between(a.Comment, "desc{", "}"))

	// help
	helps.Golang["title{}"] = "Add field user title title{Nice}"
	helps.Golang["desc{}"] = "Add field user title desc{Use it nice}"
}
