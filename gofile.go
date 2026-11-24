package main

import (
	"fmt"

	"github.com/monopolly/file"
	goimports "golang.org/x/tools/imports"
)

// saveGo formats go source, removes unused imports and saves file.
// Broken source is saved as is to see the error in place
func saveGo(filename string, src []byte) {
	res, err := goimports.Process(filename, src, &goimports.Options{Comments: true, TabIndent: true, TabWidth: 8})
	if err != nil {
		fmt.Println("go format:", filename, err)
		res = src
	}
	file.Save(filename, res)
}
