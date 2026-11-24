package main

import (
	"fmt"
	"strings"
)

// create fast json marshal
func (a *Golang) FastJson() []byte {

	var list []string
	list = append(list, "\n")
	list = append(list, "//fast json marshal")
	list = append(list, fmt.Sprintf("func (a *%s) Pack() []byte {", settings.Go.StructName))
	list = append(list, "b, _ := jsoniter.Marshal(a)")
	list = append(list, "return b")
	list = append(list, "}")
	list = append(list, "\n")

	return []byte(strings.Join(list, "\n"))
}

// create fast json unmarshal
func (a *Golang) FastJsonUnmarshall() []byte {
	var list []string
	list = append(list, "\n")
	list = append(list, "//fast json unmarshal")
	list = append(list, fmt.Sprintf("func Parse%[1]s(v []byte) (a %[1]s, err error) { ", settings.Go.StructName))
	list = append(list, `err = jsoniter.Unmarshal(v, &a)`)
	list = append(list, "return")
	list = append(list, "}")
	list = append(list, "\n")
	return []byte(strings.Join(list, "\n"))
}
