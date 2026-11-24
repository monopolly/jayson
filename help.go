package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

var helps = NewHelp()

func NewHelp() (a *Help) {
	a = new(Help)
	a.init()
	return
}

type Help struct {
	Struct     map[string]string //value
	Golang     map[string]string
	Field      map[string]string
	JS         map[string]string
	Swift      map[string]string
	SQL        map[string]string
	Clickhouse map[string]string
	Proto      map[string]string
	Custom     map[string]map[string]string
}

func (a *Help) init() (b *Help) {
	a.Struct = make(map[string]string)
	a.Golang = make(map[string]string)
	a.Field = make(map[string]string)
	a.JS = make(map[string]string)
	a.Swift = make(map[string]string)
	a.SQL = make(map[string]string)
	a.Clickhouse = make(map[string]string)
	a.Proto = make(map[string]string)
	a.Custom = make(map[string]map[string]string)
	return a
}

func (a *Help) AddCustom(category, k, v string) {
	if a.Custom[category] == nil {
		a.Custom[category] = make(map[string]string)
	}
	a.Custom[category][k] = v
}

func (a *Help) Render() (res []byte) {

	var list []string
	list = append(list, "/*")
	list = append(list, "Auto-generate. Do not change!")
	list = append(list, "Help to auto-generated correct models for golang structures.")
	list = append(list, fmt.Sprintf("Sergey Keplin (c) %d", time.Now().Year()))
	list = append(list, "a@senthy.com, lava.mobi@gmail.com, https://t.me/martinprestone")
	list = append(list, "github.com/monopolly/jsons")
	list = append(list, "Ex: //! [] go=News js=NewsJson sql=news noinit up...")
	list = append(list, "")

	// structs
	list = append(list, "STRUCT:")
	list = append(list, helpLines(a.Struct)...)

	// golang
	list = append(list, "")
	list = append(list, "Field:")
	list = append(list, helpLines(a.Field)...)

	// golang
	list = append(list, "")
	list = append(list, "GO:")
	list = append(list, helpLines(a.Golang)...)

	// sql
	list = append(list, "")
	list = append(list, "SQL:")
	list = append(list, helpLines(a.SQL)...)

	// clickhouse
	list = append(list, "")
	list = append(list, "Clickhouse:")
	list = append(list, helpLines(a.Clickhouse)...)

	// proto
	list = append(list, "")
	list = append(list, "PROTO:")
	list = append(list, helpLines(a.Proto)...)

	// swift
	list = append(list, "")
	list = append(list, "SWIFT:")
	list = append(list, helpLines(a.Swift)...)

	// js
	list = append(list, "")
	list = append(list, "JSON:")
	list = append(list, helpLines(a.JS)...)

	// add \t
	for p := range list {
		list[p] = "\t" + list[p]
	}
	list = append(list, "*/\n")

	// render
	res = []byte(strings.Join(list, "\n"))
	return
}

func Status(name string, v interface{}) string {
	return fmt.Sprintf("%s%s%v", name, strings.Repeat(" ", 30-len(name)), v)
}

// sorted help lines
func helpLines[V any](m map[string]V) (res []string) {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		res = append(res, Status(k, m[k]))
	}
	return
}
