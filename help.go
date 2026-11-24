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
	SQL        map[string]string
	Clickhouse map[string]string
	Proto      map[string]string
	Custom     map[string]map[string]string
}

func (a *Help) init() (b *Help) {
	a.Struct = make(map[string]string)
	a.Golang = make(map[string]string)
	a.Field = make(map[string]string)
	a.SQL = make(map[string]string)
	a.Clickhouse = make(map[string]string)
	a.Proto = make(map[string]string)
	a.Custom = make(map[string]map[string]string)
	return a
}

func (a *Help) Render() (res []byte) {

	var list []string
	list = append(list, "/*")
	list = append(list, "Auto-generate. Do not change!")
	list = append(list, "Help to auto-generated correct models for golang structures.")
	list = append(list, fmt.Sprintf("Sergey Keplin (c) %d", time.Now().Year()))
	list = append(list, "a@senthy.com, lava.mobi@gmail.com, https://t.me/martinprestone")
	list = append(list, "github.com/monopolly/jayson")
	list = append(list, "Ex: // go=News sql=news ch=news proto test")
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

	// own functions and engines
	list = append(list, helpExamples()...)

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
	return fmt.Sprintf("%s%s%v", name, strings.Repeat(" ", max(1, 30-len(name))), v)
}

// sorted help lines
func helpLines[V any](m map[string]V) (res []string) {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		res = append(res, Status(k, m[k]))
	}
	return
}

// examples of own functions and clickhouse engines: only for used databases
func helpExamples() (list []string) {

	name := settings.Origin
	item := settings.Go.StructName

	if settings.SQL.Table != "" {
		table := settings.SQL.Table
		list = append(list, "", "SQL FUNCTIONS: methods of interface become methods of "+settings.SQL.Class)
		list = append(list, strings.Split(fmt.Sprintf(`type %[1]sSQL interface {
	// where role = @role and key = @key limit 1
	Creds(role role.Role, key uuid.UUID) (%[2]s, error)

	// select role from %[3]s where id = @id
	Role(id int) (role.Role, error) // !404: empty value instead of not found

	// select music from %[3]s where id = @id
	Music(id int) ([]int, error)

	// select id from %[3]s where role = @role
	IDs(role role.Role) ([]int, error)

	// update %[3]s set created = now() where id = @id
	Touch(id int) error

	// delete from %[3]s where created < @before
	Clean(before time.Time) (int64, error) // count of rows
}`, name, item, table), "\n")...)
	}

	if settings.Clickhouse.Table != "" {
		table := settings.Clickhouse.Table
		list = append(list, "", "CLICKHOUSE FUNCTIONS: methods of interface become methods of "+settings.Clickhouse.Class)
		list = append(list, strings.Split(fmt.Sprintf(`type %[1]sCQL interface {
	// where role = @role and key = @key limit 1
	Creds(role role.Role, key uuid.UUID) (%[2]s, error)

	// select role from %[3]s where id = @id
	Role(id int) (role.Role, error) // !404: empty value instead of not found

	// select music from %[3]s where id = @id
	Music(id int) ([]int, error)

	// select id from %[3]s where role = @role
	IDs(role role.Role) ([]int64, error)

	// alter table %[3]s delete where created < @before
	Clean(before time.Time) error
}`, name, item, table), "\n")...)

		list = append(list, "", "CLICKHOUSE ENGINES: engine=ReplacingMergeTree(updated) partition=toYYYYMM(created)")
		for _, x := range clickhouseEngines {
			list = append(list, fmt.Sprintf("%s%s%s", x[0], strings.Repeat(" ", max(1, 44-len(x[0]))), x[1]))
		}
	}
	return
}
