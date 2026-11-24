package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"strings"
	"text/template"
)

var clickhouse Clickhouse

type Clickhouse int

var clickhouseTemplate = template.Must(template.ParseFS(templates, "templates/clickhouse.go.tmpl"))

// clickhouse view of model for templates/clickhouse.go.tmpl
type chModel struct {
	Struct string // Event
	Class  string // EventCQL
	Index  string // EventIndexTyped
	Priv   string // eventCH: prefix of private names
	Table  string // events
	EE     bool   // errors of methods are errors.E

	Columns []*chColumn
	Key     *chColumn // row id: ch{primary}, sql{inc} or id

	SelectAll   string // id, country, ip
	InsertSQL   string // insert into events (id, country, ip)
	CreateTable string
	Funcs       []sqlFunc // own functions: interface eventCQL
}

type chColumn struct {
	Name   string // column: country
	GoName string // Country
	Index  string // EventIndexCountry
	Type   string // LowCardinality(String)
	GoType string // string

	ElemGo   string // array element go type: Array(String) > string
	MapKey   string // Map(String, Int64) > string
	MapValue string // Map(String, Int64) > int
}

// Generate renders clickhouse class of model
func (a *Clickhouse) Generate() []byte {

	name := settings.Go.StructName
	m := &chModel{
		Struct: name,
		Class:  settings.Clickhouse.Class,
		Index:  settings.IndexTypeName,
		Priv:   strings.ToLower(name[:1]) + name[1:] + "CH",
		Table:  settings.Clickhouse.Table,
		EE:     settings.SQL.EE,
	}

	key := a.keyField()
	columns := map[string]string{}
	for _, x := range fields {
		if x.Clickhouse.Skip {
			continue
		}
		c := &chColumn{
			Name:   x.Clickhouse.Name,
			GoName: x.Go.Name,
			Index:  x.Go.Index,
			Type:   a.fieldType(x),
			GoType: x.GoType(),
		}

		// native arrays and maps: methods of field
		t := x.Clickhouse.Type
		if expr, err := parser.ParseExpr(x.BaseType()); err == nil {
			switch e := expr.(type) {
			case *ast.ArrayType:
				if e.Len == nil && strings.HasPrefix(t, "Array(") {
					c.ElemGo = types.ExprString(e.Elt)
				}
			case *ast.MapType:
				if strings.HasPrefix(t, "Map(") {
					c.MapKey, c.MapValue = types.ExprString(e.Key), types.ExprString(e.Value)
				}
			}
		}

		m.Columns = append(m.Columns, c)
		columns[c.Name] = c.GoType
		if c.Name == key {
			m.Key = c
		}
	}

	var names []string
	for _, c := range m.Columns {
		names = append(names, c.Name)
	}
	m.SelectAll = strings.Join(names, ", ")
	m.InsertSQL = fmt.Sprintf("insert into %s (%s)", m.Table, m.SelectAll)
	m.CreateTable = a.createTableSQL()

	d := sqlDialect{Struct: m.Struct, Conn: "a.conn", NotFound: "sql.ErrNoRows", Columns: columns}
	m.Funcs = newFuncs(d, sourceCHFuncs, chMethodNames(m))

	var b bytes.Buffer
	if err := clickhouseTemplate.Execute(&b, m); err != nil {
		panic(err)
	}
	if m.EE {
		return sqlErrors(m.Class, m.Priv, "ClickHouse", b.Bytes())
	}
	return b.Bytes()
}

// row id column: ch{primary}, sql{inc} or id
func (a *Clickhouse) keyField() string {
	if primary := a.primaryFields(); len(primary) > 0 {
		return primary[0]
	}
	if settings.Fields.IncField != "" {
		for _, x := range fields {
			if x.SQL.Name == settings.Fields.IncField {
				return x.Clickhouse.Name
			}
		}
	}
	return "id"
}

// names of generated methods of clickhouse class
func chMethodNames(m *chModel) map[string]bool {
	used := map[string]bool{}
	for _, name := range []string{"Conn", "TableName", "Fields", "Add", "Len", "Push", "Insert", "Get", "Exists", "GetBy", "GetWhere", "Has",
		"All", "List", "Select", "Count", "CountWhere", "Update", "Updates", "Inc", "Delete", "UpdateWhere", "UpdatesWhere", "DeleteWhere",
		"Stat", "CreateTable"} {
		used[name] = true
	}
	if m.Key == nil {
		return used
	}
	add := func(field, name string) {
		if used[name] {
			panic(fmt.Sprintf("clickhouse field %s: method %s already exists, rename the field or set go{name=\"...\"}", field, name))
		}
		used[name] = true
	}
	for _, c := range m.Columns {
		switch {
		case c.ElemGo != "":
			add(c.Name, "Append"+c.GoName)
			add(c.Name, "Remove"+c.GoName)
		case c.MapKey != "":
			add(c.Name, "Update"+c.GoName)
			add(c.Name, "Delete"+c.GoName)
		}
	}
	return used
}

// imports of clickhouse code
func clickhouseImports() []string {
	res := []string{
		"context", "database/sql", "errors", "fmt", "maps", "reflect", "slices", "strconv", "strings", "sync", "time",
		"github.com/ClickHouse/clickhouse-go/v2/lib/driver",
	}
	if hasClickhouseIPFields() {
		res = append(res, "net")
	}
	for _, f := range sourceCHFuncs {
		res = append(res, f.Imports...)
	}
	return res
}
