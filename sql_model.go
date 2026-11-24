package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"strings"

	"github.com/monopolly/jayson/tools"
)

// sql view of model for templates/sql.go.tmpl
type sqlModel struct {
	Package string // news
	Struct  string // News
	Class   string // NewsSQL
	DB      string // NewsDB
	Query   string // NewsQuery
	Index   string // NewsIndexTyped
	Priv    string // newsSQL: prefix of private names, several models in one package
	Table   string // news

	TableIdent string // pgx.Identifier{"public", "news"}
	KeyIndexFn string // NewsKeyIndex
	ValidKeyFn string // NewsValidKey

	Columns  []*sqlColumn // table columns in declaration order
	Insert   []*sqlColumn // columns of insert
	Key      *sqlColumn   // row id: inc, single primary key or id column
	Inc      *sqlColumn   // identity column
	Conflict []*sqlColumn // upsert conflict target
	GetBy    []*sqlColumn // sql{get}
	Customs  []string     // custom structs: Public, Me
	Searches []sqlSearch  // tsvector columns

	HasArrays bool // bigint[], text[]
	HasJSON   bool // jsonb
	HasIPNet  bool // net.IPNet

	SelectAll     string // inc, coalesce(title, '')
	InsertSQL     string // insert into news (title) values ($1)
	InsertFullSQL string // insert with all columns
	UpsertSQL     string // insert ... on conflict (id) do update set ...
	CreateTable   string // create table if not exists ...
	Indexes       []sqlIndex

	Imports []string // packages of field types
}

type sqlColumn struct {
	Field *Field

	Name   string // sql name: title
	GoName string // Title
	Index  string // IndexTitle
	Type   string // sql type: text, bigint[]
	GoType string // string, []int
	Elem   string // array element sql type: bigint
	ElemGo string // array element go type: int, role.Role

	JSON     string // jsonb kind: map, object, array
	MapKey   string // map[string]int > string
	MapValue string // map[string]int > int
	JSONElem string // []Item in jsonb > Item
	Select   string // select expression: coalesce(title, '')
	Value    string // go value for query args: a.Title
	Dest     string // go scan target: &a.Title
}

type sqlSearch struct {
	Name   string // search
	GoName string // Search
}

type sqlIndex struct {
	Name string // idx_news_title
	SQL  string // create index if not exists ...
	Help string // example of query
}

func newSQLModel() *sqlModel {

	m := &sqlModel{
		Package:    settings.PackageName,
		Struct:     settings.Go.StructName,
		Class:      settings.SQL.Class,
		DB:         settings.Go.StructName + "DB",
		Query:      settings.SQL.QueryName,
		Index:      settings.IndexTypeName,
		Priv:       strings.ToLower(settings.Go.StructName[:1]) + settings.Go.StructName[1:] + "SQL",
		Table:      settings.SQL.Table,
		KeyIndexFn: settings.Go.StructName + "KeyIndex",
		ValidKeyFn: settings.Go.StructName + "ValidKey",
	}

	// public.news > pgx.Identifier{"public", "news"}
	var ident []string
	for _, x := range strings.Split(m.Table, ".") {
		ident = append(ident, fmt.Sprintf("%q", x))
	}
	m.TableIdent = fmt.Sprintf("pgx.Identifier{%s}", strings.Join(ident, ", "))

	// imports of template
	imports := map[string]bool{
		"context": true, "github.com/goccy/go-json": true, "errors": true, "fmt": true, "maps": true, "slices": true,
		"strconv": true, "strings": true, "github.com/jackc/pgx/v5": true, "github.com/jackc/pgx/v5/pgconn": true,
	}
	customs := map[string]bool{}
	searches := map[string]bool{}
	var primary []*sqlColumn

	for _, x := range fields {

		// custom structs: @public
		for k := range x.Go.CustomStructs {
			if !customs[k] {
				customs[k] = true
				m.Customs = append(m.Customs, tools.Title(k))
			}
		}

		if x.SQL.Skip {
			continue
		}

		c := newSQLColumn(x, m.Priv)
		m.Columns = append(m.Columns, c)

		if !x.SQL.NoInsert {
			m.Insert = append(m.Insert, c)
		}
		if x.SQL.Inc && m.Inc == nil {
			m.Inc = c
		}
		if x.SQL.Primary {
			primary = append(primary, c)
		}
		if x.SQL.Get {
			m.GetBy = append(m.GetBy, c)
		}
		if c.Elem != "" {
			m.HasArrays = true
		}
		if c.GoType == "net.IPNet" {
			m.HasIPNet = true
		}
		if c.Type == "jsonb" {
			m.HasJSON = true
		}
		for _, g := range x.SQL.Index.Search {
			if !searches[g] {
				searches[g] = true
				m.Searches = append(m.Searches, sqlSearch{Name: g, GoName: sqlGoName(g)})
			}
		}
		for _, p := range x.Go.Imports {
			if !imports[p] {
				imports[p] = true
				m.Imports = append(m.Imports, p)
			}
		}
		if x.Go.BaseImport != "" && !imports[x.Go.BaseImport] {
			imports[x.Go.BaseImport] = true
			m.Imports = append(m.Imports, x.Go.BaseImport)
		}
	}
	sortStrings(m.Customs)

	// row id: inc, single primary key, id column
	switch {
	case m.Inc != nil:
		m.Key = m.Inc
	case len(primary) == 1:
		m.Key = primary[0]
	default:
		for _, c := range m.Columns {
			if c.Name == "id" {
				m.Key = c
			}
		}
	}

	// upsert conflict target: primary keys or inc
	switch {
	case len(primary) > 0:
		m.Conflict = primary
	case m.Inc != nil:
		m.Conflict = []*sqlColumn{m.Inc}
	}

	m.SelectAll = sqlJoin(m.Columns, func(c *sqlColumn) string { return c.Select })
	m.InsertSQL = sqlInsertQuery(m.Table, m.Insert)
	m.InsertFullSQL = sqlInsertQuery(m.Table, m.Columns)
	m.UpsertSQL = sqlUpsertQuery(m)
	m.CreateTable, m.Indexes = sqlDDL()

	return m
}

func newSQLColumn(x *Field, priv string) *sqlColumn {

	c := &sqlColumn{
		Field:  x,
		Name:   x.SQL.Name,
		GoName: x.Go.Name,
		Index:  x.Go.Index,
		Type:   x.SQL.Type,
		GoType: x.GoType(),
		Value:  x.SQLValue(),
		Dest:   "&a." + x.Go.Name,
	}

	base := strings.TrimLeft(x.BaseType(), "*")

	// one dimension native array: bigint[] > bigint
	if x.SQL.Array && sqlArrayDims(c.Type) == 1 && strings.HasPrefix(base, "[]") {
		c.Elem = strings.TrimSuffix(c.Type, "[]")
		c.ElemGo = strings.TrimPrefix(base, "[]")
	}

	// jsonb: map[string]int, []Item, struct
	if c.Type == "jsonb" {
		c.JSON = "object"
		if expr, err := parser.ParseExpr(base); err == nil {
			switch e := expr.(type) {
			case *ast.MapType:
				c.JSON, c.MapKey, c.MapValue = "map", types.ExprString(e.Key), types.ExprString(e.Value)
			case *ast.ArrayType:
				if e.Len == nil {
					c.JSON, c.JSONElem = "array", types.ExprString(e.Elt)
				}
			}
		}
	}

	// [16]byte: pgx scans bytea only to slice
	if goFixedBytes.MatchString(c.GoType) {
		c.Dest = fmt.Sprintf("%sScanBytes(func(b []byte) { copy(a.%s[:], b) })", priv, x.Go.Name)
	}

	// net.IPNet: pgx can not scan null
	if c.GoType == "net.IPNet" {
		c.Dest = fmt.Sprintf("%sScanPrefix(func(p netip.Prefix) { a.%s = net.IPNet{}; if p.IsValid() { a.%s = net.IPNet{IP: p.Masked().Addr().AsSlice(), Mask: net.CIDRMask(p.Bits(), p.Addr().BitLen())} } })", priv, x.Go.Name, x.Go.Name)
	}

	c.Select = sqlSelectExpr(x)
	return c
}

// select expression: null of scalar column is zero value in go
// pgx can not scan null to string, int, bool, time.Time
func sqlSelectExpr(x *Field) string {

	name := x.SQL.Name

	t := x.GoType()
	if x.Go.Base != "" {
		t = x.Go.Base // role.Role > int
	}

	switch t {
	case "string":
		if sqlTextType(x) {
			return fmt.Sprintf("coalesce(%s, '')", name)
		}
		return fmt.Sprintf("coalesce(%s::text, '')", name) // type="inet", type="jsonb"
	case "bool":
		return fmt.Sprintf("coalesce(%s, false)", name)
	case "int", "int8", "int16", "int32", "int64", "rune",
		"uint", "uint8", "uint16", "uint32", "uint64", "byte",
		"float32", "float64", "time.Duration", "decimal.Decimal":
		return fmt.Sprintf("coalesce(%s, 0)", name)
	case "time.Time":
		return fmt.Sprintf("coalesce(%s, '0001-01-01 00:00:00+00')", name)
	}

	// struct in jsonb: json null is zero struct
	if x.SQL.Type == "jsonb" || x.SQL.Type == "json" {
		switch {
		case strings.HasPrefix(t, "*"), strings.HasPrefix(t, "[]"), strings.HasPrefix(t, "map["), t == "any", t == "json.RawMessage":
		default:
			return fmt.Sprintf("coalesce(%s, 'null'::%s)", name, x.SQL.Type)
		}
	}

	// pointers, slices, maps, pgtype.*, sql.Null*, netip.*: null is supported by pgx
	return name
}

// text, varchar(64), char(2), citext
func sqlTextType(x *Field) bool {
	t := strings.ToLower(x.SQL.Type)
	if x.SQL.Replace != "" {
		t = strings.ToLower(x.SQL.Replace) // replace="varchar(64) unique"
	}
	for _, p := range []string{"text", "varchar", "char", "character", "citext", "bpchar", "name"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// insert into news (title, body) values ($1, $2)
func sqlInsertQuery(table string, columns []*sqlColumn) string {
	var args []string
	for i := range columns {
		args = append(args, fmt.Sprintf("$%d", i+1))
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", table,
		sqlJoin(columns, func(c *sqlColumn) string { return c.Name }), strings.Join(args, ", "))
}

// insert ... on conflict (id) do update set title = excluded.title
func sqlUpsertQuery(m *sqlModel) string {
	if len(m.Conflict) == 0 {
		return ""
	}
	conflict := map[string]bool{}
	for _, c := range m.Conflict {
		conflict[c.Name] = true
	}
	var set []string
	for _, c := range m.Columns {
		if !conflict[c.Name] {
			set = append(set, fmt.Sprintf("%[1]s = excluded.%[1]s", c.Name))
		}
	}
	action := "do nothing"
	if len(set) > 0 {
		action = "do update set " + strings.Join(set, ", ")
	}
	return fmt.Sprintf("%s on conflict (%s) %s", sqlInsertQuery(m.Table, m.Columns),
		sqlJoin(m.Conflict, func(c *sqlColumn) string { return c.Name }), action)
}

func sqlJoin(columns []*sqlColumn, f func(*sqlColumn) string) string {
	var list []string
	for _, c := range columns {
		list = append(list, f(c))
	}
	return strings.Join(list, ", ")
}

// name_index > NameIndex
func sqlGoName(v string) string {
	var res []string
	for _, x := range strings.FieldsFunc(v, func(r rune) bool { return r == '_' || r == '-' || r == ' ' }) {
		res = append(res, tools.Title(x))
	}
	return strings.Join(res, "")
}
