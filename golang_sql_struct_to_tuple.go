package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"strings"
)

// parser from the original struct to []interface{}
func (a *Golang) ClickhouseStructToTuple() []byte {

	lines := []string{
		"\n",
		"//Tuple create an array from struct for clickhouse",
		fmt.Sprintf("func (a *%s) clickhouseTuple() (r []any){ ", settings.Go.StructName),
	}
	var fieldlist []string
	for _, x := range fields {
		if x.Clickhouse.Skip {
			continue
		}
		// ip string > net.IP, netip.Addr/net.IP as is
		switch x.Clickhouse.IP && x.BaseType() == "string" {
		case true:
			fieldlist = append(fieldlist, fmt.Sprintf(`net.ParseIP(a.%s)`, x.Go.Name))
		default:
			fieldlist = append(fieldlist, x.ClickhouseValue())
		}
	}
	lines = append(lines, fmt.Sprintf("return []any{%s}", strings.Join(fieldlist, ",")))
	lines = append(lines, "}")

	return []byte(strings.Join(lines, "\n"))
}

// struct value for pgx: [16]byte > a.Hash[:] (pgx does not encode byte arrays to bytea)
func (a *Field) SQLValue() string {
	if goFixedBytes.MatchString(a.GoType()) && a.SQL.Type == "bytea" {
		return fmt.Sprintf("a.%s[:]", a.Go.Name)
	}
	// pgx can not encode empty net.IPNet: null
	if strings.TrimPrefix(a.GoType(), "*") == "net.IPNet" && !strings.HasPrefix(a.GoType(), "*") {
		return fmt.Sprintf("func() any { if a.%[1]s.IP == nil { return nil }; return a.%[1]s }()", a.Go.Name)
	}

	// pgx sends string in any as raw json: marshal it
	switch a.GoType() {
	case "any", "interface{}":
		if a.SQL.Type == "jsonb" || a.SQL.Type == "json" {
			return fmt.Sprintf("func() []byte { b, _ := gojson.Marshal(a.%s); return b }()", a.Go.Name)
		}
	}
	return a.GoValue()
}

// go types of clickhouse numbers: driver needs exact types in maps
var clickhouseNative = map[string]string{"int": "int64", "uint": "uint64"}

// struct value for clickhouse: map[string]int > map[string]int64 for Map(String, Int64)
func (a *Field) ClickhouseValue() string {
	if !strings.HasPrefix(a.Clickhouse.Type, "Map(") {
		return a.GoValue()
	}
	expr, err := parser.ParseExpr(a.BaseType())
	if err != nil {
		return a.GoValue()
	}
	m, ok := expr.(*ast.MapType)
	if !ok {
		return a.GoValue()
	}
	k, v := types.ExprString(m.Key), types.ExprString(m.Value)
	nk, nv := clickhouseNative[k], clickhouseNative[v]
	if nk == "" && nv == "" {
		return a.GoValue()
	}
	if nk == "" {
		nk = k
	}
	if nv == "" {
		nv = v
	}
	return fmt.Sprintf("func() map[%[1]s]%[2]s { r := make(map[%[1]s]%[2]s, len(a.%[3]s)); for k, v := range a.%[3]s { r[%[1]s(k)] = %[2]s(v) }; return r }()", nk, nv, a.Go.Name)
}
