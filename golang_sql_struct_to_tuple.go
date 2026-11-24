package main

import (
	"fmt"
	"strings"
)

// parser from the original struct to []interface{}
func (a *Golang) SQLStructToTuple() []byte {

	lines := []string{
		"\n",
		"//Tuple create an array from struct",
		fmt.Sprintf("func (a *%s) sqlTuple() (r []any){ ", settings.Go.StructName),
	}
	var fieldlist []string
	for _, x := range fields {
		if x.SQL.NoInsert || x.SQL.Skip {
			continue
		}
		fieldlist = append(fieldlist, x.SQLValue())
	}
	lines = append(lines, fmt.Sprintf("return []any{%s}", strings.Join(fieldlist, ",")))
	lines = append(lines, "}")

	return []byte(strings.Join(lines, "\n"))
}

// parser from the original struct to []interface{}
func (a *Golang) SQLAllStructToTuple() []byte {

	lines := []string{
		"\n",
		"//Tuple create an array from struct",
		fmt.Sprintf("func (a *%s) sqlAllTuples() (r []any){ ", settings.Go.StructName),
	}
	var fieldlist []string
	for _, x := range fields {
		if x.SQL.Skip {
			continue
		}
		fieldlist = append(fieldlist, x.SQLValue())
	}
	lines = append(lines, fmt.Sprintf("return []any{%s}", strings.Join(fieldlist, ",")))
	lines = append(lines, "}")

	return []byte(strings.Join(lines, "\n"))
}

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
			fieldlist = append(fieldlist, x.GoValue())
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
			return fmt.Sprintf("func() []byte { b, _ := json.Marshal(a.%s); return b }()", a.Go.Name)
		}
	}
	return a.GoValue()
}
