package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"strings"
)

// go type > postgres type, checked before base types: time.Duration is not int64 here
var sqlSpecialTypes = map[string]string{
	"time.Time":     "timestamptz",
	"time.Duration": "bigint",

	uuidType:      "uuid",
	"pgtype.UUID": "uuid",

	"json.RawMessage": "jsonb",

	"netip.Addr":   "inet",
	"netip.Prefix": "cidr",
	"net.IP":       "inet",
	"net.IPNet":    "cidr",

	"decimal.Decimal": "numeric",
	"pgtype.Numeric":  "numeric",

	"pgtype.Text":        "text",
	"pgtype.Bool":        "boolean",
	"pgtype.Int2":        "smallint",
	"pgtype.Int4":        "integer",
	"pgtype.Int8":        "bigint",
	"pgtype.Uint32":      "bigint",
	"pgtype.Float4":      "real",
	"pgtype.Float8":      "double precision",
	"pgtype.Date":        "date",
	"pgtype.Time":        "time",
	"pgtype.Timestamp":   "timestamp",
	"pgtype.Timestamptz": "timestamptz",
	"pgtype.Interval":    "interval",
	"pgtype.Hstore":      "hstore",
	"pgtype.Point":       "point",
	"pgtype.Box":         "box",
	"pgtype.Circle":      "circle",
	"pgtype.Line":        "line",
	"pgtype.Lseg":        "lseg",
	"pgtype.Path":        "path",
	"pgtype.Polygon":     "polygon",
	"pgtype.Bits":        "varbit",

	"sql.NullString":  "text",
	"sql.NullBool":    "boolean",
	"sql.NullByte":    "smallint",
	"sql.NullInt16":   "smallint",
	"sql.NullInt32":   "integer",
	"sql.NullInt64":   "bigint",
	"sql.NullFloat64": "double precision",
	"sql.NullTime":    "timestamptz",
}

// go builtin type > postgres type
var sqlBuiltinTypes = map[string]string{
	"bool":    "boolean",
	"string":  "text",
	"int":     "bigint",
	"int8":    "smallint",
	"int16":   "smallint",
	"int32":   "integer",
	"rune":    "integer",
	"int64":   "bigint",
	"uint":    "numeric(20,0)",
	"uint8":   "smallint",
	"byte":    "smallint",
	"uint16":  "integer",
	"uint32":  "bigint",
	"uint64":  "numeric(20,0)",
	"float32": "real",
	"float64": "double precision",
}

// pgtype.Range[pgtype.Int4] > int4range
var sqlRangeTypes = map[string]string{
	"integer":          "int4range",
	"bigint":           "int8range",
	"numeric":          "numrange",
	"date":             "daterange",
	"timestamp":        "tsrange",
	"timestamptz":      "tstzrange",
	"smallint":         "int4range",
	"numeric(20,0)":    "numrange",
	"double precision": "numrange",
}

// go type to postgres type: time.Time > timestamptz, []int > bigint[], *string > text
func goToSQLType(goType string) (string, error) {
	expr, err := parser.ParseExpr(goType)
	if err != nil {
		return "", fmt.Errorf("%s: %w", goType, err)
	}
	return exprToSQLType(expr, source.Imports, 0)
}

func exprToSQLType(expr ast.Expr, imports []string, depth int) (string, error) {

	if depth > 10 {
		return "", fmt.Errorf("type loop")
	}

	name := types.ExprString(expr)
	if t, ok := sqlSpecialTypes[name]; ok {
		return t, nil
	}

	switch e := expr.(type) {

	// int, local type Role
	case *ast.Ident:
		switch e.Name {
		case "any":
			return "jsonb", nil
		case "uintptr", "complex64", "complex128":
			return "", fmt.Errorf("%s is not supported in sql, set sql{type=\"...\"} or sql{skip}", e.Name)
		}
		if t, ok := sqlBuiltinTypes[e.Name]; ok {
			return t, nil
		}
		local, ok := sourceLocalTypes()[e.Name]
		if !ok {
			return "", fmt.Errorf("type %s not found in package", e.Name)
		}
		return exprToSQLType(local.Expr, local.Imports, depth+1)

	// role.Role
	case *ast.SelectorExpr:
		base, err := resolveBaseType(e, imports, 0)
		if err != nil {
			// external struct type
			if strings.Contains(err.Error(), "is not supported") {
				return "jsonb", nil
			}
			return "", err
		}
		return goToSQLTypeNested(base, depth)

	// *string > text, null by default
	case *ast.StarExpr:
		return exprToSQLType(e.X, imports, depth+1)

	// []T, [N]T
	case *ast.ArrayType:
		elem := types.ExprString(e.Elt)
		switch elem {
		case "byte", "uint8":
			return "bytea", nil
		}
		t, err := exprToSQLType(e.Elt, imports, depth+1)
		if err != nil {
			return "", err
		}
		// []struct, []map
		// [][]int: pgx rows.Values() flattens multidimensional arrays and postgres does not allow ragged arrays
		if t == "jsonb" || strings.HasSuffix(t, "[]") {
			return "jsonb", nil
		}
		return t + "[]", nil

	// sql.Null[T], pgtype.Range[T], pgtype.Multirange[T]
	case *ast.IndexExpr:
		inner, err := exprToSQLType(e.Index, imports, depth+1)
		if err != nil {
			return "", err
		}
		switch types.ExprString(e.X) {
		case "sql.Null":
			return inner, nil
		case "pgtype.Range":
			r, ok := sqlRangeTypes[inner]
			if !ok {
				return "", fmt.Errorf("%s: range of %s is not supported", name, inner)
			}
			return r, nil
		case "pgtype.Multirange":
			// pgtype.Multirange[pgtype.Range[pgtype.Int8]] > int8multirange
			if !strings.HasSuffix(inner, "range") {
				return "", fmt.Errorf("%s: multirange of %s is not supported", name, inner)
			}
			return strings.TrimSuffix(inner, "range") + "multirange", nil
		}
		return "", fmt.Errorf("generic type %s is not supported", name)

	case *ast.MapType, *ast.StructType, *ast.InterfaceType:
		return "jsonb", nil

	default:
		return "", fmt.Errorf("type %s is not supported in sql", name)
	}
}

func goToSQLTypeNested(goType string, depth int) (string, error) {
	expr, err := parser.ParseExpr(goType)
	if err != nil {
		return "", fmt.Errorf("%s: %w", goType, err)
	}
	return exprToSQLType(expr, source.Imports, depth+1)
}

// insert sql query
func GoTypeToSQLType(x *Field) {

	t, err := goToSQLType(x.GoType())
	if err != nil && x.Go.Base != "" {
		// go{base="int"}
		t, err = goToSQLType(x.Go.Base)
	}
	if err != nil {
		panic(fmt.Sprintf("sql %s: %s", x.Name, err))
	}

	x.SQL.Type = t

	switch {
	case strings.HasSuffix(t, "[]"):
		// native postgres array: bigint[], text[][]
		x.SQL.Array = true
		x.SQL.Default.Value = "default '{}'" //by default for later updates
	case t == "jsonb":
		switch strings.HasPrefix(strings.TrimLeft(x.BaseType(), "*"), "[") {
		case true:
			// []struct, []map
			x.SQL.JsonbArray = true
			x.SQL.Default.Value = "default '[]'::jsonb" //by default for later updates
		case false:
			x.SQL.Default.Value = "default '{}'::jsonb" //by default for later updates
		}
	}
}

// array dimensions: bigint[][] > 2
func sqlArrayDims(t string) int {
	return strings.Count(t, "[]")
}

// insert sql query
func DetectSQLDefault(x *Field) {

	switch x.SQL.Default.Unixtime {
	case true:
		x.SQL.Default.Value = "default extract(epoch from now())"
	case false:
		switch x.SQL.Default.Simple {
		case true:
			switch x.SQL.Type {
			case "bigint", "int", "integer", "smallint", "real", "double precision", "numeric", "numeric(20,0)":
				x.SQL.Default.Value = "default 0"
			case "boolean":
				x.SQL.Default.Value = "default false"
			case "text":
				x.SQL.Default.Value = "default ''"
			case "uuid":
				x.SQL.Default.Value = "default gen_random_uuid()"
			case "timestamptz", "timestamp":
				x.SQL.Default.Value = "default now()"
			case "date":
				x.SQL.Default.Value = "default current_date"
			}
		}
	}
}
