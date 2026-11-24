package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"regexp"
	"strings"

	"github.com/monopolly/cast"
)

// [16]byte, [32]uint8
var goFixedBytes = regexp.MustCompile(`^\[\d+\](byte|uint8)$`)

const (
	uuidType   = "uuid.UUID"
	uuidImport = "github.com/google/uuid"
)

func goConvertCode(fieldName, sourceName, fieldType string) string {
	return goConvertTo("a."+fieldName, sourceName, fieldType)
}

// convert source to target var: cast.Convert(&a.Name, x)
// sources: json values, pgx rows.Values(), clickhouse values
func goConvertTo(target, sourceName, fieldType string) string {
	return goConvertValue(target, sourceName, fieldType, 0)
}

func goConvertValue(target, src, t string, depth int) string {

	// aliases
	switch t {
	case "byte":
		t = "uint8"
	case "rune":
		t = "int32"
	case "[]uint8":
		t = "[]byte"
	case "interface{}":
		t = "any"
	}

	switch {

	case t == "any":
		return fmt.Sprintf(`%s = %s`, target, src)

	case t == uuidType:
		// pgx: [16]byte, clickhouse: uuid.UUID, json: string
		return fmt.Sprintf(`switch v := %[2]s.(type) {
			case uuid.UUID:
				%[1]s = v
			case [16]byte:
				%[1]s = v
			case string:
				%[1]s, _ = uuid.Parse(v)
			case []byte:
				if len(v) == 16 {
					%[1]s, _ = uuid.FromBytes(v)
				} else {
					%[1]s, _ = uuid.ParseBytes(v)
				}
			}`, target, src)

	case t == "uint" || t == "uint64":
		// pgx numeric(20,0): pgtype.Numeric
		return fmt.Sprintf(`if n, ok := %[2]s.(interface{ MarshalJSON() ([]byte, error) }); ok {
				b, _ := n.MarshalJSON()
				cast.Convert(&%[1]s, string(b))
			} else {
				cast.Convert(&%[1]s, %[2]s)
			}`, target, src)

	case t == "[]byte":
		return fmt.Sprintf(`cast.Convert(&%s, %s)`, target, src)

	// [16]byte from bytea
	case goFixedBytes.MatchString(t):
		return fmt.Sprintf(`switch v%[3]d := %[2]s.(type) {
			case %[4]s:
				%[1]s = v%[3]d
			case []byte:
				copy(%[1]s[:], v%[3]d)
			case string:
				copy(%[1]s[:], v%[3]d)
			}`, target, src, depth, t)

	// pgx arrays: []any
	case strings.HasPrefix(t, "[]"):
		elem := strings.TrimPrefix(t, "[]")
		if elem == "any" || elem == "interface{}" {
			return fmt.Sprintf(`cast.Convert(&%s, %s)`, target, src)
		}
		fallback := fmt.Sprintf(`b, _ := jsoniter.Marshal(v%[2]d)
				_ = jsoniter.Unmarshal(b, &%[1]s)`, target, depth)
		if cast.Supported(t) == nil {
			fallback = fmt.Sprintf(`cast.Convert(&%s, v%d)`, target, depth)
		}
		return fmt.Sprintf(`switch v%[4]d := %[2]s.(type) {
			case nil:
			case %[3]s:
				%[1]s = v%[4]d
			case []any:
				r%[4]d := make(%[3]s, 0, len(v%[4]d))
				for _, e%[4]d := range v%[4]d {
					var el%[4]d %[5]s
					%[6]s
					r%[4]d = append(r%[4]d, el%[4]d)
				}
				%[1]s = r%[4]d
			default:
				%[7]s
			}`, target, src, t, depth, elem,
			goConvertValue(fmt.Sprintf("el%d", depth), fmt.Sprintf("e%d", depth), elem, depth+1),
			fallback)

	// *string: nil > nil
	case strings.HasPrefix(t, "*"):
		elem := strings.TrimPrefix(t, "*")
		return fmt.Sprintf(`if %[2]s != nil {
				var p%[3]d %[4]s
				%[5]s
				%[1]s = &p%[3]d
			}`, target, src, depth, elem,
			goConvertValue(fmt.Sprintf("p%d", depth), src, elem, depth+1))

	case cast.Supported(t) == nil:
		return fmt.Sprintf(`cast.Convert(&%s, %s)`, target, src)
	}

	// custom type: role.Role > int
	if base := goBaseTypeOf(t); base != "" && base != t {
		return fmt.Sprintf(`var b%[3]d %[4]s
			%[5]s
			%[1]s = %[2]s(b%[3]d)`, target, t, depth, base,
			goConvertValue(fmt.Sprintf("b%d", depth), src, base, depth+1))
	}

	return goConvertAny(target, src, t, depth)
}

// any other type: netip.Addr, json.RawMessage, pgtype.Date, sql.NullString, decimal.Decimal, structs, maps
func goConvertAny(target, src, t string, depth int) string {

	// pgx inet/cidr: netip.Prefix
	var extra string
	switch t {
	case "netip.Addr":
		extra = fmt.Sprintf(`case netip.Prefix:
			%s = v%d.Addr()`, target, depth)
	case "net.IP":
		extra = fmt.Sprintf(`case netip.Prefix:
			%s = net.IP(v%d.Addr().AsSlice())`, target, depth)
	// pgx uuid: [16]byte
	case "pgtype.UUID":
		extra = fmt.Sprintf(`case [16]byte:
			%s = pgtype.UUID{Bytes: v%d, Valid: true}`, target, depth)
	case "net.IPNet":
		extra = fmt.Sprintf(`case netip.Prefix:
			%s = net.IPNet{IP: v%[2]d.Masked().Addr().AsSlice(), Mask: net.CIDRMask(v%[2]d.Bits(), v%[2]d.Addr().BitLen())}`, target, depth)
	}

	return fmt.Sprintf(`switch v%[4]d := %[2]s.(type) {
		case nil:
		case %[3]s:
			%[1]s = v%[4]d
		case *%[3]s:
			if v%[4]d != nil {
				%[1]s = *v%[4]d
			}
		%[5]s
		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&%[1]s).(interface{ Scan(any) error }); !ok || s.Scan(v%[4]d) != nil {
				b, _ := jsoniter.Marshal(v%[4]d)
				_ = jsoniter.Unmarshal(b, &%[1]s)
			}
		}`, target, src, t, depth, extra)
}

// underlying builtin type of custom type: role.Role > int, "" if not resolved
func goBaseTypeOf(t string) string {
	if _, ok := sqlSpecialTypes[t]; ok {
		return ""
	}
	expr, err := parser.ParseExpr(t)
	if err != nil {
		return ""
	}
	switch e := expr.(type) {
	case *ast.Ident:
		if typeBuiltin(e.Name) {
			return ""
		}
	case *ast.SelectorExpr:
	default:
		return ""
	}
	base, err := resolveBaseType(expr, source.Imports, 0)
	if err != nil {
		return ""
	}
	return base
}

// field converter, custom type via base type:
// var v int; cast.Convert(&v, x); a.Role = role.Role(v)
func goConvertField(x *Field, sourceName string) string {
	if x.Go.Base == "" {
		return goConvertCode(x.Go.Name, sourceName, x.Type)
	}
	return fmt.Sprintf("var v %s\n%s\na.%s = %s(v)", x.Go.Base, goConvertTo("v", sourceName, x.Go.Base), x.Go.Name, x.GoType())
}

func goConvertSupported(fieldType string) bool {
	switch fieldType {
	case uuidType, "[]" + uuidType, "[]time.Time", "[]time.Duration",
		"[]uint", "[]uint8", "[]uint16", "[]uint32", "[]uint64",
		"[]int8", "[]int16", "[]int32", "[]int64",
		"[]float32":
		return true
	default:
		return false
	}
}
