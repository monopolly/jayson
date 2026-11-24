package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// own sql functions of model: interfaces newsSQL and newsCQL in model file
var sourceFuncs, sourceCHFuncs []sourceFunc

// database of own functions
type sqlDialect struct {
	Struct   string            // News
	Conn     string            // a.db, a.conn
	NotFound string            // pgx.ErrNoRows, sql.ErrNoRows
	Affected bool              // exec returns count of rows
	Columns  map[string]string // column > go type: array column is one value
}

// generated own sql function
type sqlFunc struct {
	Name      string
	Comment   string // sql
	Signature string // (role int) (res []News, err error)
	Body      string
}

// @name in sql
var sqlNamedParam = regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)`)

// flags of function: !404
var sqlFuncFlag = regexp.MustCompile(`![0-9A-Za-z]+`)

// methods of class from interface newsSQL
func newSQLFuncs(m *sqlModel) (res []sqlFunc) {
	columns := map[string]string{}
	for _, c := range m.Columns {
		columns[c.Name] = c.GoType
	}
	d := sqlDialect{Struct: m.Struct, Conn: "a.db", NotFound: "pgx.ErrNoRows", Affected: true, Columns: columns}
	res = newFuncs(d, sourceFuncs, sqlMethodNames(m))
	for _, f := range sourceFuncs {
		for _, p := range f.Imports {
			m.addImport(p)
		}
	}
	return
}

// own functions of interface as methods of class
func newFuncs(d sqlDialect, list []sourceFunc, used map[string]bool) (res []sqlFunc) {
	for _, f := range list {
		if used[f.Name] {
			panic(fmt.Sprintf("function %s: name is used by generated method", f.Name))
		}
		used[f.Name] = true

		x, err := newSQLFunc(d, f)
		if err != nil {
			panic(fmt.Sprintf("function %s: %s", f.Name, err))
		}
		res = append(res, x)
	}
	return
}

func newSQLFunc(d sqlDialect, f sourceFunc) (res sqlFunc, err error) {

	// flags in the end of line or in sql comment
	flags := f.Flags + " " + strings.Join(sqlFuncFlag.FindAllString(f.SQL, -1), " ")
	query := strings.TrimSpace(sqlFuncFlag.ReplaceAllString(f.SQL, ""))
	noNotFound := strings.Contains(flags, "!404")

	if query == "" {
		return res, fmt.Errorf("no sql in comment above the method")
	}

	// results: (T, error) or error
	n := len(f.Results)
	if n == 0 || n > 2 || f.Results[n-1] != "error" {
		return res, fmt.Errorf("result must be error or (type, error)")
	}
	var result string
	if n == 2 {
		result = f.Results[0]
	}

	// @name > $1
	var params, args []string
	position := map[string]int{}
	for i, p := range f.Params {
		params = append(params, p.Name+" "+p.Type)
		args = append(args, p.Name)
		position[p.Name] = i + 1
	}
	named := map[string]bool{}
	var unknown []string
	query = sqlNamedParam.ReplaceAllStringFunc(query, func(v string) string {
		name := v[1:]
		if position[name] == 0 {
			unknown = append(unknown, v)
			return v
		}
		named[name] = true
		return "$" + strconv.Itoa(position[name])
	})
	if len(unknown) > 0 {
		return res, fmt.Errorf("%s: no such parameter of function", strings.Join(unknown, ", "))
	}
	if len(named) > 0 {
		for _, p := range f.Params {
			if !named[p.Name] {
				return res, fmt.Errorf("parameter %s is not used in sql, write @%s", p.Name, p.Name)
			}
		}
	}

	argList := strings.Join(args, ", ")
	withArgs := func(v string) string {
		if argList == "" {
			return v
		}
		return v + ", " + argList
	}

	word := strings.ToLower(strings.Fields(query)[0])
	const ctx = "context.Background()"
	notFound := ""
	if noNotFound {
		notFound = "\nif errors.Is(err, " + d.NotFound + ") {\nerr = nil\n}"
	}

	res.Name = f.Name
	res.Comment = f.SQL
	res.Signature = fmt.Sprintf("(%s) (res %s, err error)", strings.Join(params, ", "), result)

	// model rows: sql after table name
	model := result == d.Struct || result == "[]"+d.Struct
	if model {
		switch word {
		case "where", "order", "limit", "offset", "join", "inner", "left":
		default:
			return res, fmt.Errorf("sql for %s must start with where, order or limit: columns are added by generator", result)
		}
	}
	tail := strconv.Quote(" " + query)

	switch {

	// error
	case result == "":
		res.Signature = fmt.Sprintf("(%s) (err error)", strings.Join(params, ", "))
		res.Body = fmt.Sprintf("_, err = %s.Exec(%s)\nreturn", d.Conn, withArgs(ctx+", "+strconv.Quote(query)))
		if !d.Affected {
			res.Body = fmt.Sprintf("return %s.Exec(%s)", d.Conn, withArgs(ctx+", "+strconv.Quote(query)))
		}

	// []News
	case result == "[]"+d.Struct:
		res.Body = fmt.Sprintf("return a.query(%s)", withArgs("nil, "+tail))

	// News
	case result == d.Struct:
		res.Body = fmt.Sprintf("res, err = a.queryRow(%s)%s\nreturn", withArgs("nil, "+tail), notFound)

	// rows affected
	case d.Affected && result == "int64" && (word == "update" || word == "delete" || word == "insert") && !strings.Contains(strings.ToLower(query), " returning "):
		res.Body = fmt.Sprintf("tag, err := %s.Exec(%s)\nreturn tag.RowsAffected(), err", d.Conn, withArgs(ctx+", "+strconv.Quote(query)))

	// []int: column of values, or one array value: select music from news where id = $1
	case strings.HasPrefix(result, "[]") && result != "[]byte" && result != "[]uint8" && !strings.Contains(flags, "!array") && !sqlSelectsColumn(d, query, result):
		elem := strings.TrimPrefix(result, "[]")
		switch d.Affected {
		case true: // pgx
			res.Body = fmt.Sprintf("rows, err := %s.Query(%s)\nif err != nil {\nreturn\n}\nreturn pgx.CollectRows(rows, pgx.RowTo[%s])",
				d.Conn, withArgs(ctx+", "+strconv.Quote(query)), elem)
		default:
			res.Body = fmt.Sprintf("rows, err := %s.Query(%s)\nif err != nil {\nreturn\n}\ndefer rows.Close()\nfor rows.Next() {\nvar v %s\nif err = rows.Scan(&v); err != nil {\nreturn\n}\nres = append(res, v)\n}\nreturn res, rows.Err()",
				d.Conn, withArgs(ctx+", "+strconv.Quote(query)), elem)
		}

	// one value
	default:
		res.Body = fmt.Sprintf("err = %s.QueryRow(%s).Scan(&res)%s\nreturn", d.Conn, withArgs(ctx+", "+strconv.Quote(query)), notFound)
	}

	return res, nil
}

// select <column> from ...
var sqlSelectOne = regexp.MustCompile(`(?i)^select\s+(\w+)\s+from\b`)

// query selects one column of model with this go type: array column is one value, not rows
func sqlSelectsColumn(d sqlDialect, query, goType string) bool {
	p := sqlSelectOne.FindStringSubmatch(query)
	return p != nil && d.Columns[p[1]] == goType
}

// names of generated methods of sql class.
// Methods of fields must not repeat them: field where > UpdateWhere
func sqlMethodNames(m *sqlModel) map[string]bool {
	used := map[string]bool{}
	for _, name := range []string{"Pool", "Conn", "Begin", "TX", "TableName", "Fields", "Get", "Exists", "GetBy", "GetWhere", "Has",
		"All", "List", "Select", "Count", "CountWhere", "Insert", "InsertNoConflict", "InsertFull", "Upsert", "InsertMany", "InsertBatch",
		"Update", "Updates", "Save", "Inc", "Delete", "UpdateWhere", "UpdatesWhere", "DeleteWhere", "Search",
		"CreateTable", "CreateIndexes", "ResetSequence", "SetSequence"} {
		used[name] = true
	}

	add := func(field, name string) {
		if used[name] {
			panic(fmt.Sprintf("sql field %s: method %s already exists, rename the field or set go{name=\"...\"}", field, name))
		}
		used[name] = true
	}

	for _, x := range m.Customs {
		add("@"+x, "Get"+x)
	}
	for _, x := range m.Searches {
		add(x.Name, "FullText"+x.GoName)
	}
	for _, c := range m.GetBy {
		add(c.Name, "GetBy"+c.GoName)
	}
	if m.Key == nil {
		return used
	}
	for _, c := range m.Columns {
		switch {
		case c.Elem != "":
			add(c.Name, "Append"+c.GoName)
			add(c.Name, "Remove"+c.GoName)
		case c.JSON == "array":
			add(c.Name, "Append"+c.GoName)
		case c.JSON != "":
			for _, action := range []string{"Update", "Merge", "Delete", "Rename"} {
				add(c.Name, action+c.GoName)
			}
		}
	}
	return used
}
