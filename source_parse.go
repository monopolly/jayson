package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
)

// model source: package, struct with options and fields
type sourceStruct struct {
	Package string
	Name    string         // news
	Options map[string]any // go=News sql=news
	Imports []string
	Fields  []sourceField
	Funcs   []sourceFunc // methods of interface nameSQL
	CHFuncs []sourceFunc // methods of interface nameCQL
}

// own sql function: method of interface newsSQL
//
//	// where role = @role order by created desc
//	ByRole(role int) ([]News, error) // !404
type sourceFunc struct {
	Name    string
	SQL     string        // comment above
	Flags   string        // comment at the end of line
	Params  []sourceParam // role int
	Results []string      // []News, error
	Imports []string      // packages of types
}

type sourceParam struct {
	Name string
	Type string
}

type sourceField struct {
	Name    string // title
	Type    string // []int
	Comment string // sql{index} #readonly
}

// struct options: // go=News sql=news ch proto
var sourceOptionKeys = map[string]bool{
	"go": true, "sql": true, "ch": true, "proto": true, "test": true, "lock": true, "debug": true, "noinit": true,
	"gotiny": true, "msgp": true, "!omit": true, "optimize": true, "engine": true, "partition": true, "ee": true,
}

// parse model file: only the model struct, other types of file are ignored
// model struct: struct with options comment, or the last struct of file
func parseSource(filename string) (res sourceStruct, err error) {

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return
	}

	res.Package = file.Name.String()
	for _, x := range file.Imports {
		res.Imports = append(res.Imports, strings.Trim(x.Path.Value, `"`))
	}

	var model *ast.StructType
	for _, d := range file.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.TYPE {
			continue
		}
		for _, sp := range g.Specs {
			ts := sp.(*ast.TypeSpec)
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}

			doc := ts.Doc
			if doc == nil {
				doc = g.Doc
			}
			options := parseSourceOptions(doc.Text())

			switch {
			// struct with options
			case sourceHasOptions(doc.Text()):
				if res.Options != nil {
					err = fmt.Errorf("%s: only one model struct in file, found %s and %s", filename, res.Name, ts.Name.Name)
					return
				}
				model, res.Name, res.Options = st, ts.Name.Name, options
			// last struct of file
			case res.Options == nil:
				model, res.Name = st, ts.Name.Name
			}
		}
	}

	if model == nil {
		err = fmt.Errorf("%s: struct not found", filename)
		return
	}

	// own functions: interfaces newsSQL (postgres) and newsCQL (clickhouse)
	for _, d := range file.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.TYPE {
			continue
		}
		for _, sp := range g.Specs {
			ts := sp.(*ast.TypeSpec)
			it, ok := ts.Type.(*ast.InterfaceType)
			if !ok {
				continue
			}
			switch ts.Name.Name {
			case res.Name + "SQL":
				res.Funcs, err = parseSourceFuncs(it, res.Imports)
			case res.Name + "CQL":
				res.CHFuncs, err = parseSourceFuncs(it, res.Imports)
			}
			if err != nil {
				err = fmt.Errorf("%s: %s: %w", filename, ts.Name.Name, err)
				return
			}
		}
	}

	// options in the first comment of file
	if res.Options == nil {
		res.Options = map[string]any{}
		if len(file.Comments) > 0 {
			res.Options = parseSourceOptions(file.Comments[0].Text())
		}
	}

	for _, x := range model.Fields.List {
		t := strings.ReplaceAll(types.ExprString(x.Type), "interface{}", "any")
		comment := strings.TrimSpace(strings.ReplaceAll(x.Comment.Text(), "//", ""))
		for _, name := range x.Names {
			res.Fields = append(res.Fields, sourceField{Name: name.Name, Type: t, Comment: comment})
		}
	}

	return
}

// go=News sql partition="toYYYYMM(created)" > {go: News, sql: true, partition: toYYYYMM(created)}
func parseSourceOptions(v string) map[string]any {
	res := map[string]any{}
	for _, x := range sourceOptionFields(v) {
		k, value, ok := strings.Cut(x, "=")
		switch ok {
		case true:
			res[k] = strings.Trim(value, `"`)
		case false:
			res[k] = true
		}
	}
	return res
}

// words of options, value in quotes may have spaces
func sourceOptionFields(v string) (res []string) {
	var b strings.Builder
	var quoted bool
	flush := func() {
		if b.Len() > 0 {
			res = append(res, b.String())
			b.Reset()
		}
	}
	for _, r := range v {
		switch {
		case r == '"':
			quoted = !quoted
			b.WriteRune(r)
		case !quoted && (r == ' ' || r == '\t' || r == '\n' || r == '\r'):
			flush()
		default:
			b.WriteRune(r)
		}
	}
	flush()
	return
}

// options comment starts with option: "go=News sql=news", not "Config for go"
func sourceHasOptions(doc string) bool {
	words := strings.Fields(doc)
	if len(words) == 0 {
		return false
	}
	k, _, _ := strings.Cut(words[0], "=")
	return sourceOptionKeys[k]
}

// methods of interface with sql in comments
func parseSourceFuncs(it *ast.InterfaceType, imports []string) (res []sourceFunc, err error) {
	for _, m := range it.Methods.List {
		ft, ok := m.Type.(*ast.FuncType)
		if !ok || len(m.Names) != 1 {
			continue
		}
		f := sourceFunc{
			Name:  m.Names[0].Name,
			SQL:   strings.Join(strings.Fields(m.Doc.Text()), " "),
			Flags: strings.TrimSpace(m.Comment.Text()),
		}
		if ft.Params != nil {
			for _, p := range ft.Params.List {
				if len(p.Names) == 0 {
					return nil, fmt.Errorf("%s: parameter without name", f.Name)
				}
				for _, name := range p.Names {
					f.Params = append(f.Params, sourceParam{Name: name.Name, Type: types.ExprString(p.Type)})
				}
				f.Imports = append(f.Imports, exprImports(p.Type, imports)...)
			}
		}
		if ft.Results != nil {
			for _, r := range ft.Results.List {
				for range max(len(r.Names), 1) {
					f.Results = append(f.Results, types.ExprString(r.Type))
				}
				f.Imports = append(f.Imports, exprImports(r.Type, imports)...)
			}
		}
		res = append(res, f)
	}
	return
}

// import paths of packages used in type
func exprImports(expr ast.Expr, imports []string) (res []string) {
	ast.Inspect(expr, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok {
			if p := sourceImportPath(id.Name, imports); p != "" {
				res = append(res, p)
			}
		}
		return false
	})
	return
}
