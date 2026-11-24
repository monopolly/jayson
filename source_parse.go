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
}

type sourceField struct {
	Name    string // title
	Type    string // []int
	Comment string // sql{index} #readonly
}

// struct options: // go=News sql=news ch proto
var sourceOptionKeys = map[string]bool{
	"go": true, "sql": true, "ch": true, "js": true, "ts": true, "proto": true, "swift": true,
	"demo": true, "enum": true, "test": true, "lock": true, "debug": true, "noinit": true,
	"gotiny": true, "msgp": true, "!omit": true, "optimize": true, "noprefix": true, "chengine": true,
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

// go=News sql ch > {go: News, sql: true, ch: true}
func parseSourceOptions(v string) map[string]any {
	res := map[string]any{}
	for _, x := range strings.Fields(v) {
		k, value, ok := strings.Cut(x, "=")
		switch ok {
		case true:
			res[k] = value
		case false:
			res[k] = true
		}
	}
	return res
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
