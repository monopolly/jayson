package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"regexp"
	"strings"
)

// option ee: errors of sql methods are errors.E of github.com/monopolly/errors
const errorsImport = "github.com/monopolly/errors"

// standard errors package has alias when option ee is set
var stdErrorsCall = regexp.MustCompile(`\berrors\.(New|Is|As|Join|Unwrap)\(`)

func stdErrorsAlias(code []byte) []byte {
	return stdErrorsCall.ReplaceAll(code, []byte("stderrors.$1("))
}

// sqlErrors converts public methods of sql class to errors.E:
// method Get becomes private stdGet with standard error,
// public Get is a wrapper: any error is errors.PGX(err)
func sqlErrors(class, priv, convert string, code []byte) []byte {

	src := append([]byte("package x\n"), stdErrorsAlias(code)...)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		panic(fmt.Sprintf("sql errors: %s", err))
	}

	text := func(n ast.Node) string {
		return string(src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset])
	}

	// public methods with error
	renamed := map[string]string{}
	docs := map[*ast.CommentGroup]bool{}
	var wrappers []string

	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || !sqlClassMethod(fn, class) || !fn.Name.IsExported() || fn.Type.Results == nil {
			continue
		}
		results := fn.Type.Results.List
		last, ok := results[len(results)-1].Type.(*ast.Ident)
		if !ok || last.Name != "error" {
			continue
		}

		name := fn.Name.Name
		renamed[name] = "std" + name

		// call arguments
		var args []string
		for _, p := range fn.Type.Params.List {
			for _, n := range p.Names {
				arg := n.Name
				if _, ok := p.Type.(*ast.Ellipsis); ok {
					arg += "..."
				}
				args = append(args, arg)
			}
		}
		call := fmt.Sprintf("a.std%s(%s)", name, strings.Join(args, ", "))

		var doc string
		if fn.Doc != nil {
			doc = text(fn.Doc) + "\n"
			docs[fn.Doc] = true
		}

		// results before error: one type for every name
		var resTypes, resNames []string
		for _, r := range results[:len(results)-1] {
			for range max(1, len(r.Names)) {
				resNames = append(resNames, fmt.Sprintf("r%d", len(resNames)+1))
				resTypes = append(resTypes, types.ExprString(r.Type))
			}
		}
		if len(resNames) == 0 {
			wrappers = append(wrappers, fmt.Sprintf("%sfunc (a *%s) %s%s errors.E {\nreturn %sErr(%s)\n}",
				doc, class, name, text(fn.Type.Params), priv, call))
			continue
		}
		names := strings.Join(resNames, ", ")
		wrappers = append(wrappers, fmt.Sprintf("%sfunc (a *%s) %s%s (%s, errors.E) {\n%s, err := %s\nreturn %s, %sErr(err)\n}",
			doc, class, name, text(fn.Type.Params), strings.Join(resTypes, ", "), names, call, names, priv))
	}

	// rename methods and calls of them inside of methods: a.Get > a.stdGet
	var comments []*ast.CommentGroup
	for _, c := range file.Comments {
		if !docs[c] {
			comments = append(comments, c)
		}
	}
	file.Comments = comments

	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || !sqlClassMethod(fn, class) {
			continue // constructor calls public methods
		}
		if to, ok := renamed[fn.Name.Name]; ok && fn.Name.IsExported() {
			fn.Name.Name = to
			fn.Doc = nil
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "a" {
				if to, ok := renamed[sel.Sel.Name]; ok {
					sel.Sel.Name = to
				}
			}
			return true
		})
	}

	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, file); err != nil {
		panic(fmt.Sprintf("sql errors: %s", err))
	}
	res := bytes.TrimPrefix(b.Bytes(), []byte("package x\n"))

	helper := fmt.Sprintf(`
// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// %[1]sErr converts error of database to errors.E
func %[1]sErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.%[2]s(err)
}
`, priv, convert)

	return []byte(string(res) + helper + "\n" + strings.Join(wrappers, "\n\n") + "\n")
}

// method of sql class: func (a *NewsSQL) ...
func sqlClassMethod(fn *ast.FuncDecl, class string) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	id, ok := star.X.(*ast.Ident)
	return ok && id.Name == class
}
