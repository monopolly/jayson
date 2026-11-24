package main

import (
	"bytes"
	"embed"
	"text/template"
)

//go:embed templates/*.tmpl
var templates embed.FS

var sqlTemplate = template.Must(template.ParseFS(templates, "templates/sql.go.tmpl"))

// sqlGenerate renders postgres queries of model on pgx, part of NameGO.go
func sqlGenerate() []byte {
	var b bytes.Buffer
	if err := sqlTemplate.Execute(&b, newSQLModel()); err != nil {
		panic(err)
	}
	return b.Bytes()
}

// imports of sql code
func sqlImports() []string {
	res := []string{
		"context", "errors", "fmt", "maps", "slices", "strconv", "strings",
		"gojson \"github.com/goccy/go-json\"",
		"github.com/jackc/pgx/v5", "github.com/jackc/pgx/v5/pgconn", "github.com/jackc/pgx/v5/pgxpool",
	}
	for _, f := range sourceFuncs {
		res = append(res, f.Imports...)
	}
	return res
}
