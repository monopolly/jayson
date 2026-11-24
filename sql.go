package main

import (
	"bytes"
	"embed"
	"text/template"
)

//go:embed templates/*.tmpl
var templates embed.FS

var sqlTemplate = template.Must(template.ParseFS(templates, "templates/sql.go.tmpl"))

// sqlGenerate renders NameSQL.go: postgres queries of model on pgx
func sqlGenerate() []byte {
	var b bytes.Buffer
	if err := sqlTemplate.Execute(&b, newSQLModel()); err != nil {
		panic(err)
	}
	return b.Bytes()
}
