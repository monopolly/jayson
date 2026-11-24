package main

import "fmt"

var settings = NewSettings()

func NewSettings() (a *Settings) {
	a = new(Settings)
	a.Imports = make(map[string]bool)
	return
}

type Settings struct {
	Debug      bool
	StructSize int //bytes

	Origin        string //type news struct{}
	PackageName   string
	IndexTypeName string //NewsIndexType

	Go struct {
		StructName  string //struct name: News
		NoOmit      bool
		NoPrefix    bool
		Gotiny      bool     //fast binary format
		MessagePack bool     //msgpack binary format
		EasyJson    bool     //generate easy json fast code gen
		Indexes     []string //string list all indexes [IndexNewsID, IndexNewsTitle...]
	}

	Proto struct {
		Name    string //NewsProto
		Package string //pb
	}

	SQL struct {
		Class     string //accountSQL: type accountSQL int{}
		Table     string //sql table from struct
		QueryName string //NewsQuery
		EE        bool   //errors of sql methods are errors.E of github.com/monopolly/errors
	}

	Clickhouse struct {
		Table     string //sql table from struct
		Engine    string //MergeTree
		Class     string //AccountClickhouseSQL: type accountCHSQL int{}
		QueryName string //NewsClickhouseQuery
	}

	Options struct {
		Noinit   bool // removes New() entirely
		Optimize bool //demo json file with default values
		Test     bool //generate go tests for generated file
	}

	Fields struct {
		IncField string //fields has ID int field (for sql insert generates)
	}

	File struct {
		path           string //test/news.go
		golang         string //test/news.go
		golangTest     string //test/newsGO_test.go
		golangSQL      string //test/newsSQL.go
		golangSQLQuery string //test/newsQuery.go
		sql            string //test/news.sql
		clickhouse     string //test/news.cql
		proto          string //test/news.proto
	}

	Padding int //max len field title

	Imports map[string]bool
}

// github.com/something
func (a *Settings) AddImportPackage(v string) {
	a.Imports[fmt.Sprintf(`"%s"`, v)] = true
}

// jj "github.com/something"
func (a *Settings) AddImportRawPackage(v string) {
	a.Imports[v] = true
}
