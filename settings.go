package main

var settings = NewSettings()

func NewSettings() (a *Settings) {
	a = new(Settings)
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
		Gotiny      bool     //fast binary format
		MessagePack bool     //msgpack binary format
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
		Engine    string //MergeTree, ReplacingMergeTree(ver)
		Partition string //toYYYYMM(created)
		TTL       string //created + interval 30 day
		Class     string //AccountClickhouseSQL: type accountCHSQL int{}
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
		path       string //test/news.go
		golang     string //test/news.go
		golangTest string //test/newsGO_test.go
		golangSQL  string //test/newsSQL.go
		sql        string //test/news.sql
		clickhouse string //test/news.cql
		proto      string //test/news.proto
	}

	Padding int //max len field title

}
